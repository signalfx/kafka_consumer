package main

import (
	"context"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dpsink"
	"github.com/signalfx/golib/event"
	"github.com/signalfx/golib/sfxclient"
	"log"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var hasher128Pool = sync.Pool{
	New: func() interface{} {
		x := InitMurmur128Hasher()
		return &x
	},
}

type signalfxForwarder struct {
	chans  []chan *datapoint.Datapoint
	sinks  []dpsink.Sink
	dps    chan *datapoint.Datapoint
	evts   chan *event.Event
	wg     sync.WaitGroup
	done   chan struct{}
	config *config
	ctx    context.Context
	stats  struct {
		numE         int64
		dPBatchSizes *sfxclient.RollingBucket
		dPSendTime   *sfxclient.RollingBucket
		numDpErrors  int64
	}
}

func (s *signalfxForwarder) close() {
	close(s.done)
	s.wg.Wait()
}

func (s *signalfxForwarder) mainDrain() {
	for {
		select {
		case dp := <-s.dps:
			i := s.hash(dp)
			s.chans[i] <- dp // blocks

		case <-s.done:
			s.wg.Done()
			return
		}
	}
}

func (s *signalfxForwarder) drainChannel(i int) {
	buf := make([]*datapoint.Datapoint, 0) // it will grow
	var c chan *datapoint.Datapoint
	if s.config.useHashing {
		c = s.chans[i]
	} else {
		c = s.dps
	}

	for {
		select {
		case dp := <-c:
			buf = append(buf, dp)
			s.fillAndSend(buf, c, i)
			buf = buf[:0]
		case e := <-s.evts:
			atomic.AddInt64(&s.stats.numE, int64(1)) // send events 1x1
			if err := s.sinks[i].AddEvents(s.ctx, []*event.Event{e}); err != nil {
				log.Printf("E! Error sending events to signalfx! %s", err.Error())
			}
		case <-s.done:
			s.wg.Done()
			return
		}

	}
}

func (s *signalfxForwarder) hash(dp *datapoint.Datapoint) int {
	m := hasher128Pool.Get().(*Murmur128Hasher)
	_, low := m.Sum128(dp)
	partition := low % uint64(len(s.chans))
	return int(partition)

}

func (s *signalfxForwarder) fillAndSend(buf []*datapoint.Datapoint, c chan *datapoint.Datapoint, i int) {
	for {
		select {
		case dp := <-c:
			buf = append(buf, dp)
			if len(buf) >= s.config.batchSize {
				s.sendToSignalFx(buf, i)
				buf = buf[:0]
			}
		default:
			if len(buf) > 0 {
				s.sendToSignalFx(buf, i)
			}
			return
		}
	}
}

func (s *signalfxForwarder) Datapoints() []*datapoint.Datapoint {
	dps := s.stats.dPBatchSizes.Datapoints()
	dps = append(dps, s.stats.dPSendTime.Datapoints()...)
	dims := map[string]string{"path": "kafka_consumer", "obj": "forwarder"}
	dps = append(dps, []*datapoint.Datapoint{
		sfxclient.CumulativeP("total_events", dims, &s.stats.numE),
		sfxclient.CumulativeP("total_datapoint_errors", dims, &s.stats.numDpErrors),
		sfxclient.Gauge("buffer_size", map[string]string{"path": "kafka_consumer", "obj": "forwarder", "chan": "main", "type": "datapoint"}, int64(len(s.dps))),
		sfxclient.Gauge("buffer_size", map[string]string{"path": "kafka_consumer", "obj": "forwarder", "chan": "main", "type": "event"}, int64(len(s.evts))),
	}...)
	if s.config.useHashing {
		for i, c := range s.chans {
			dps = append(dps, sfxclient.Gauge("buffer_size", map[string]string{"path": "kafka_consumer", "obj": "forwarder", "chan": strconv.Itoa(i)}, int64(len(c))))
		}
	}
	return dps
}

type byTimestamp []*datapoint.Datapoint

func (c byTimestamp) Len() int           { return len(c) }
func (c byTimestamp) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }
func (c byTimestamp) Less(i, j int) bool { return c[i].Timestamp.UnixNano() < c[j].Timestamp.UnixNano() }

func (s *signalfxForwarder) sendToSignalFx(buf []*datapoint.Datapoint, i int) {
	sort.Sort(byTimestamp(buf))
	s.stats.dPBatchSizes.Add(float64(len(buf)))
	now := time.Now().UnixNano()
	if err := s.sinks[i].AddDatapoints(s.ctx, buf); err != nil {
		log.Printf("E! Error sending datapoints to signalfx! %s", err.Error())
		atomic.AddInt64(&s.stats.numDpErrors, 1)
	}
	s.stats.dPSendTime.Add(float64(time.Now().UnixNano() - now))
}

func newSignalFxForwarder(c *config) *signalfxForwarder {
	chans := make([]chan *datapoint.Datapoint, c.numDrainThreads)
	sinks := make([]dpsink.Sink, c.numDrainThreads)
	tr := &http.Transport{
		MaxIdleConns:       c.numDrainThreads,
		IdleConnTimeout:    30 * time.Second,
		DisableCompression: true,
	}
	client := http.Client{
		Transport: tr,
		Timeout:   5 * time.Second,
	}
	for i := range chans {
		s := sfxclient.NewHTTPSink()
		s.Client = client
		s.AuthToken = c.sfxToken
		s.DatapointEndpoint = c.sfxEndpoint + "/v2/datapoint"
		s.EventEndpoint = c.sfxEndpoint + "/v2/event"
		sinks[i] = s

		if c.useHashing {
			chans[i] = make(chan *datapoint.Datapoint, c.channelSize)
		}
	}

	f := &signalfxForwarder{
		chans:  chans,
		sinks:  sinks,
		dps:    make(chan *datapoint.Datapoint, c.channelSize*c.numDrainThreads),
		evts:   make(chan *event.Event, c.channelSize),
		done:   make(chan struct{}, 1),
		config: c,
		ctx:    context.TODO(),
	}
	f.stats.dPBatchSizes = sfxclient.NewRollingBucket("batch_sizes", map[string]string{"path": "kafka_consumer", "obj": "datapoint"})
	f.stats.dPSendTime = sfxclient.NewRollingBucket("request_duration", map[string]string{"path": "kafka_consumer", "obj": "datapoint"})
	for i := range chans {
		go f.drainChannel(i)
	}
	f.wg.Add(c.numDrainThreads)
	if c.useHashing {
		go f.mainDrain()
		f.wg.Add(1)
	}
	return f
}
