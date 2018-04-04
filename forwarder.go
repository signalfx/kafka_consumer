package main

import (
	"context"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dpsink"
	"github.com/signalfx/golib/event"
	"github.com/signalfx/golib/sfxclient"
	"log"
	"sync"
	"sync/atomic"
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
		numD int64
		numE int64
	}
}

func (s *signalfxForwarder) close() {
	close(s.done)
	s.wg.Wait()
	log.Printf("Forwarder stats: datapoints %d events %d", atomic.LoadInt64(&s.stats.numD), atomic.LoadInt64(&s.stats.numE))
}

func (s *signalfxForwarder) mainDrain() {
	for {
		select {
		case dp := <-s.dps:
			i := s.hash(dp)
			select {
			case s.chans[i] <- dp:
			default:
				log.Printf("E! dropping point %v because chan %d is full", *dp, i)
			}

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
			s.fillAndSend(buf, i)
			buf = buf[:0]
		case e := <-s.evts:
			atomic.AddInt64(&s.stats.numD, int64(1))
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
func (s *signalfxForwarder) fillAndSend(buf []*datapoint.Datapoint, i int) {
	for {
		select {
		case dp := <-s.chans[i]:
			buf = append(buf, dp)
			if len(buf) >= s.config.batchSize {
				s.sendToSignalFx(buf, i)
			}
		default:
			if len(buf) > 0 {
				s.sendToSignalFx(buf, i)
			}
			return
		}
	}
}

func (s *signalfxForwarder) sendToSignalFx(buf []*datapoint.Datapoint, i int) {
	atomic.AddInt64(&s.stats.numD, int64(len(buf)))
	if err := s.sinks[i].AddDatapoints(s.ctx, buf); err != nil {
		log.Printf("E! Error sending datapoints to signalfx! %s", err.Error())
	}
}

func newSignalFxForwarder(c *config) *signalfxForwarder {
	chans := make([]chan *datapoint.Datapoint, c.numDrainThreads)
	sinks := make([]dpsink.Sink, c.numDrainThreads)
	for i := range chans {
		s := sfxclient.NewHTTPSink()
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
