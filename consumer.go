package main

import (
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/event"
	"github.com/signalfx/golib/sfxclient"
	"log"
	"reflect"
	"regexp"
	"sync"
	"sync/atomic"
	"time"
)

type parser interface {
	parse(msg []byte, dps chan *datapoint.Datapoint, events chan *event.Event) (int, error)
}

type consumer struct {
	consumer clusterConsumer
	topics   []string
	config   *config
	regexed  *regexp.Regexp

	replacements chan struct{}

	done  chan struct{}
	wg    sync.WaitGroup
	parser parser
	dps   chan *datapoint.Datapoint
	evts  chan *event.Event
	stats struct {
		numMessages          int64
		numMetricsParsed     int64
		numErrs              int64
		numReplacements      int64
		numParseErrs         int64
		numTelegrafParseErrs int64
	}
}

func (c *consumer) refresh() {
	for {
		select {
		case <-c.done:
			c.wg.Done()
			return
		case <-time.After(c.config.refreshInterval):
			c.replacements <- struct{}{}
		}
	}
}

func (c *consumer) consume() {
	for {
		select {
		case <-c.done:
			c.closeConsumer()
			log.Println("I! Consumer drained")
			c.wg.Done()
			return
		case <-c.replacements:
			logIfErr("E! Error fetching new topic list! %s", c.refreshInternal())
		case msg := <-c.consumer.Messages():
			atomic.AddInt64(&c.stats.numMessages, 1)

			if numMetrics, err := c.parser.parse(msg.Value, c.dps, c.evts); err == nil {
				atomic.AddInt64(&c.stats.numMetricsParsed, int64(numMetrics))
			} else {
				atomic.AddInt64(&c.stats.numParseErrs, 1)
				log.Printf("E! Message Parse Error\nmessage: %v\nerror: %s", msg, err)
			}

			c.consumer.MarkOffset(msg, "")
		case err := <-c.consumer.Errors():
			atomic.AddInt64(&c.stats.numErrs, 1)
			if err != nil {
				log.Printf("E! consumer Error: %s\n", err.Error())
			}
		}
	}
}

func (c *consumer) closeConsumer() error {
	if err := c.consumer.Close(); err != nil {
		log.Printf("E! Error closing consumer: %s", err.Error())
		return err
	}
	return nil
}

func (c *consumer) refreshInternal() (err error) {
	valid, err := c.config.getTopicList(c.regexed)
	log.Printf("D! refreshing, got %v, %s", valid, err)
	if err == nil {
		answer := reflect.DeepEqual(valid, c.topics)
		if !answer {
			log.Printf("I! New topics detected, old %v new %v", c.topics, valid)
			c.replaceConsumer(valid)
		}
	}
	return err
}

func (c *consumer) replaceConsumer(valid []string) error {
	log.Printf("D! inside replaceConsumer valid")
	defer log.Printf("D! exiting replaceConsumer")

	log.Printf("D! getting new consumer: %v", valid)
	newConsumer, err := c.config.getClusterConsumer(valid, "oldest") // must do oldest here so we don't miss anything
	if err != nil {
		log.Printf("E! Error creating new consumer! %s", err.Error())
		return err
	}

	log.Printf("D! closing old consumer")
	c.closeConsumer()
	log.Printf("D! done closing old consumer")

	c.consumer = newConsumer
	c.topics = valid
	atomic.AddInt64(&c.stats.numReplacements, 1)
	return nil
}

func (c *consumer) close() {
	close(c.done)
	c.wg.Wait()
	close(c.replacements)
}

func (c *consumer) Datapoints() []*datapoint.Datapoint {
	dims := map[string]string{"path": "kafka_consumer", "obj": "consumer"}
	dps := []*datapoint.Datapoint{
		sfxclient.CumulativeP("total_messages_received", dims, &c.stats.numMessages),
		sfxclient.CumulativeP("total_messages_parsed", dims, &c.stats.numMetricsParsed),
		sfxclient.CumulativeP("total_errors_received", dims, &c.stats.numErrs),
	}
	return dps
}

func newConsumer(c *config, dps chan *datapoint.Datapoint, evts chan *event.Event) (*consumer, error) {
	regexed, err := regexp.Compile(c.topicPattern)
	if err != nil {
		return nil, err
	}

	valid, err := c.getTopicList(regexed)
	if err != nil {
		return nil, err
	}

	cc, err := c.getClusterConsumer(valid, c.offset)
	if err != nil {
		return nil, err
	}

	parser, err := c.getParser()
	if err != nil {
		return nil, err
	}

	consumer := &consumer{
		consumer:     cc,
		config:       c,
		topics:       valid,
		parser:       parser,
		done:         make(chan struct{}),
		regexed:      regexed,
		dps:          dps,
		evts:         evts,
		replacements: make(chan struct{}, 10),
	}

	go consumer.consume()
	go consumer.refresh()
	consumer.wg.Add(2)
	return consumer, nil
}
