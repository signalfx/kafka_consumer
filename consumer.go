package main

import (
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/parsers"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/event"
	"github.com/signalfx/golib/sfxclient"
	"github.com/signalfx/telegraf/plugins/outputs/signalfx"
	"log"
	"reflect"
	"regexp"
	"sync"
	"sync/atomic"
	"time"
)

type telegrafParser interface {
	GetObjects(metrics []telegraf.Metric, dps chan *datapoint.Datapoint, evets chan *event.Event)
}

type consumer struct {
	consumer clusterConsumer
	topics   []string
	config   *config
	parser   parsers.Parser
	regexed  *regexp.Regexp

	replacements chan struct{}

	done  chan struct{}
	wg    sync.WaitGroup
	tsfx  telegrafParser
	dps   chan *datapoint.Datapoint
	evts  chan *event.Event
	stats struct {
		numMessages          int64
		numSentToParse       int64
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
			metrics, err := c.parser.Parse(msg.Value)
			if err == nil {
				atomic.AddInt64(&c.stats.numSentToParse, 1)
				c.tsfx.GetObjects(metrics, c.dps, c.evts)
			} else {
				log.Printf("E! Message Parse Error\nmessage: %s\nerror: %s", msg.Value, err)
				atomic.AddInt64(&c.stats.numParseErrs, 1)
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
		sfxclient.CumulativeP("total_messages_sent_to_parse", dims, &c.stats.numSentToParse),
		sfxclient.CumulativeP("total_errors_received", dims, &c.stats.numErrs),
	}
	return dps
}

func newConsumer(c *config, dps chan *datapoint.Datapoint, evts chan *event.Event) (*consumer, error) {
	regexed, err := regexp.Compile(c.topicPattern)
	if err == nil {
		var valid []string
		valid, err = c.getTopicList(regexed)
		if err == nil {
			var cc clusterConsumer
			cc, err = c.getClusterConsumer(valid, c.offset)
			if err == nil {
				var parser parsers.Parser
				parser, err = c.parserConstructor()
				if err == nil {
					consumer := &consumer{
						consumer:     cc,
						config:       c,
						topics:       valid,
						parser:       parser,
						done:         make(chan struct{}),
						regexed:      regexed,
						tsfx:         signalfx.NewSignalFx(),
						dps:          dps,
						evts:         evts,
						replacements: make(chan struct{}, 10),
					}

					go consumer.consume()
					go consumer.refresh()
					consumer.wg.Add(2)

					return consumer, nil
				}
			}
		}
	}
	return nil, err
}
