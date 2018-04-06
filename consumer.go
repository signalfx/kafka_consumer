package main

import (
	"github.com/Shopify/sarama"
	"github.com/influxdata/telegraf/plugins/parsers"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/sfxclient"
	"github.com/signalfx/sarama-cluster"
	"log"
	"reflect"
	"regexp"
	"sync"
	"sync/atomic"
	"time"
)

type consumer struct {
	consumer *cluster.Consumer
	topics   []string
	config   *config
	parser   parsers.Parser
	regexed  *regexp.Regexp

	mu sync.RWMutex

	in    <-chan *sarama.ConsumerMessage
	errs  <-chan error
	done  chan struct{}
	wg    sync.WaitGroup
	tsfx  *telegrafToSfx
	f     *signalfxForwarder
	stats struct {
		numMessages    int64
		numSentToParse int64
		numErrs        int64
	}
}

func (c *consumer) consumeInner() {
	c.mu.RLock()
	defer c.mu.RUnlock()
	select {
	case err := <-c.errs:
		atomic.AddInt64(&c.stats.numErrs, 1)
		if err != nil {
			log.Printf("E! consumer Error: %s\n", err.Error())
		}
	case msg := <-c.in:
		atomic.AddInt64(&c.stats.numMessages, 1)
		metrics, err := c.parser.Parse(msg.Value)
		if err != nil {
			log.Printf("E! Message Parse Error\nmessage: %s\nerror: %s", msg.Value, err)
		}
		atomic.AddInt64(&c.stats.numSentToParse, 1)
		if c.tsfx.parse(metrics, c.f.dps, c.f.evts); err != nil {
			log.Printf("E! Message Sending Error: %s", err)
		}
		c.consumer.MarkOffset(msg, "")
	}
}

func (c *consumer) consume() {
	for {
		select {
		case <-c.done:
			if err := c.consumer.Close(); err != nil {
				log.Printf("E! Error closing consumer: %s", err.Error())
			}
			log.Println("I! Consumer drained")
			c.wg.Done()
			return
		default:
			c.consumeInner()
		}
	}
}

func (c *consumer) refresh() {
	for {
		select {
		case <-c.done:
			c.wg.Done()
			log.Printf("I! Exiting consumer refresh")
			return
		case <-time.After(c.config.refreshInterval):
			valid, err := c.config.getTopicList(c.regexed)
			if err != nil {
				log.Printf("E! Error fetching new topic list! %s", err.Error())
				continue
			}
			if !reflect.DeepEqual(valid, c.topics) {
				log.Printf("I! New topics detected")
				c.replaceConsumer(valid)
			}
		}
	}
}

func (c *consumer) replaceConsumer(valid []string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.consumer.Close(); err != nil {
		log.Printf("E! Error closing consumer: %s", err.Error())
	}

	newConsumer, err := c.config.getClusterConsumer(valid, "oldest") // must do oldest here so we don't miss anything
	if err != nil {
		log.Printf("E! Error creating new consumer! %s", err.Error())
		return
	}
	c.consumer = newConsumer
	c.topics = valid
	c.in = c.consumer.Messages()
	c.errs = c.consumer.Errors()
}

func (c *consumer) close() {
	close(c.done)
	c.wg.Wait()
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

func newConsumer(c *config, f *signalfxForwarder) (*consumer, error) {
	regexed, err := regexp.Compile(c.topicPattern)
	if err != nil {
		return nil, err
	}

	valid, err := c.getTopicList(regexed)

	if err != nil {
		return nil, err
	}

	clusterConsumer, err := c.getClusterConsumer(valid, c.offset)

	if err != nil {
		return nil, err
	}
	parser, err := parsers.NewInfluxParser()

	if err != nil {
		return nil, err
	}

	consumer := &consumer{
		consumer: clusterConsumer,
		config:   c,
		topics:   valid,
		in:       clusterConsumer.Messages(),
		errs:     clusterConsumer.Errors(),
		parser:   parser,
		done:     make(chan struct{}),
		regexed:  regexed,
		f:        f,
		tsfx:     newTelegraf(),
	}

	go consumer.consume()
	go consumer.refresh()
	consumer.wg.Add(2)

	return consumer, nil
}