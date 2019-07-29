package main

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/event"
	"github.com/signalfx/golib/sfxclient"
	"log"
	"strconv"
	"sync/atomic"
)

type parser interface {
	parse(msg []byte, dps chan *datapoint.Datapoint, events chan *event.Event) (int, error)
}

type consumer struct {
	consumer consumerGroup
	config   *config
	parser   parser
	dps      chan *datapoint.Datapoint
	evts     chan *event.Event
	id       int

	stats struct {
		numMessages          int64
		numMetricsParsed     int64
		numErrs              int64
		numReplacements      int64
		numParseErrs         int64
		numTelegrafParseErrs int64
	}
	cancel context.CancelFunc
}

func (*consumer) Setup(sess sarama.ConsumerGroupSession) error {
	log.Printf("I! Starting consumer for claims %v", sess.Claims())
	return nil
}

func (*consumer) Cleanup(sess sarama.ConsumerGroupSession) error {
	log.Printf("I! Cleaning up consumer for claims %v", sess.Claims())
	return nil
}

func (c *consumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		atomic.AddInt64(&c.stats.numMessages, 1)

		if numMetrics, err := c.parser.parse(msg.Value, c.dps, c.evts); err == nil {
			atomic.AddInt64(&c.stats.numMetricsParsed, int64(numMetrics))
		} else {
			atomic.AddInt64(&c.stats.numParseErrs, 1)
			log.Printf("E! Message Parse Error\nmessage: %v\nerror: %s", msg, err)
		}

		sess.MarkMessage(msg, "")
	}

	return nil
}

func (c *consumer) close() error {
	if err := c.consumer.Close(); err != nil {
		log.Printf("E! Error closing consumer: %s", err.Error())
		return err
	}

	log.Printf("I! Waiting for consumer to stop")
	return nil
}

func (c *consumer) Datapoints() []*datapoint.Datapoint {
	dims := map[string]string{"path": "kafka_consumer", "obj": "consumer", "consumer_id": strconv.Itoa(c.id)}
	dps := []*datapoint.Datapoint{
		sfxclient.CumulativeP("total_messages_received", dims, &c.stats.numMessages),
		sfxclient.CumulativeP("total_messages_parsed", dims, &c.stats.numMetricsParsed),
		sfxclient.CumulativeP("total_errors_received", dims, &c.stats.numErrs),
		sfxclient.CumulativeP("total_parse_errors_received", dims, &c.stats.numParseErrs),
	}
	return dps
}

func newConsumer(ctxt context.Context, c *config, id int, topics []string, dps chan *datapoint.Datapoint, evts chan *event.Event) (*consumer, error) {
	log.Printf("I! Topics being monitored: %s", topics)

	cc, err := c.getConsumerGroup(c.offset, hostname + "-" + strconv.Itoa(id))
	if err != nil {
		return nil, err
	}

	parser, err := c.getParser()
	if err != nil {
		return nil, err
	}

	consumer := &consumer{
		consumer: cc,
		config:   c,
		parser:   parser,
		dps:  dps,
		evts: evts,
		id: id,
	}

	go func() {
		for {
			if err := cc.Consume(ctxt, topics, consumer); err != nil {
				if err := cc.Close(); err != nil {
					log.Printf("W! unable to close consumer on Consume error: %s", err)
				}
				log.Printf("E! Consume returned error: %s", err)
			}

			// TODO: check that this was actually implemented and canceled
			if ctxt.Err() != nil {
				return
			}
		}
	}()

	go func() {
		for err := range cc.Errors() {
			atomic.AddInt64(&consumer.stats.numErrs, 1)
			if err != nil {
				log.Printf("E! consumer Error: %s", err.Error())
			}
		}
	}()

	return consumer, nil
}
