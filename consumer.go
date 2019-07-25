package main

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/event"
	"github.com/signalfx/golib/sfxclient"
	"log"
	"sync/atomic"
)

type parser interface {
	parse(msg []byte, dps chan *datapoint.Datapoint, events chan *event.Event) (int, error)
}

//type consumerHandler struct {
//
//}

//func (*consumerHandler) Setup(sarama.ConsumerGroupSession) error {
//	//panic("implement me")
//}
//
//func (*consumerHandler) Cleanup(sarama.ConsumerGroupSession) error {
//	//panic("implement me")
//}

//func (c *consumerHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
//	for msg := range claim.Messages() {
//		atomic.AddInt64(&c.stats.numMessages, 1)
//
//		if numMetrics, err := c.parser.parse(msg.Value, c.dps, c.evts); err == nil {
//			atomic.AddInt64(&c.stats.numMetricsParsed, int64(numMetrics))
//		} else {
//			atomic.AddInt64(&c.stats.numParseErrs, 1)
//			log.Printf("E! Message Parse Error\nmessage: %v\nerror: %s", msg, err)
//		}
//	}

	//for {
	//	select {
	//	case <-c.done:
	//		c.closeConsumer()
	//		log.Println("I! Consumer drained")
	//		c.wg.Done()
	//		return
	//	case <-c.replacements:
	//		logIfErr("E! Error fetching new topic list! %s", c.refreshInternal())
	//	case msg := <-c.consumer.Messages():
	//		atomic.AddInt64(&c.stats.numMessages, 1)
	//
	//		if numMetrics, err := c.parser.parse(msg.Value, c.dps, c.evts); err == nil {
	//			atomic.AddInt64(&c.stats.numMetricsParsed, int64(numMetrics))
	//		} else {
	//			atomic.AddInt64(&c.stats.numParseErrs, 1)
	//			log.Printf("E! Message Parse Error\nmessage: %v\nerror: %s", msg, err)
	//		}
	//
	//		c.consumer.MarkOffset(msg, "")
	//	case err := <-c.consumer.Errors():
	//		atomic.AddInt64(&c.stats.numErrs, 1)
	//		if err != nil {
	//			log.Printf("E! consumer Error: %s\n", err.Error())
	//		}
	//	}
	//}
	//}

type consumer struct {
	consumer consumerGroup
	config   *config
	//done  chan struct{}
	//wg    sync.WaitGroup
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

func (*consumer) Setup(sarama.ConsumerGroupSession) error {
	//panic("implement me")
	return nil
}

func (*consumer) Cleanup(sarama.ConsumerGroupSession) error {
	//panic("implement me")
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
	dims := map[string]string{"path": "kafka_consumer", "obj": "consumer"}
	dps := []*datapoint.Datapoint{
		sfxclient.CumulativeP("total_messages_received", dims, &c.stats.numMessages),
		sfxclient.CumulativeP("total_messages_parsed", dims, &c.stats.numMetricsParsed),
		sfxclient.CumulativeP("total_errors_received", dims, &c.stats.numErrs),
		sfxclient.CumulativeP("total_parse_errors_received", dims, &c.stats.numParseErrs),
	}
	return dps
}


func newConsumer(c *config, topics []string, dps chan *datapoint.Datapoint, evts chan *event.Event) (*consumer, error) {
	log.Printf("I! Topics being monitored: %s", topics)

	cc, err := c.getConsumerGroup(c.offset)
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
		parser:       parser,
		//done:         make(chan struct{}),
		dps:          dps,
		evts:         evts,
	}

	if err := cc.Consume(context.Background(), topics, consumer); err != nil {
		if err := cc.Close(); err != nil {
			log.Printf("W! unable to close consumer on Consume error: %s", err)
		}
		return nil, err
	}

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
