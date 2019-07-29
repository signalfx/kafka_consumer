package main

import (
	"context"
	"errors"
	"github.com/Shopify/sarama"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/parsers"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/event"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var testConfig *testingConfig

type testSaramaClient struct {
	topics []string
	err    error
	mu     sync.Mutex
}

func (t *testSaramaClient) setTopics(topics []string) {
	t.mu.Lock()
	t.topics = topics
	t.mu.Unlock()
}

func (t *testSaramaClient) setError(err error) {
	t.mu.Lock()
	t.err = err
	t.mu.Unlock()
}

func (t *testSaramaClient) Close() error {
	return t.err
}

func (t *testSaramaClient) Topics() ([]string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.topics, t.err
}

type testCluster struct {
	msgs chan *sarama.ConsumerMessage
	errs chan error
	err  error
	mu   sync.Mutex
}

func (t *testCluster) Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	//panic("implement me")
	return nil
}

func (t *testCluster) MarkOffset(*sarama.ConsumerMessage, string) {
}

func (t *testCluster) setError(err error) {
	t.mu.Lock()
	t.err = err
	t.mu.Unlock()
}

func (t *testCluster) Close() error {
	return t.err
}

func (t *testCluster) Messages() <-chan *sarama.ConsumerMessage {
	return t.msgs
}

func (t *testCluster) Errors() <-chan error {
	return t.errs
}

type testParser struct {
	p   parsers.Parser
	err error
	mu  sync.Mutex
}

func (t *testParser) getError() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.err
}

func (t *testParser) setError(err error) {
	t.mu.Lock()
	t.err = err
	t.mu.Unlock()
}

func (t *testParser) Parse(buf []byte) ([]telegraf.Metric, error) {
	if err := t.getError(); err != nil {
		return nil, err
	}
	return t.p.Parse(buf)
}

func (t *testParser) ParseLine(line string) (telegraf.Metric, error) {
	if err := t.getError(); err != nil {
		return nil, err
	}
	return t.p.ParseLine(line)
}

func (t *testParser) SetDefaultTags(tags map[string]string) {
}

type testingConfig struct {
	config
	client   *testSaramaClient
	tcluster *testCluster
	tParser  *testParser
}

func getTestConfig(t *testing.T) *testingConfig {
	if testConfig == nil {
		os.Args = append(os.Args, "-KafkaBroker", "0.0.0.0:0", "-SfxToken", "TOKEN", "-SfxEndpoint", "localhost:-1")
		tc, err := getConfig()
		testConfig = &testingConfig{
			config: *tc,
		}
		assert.Nil(t, err)
	}
	testConfig.batchSize = 1
	testConfig.metricInterval = time.Millisecond * 10
	testConfig.debugServer = "0.0.0.0:0"
	testConfig.client = &testSaramaClient{}
	testConfig.tcluster = &testCluster{
		msgs: make(chan *sarama.ConsumerMessage, 10),
		errs: make(chan error, 10),
	}
	p, _ := parsers.NewInfluxParser()
	testConfig.tParser = &testParser{
		p: p,
	}
	instanceConfig = &testConfig.config
	instanceConfig.newClientConstructor = func(addrs []string, conf *sarama.Config) (saramaClient, error) {
		return testConfig.client, nil
	}
	instanceConfig.newClusterConstructor = func(addrs []string, groupID string, config *sarama.Config) (sarama.ConsumerGroup, error) {
		return testConfig.tcluster, nil
	}
	instanceConfig.parserConstructor = func() (parsers.Parser, error) {
		return testConfig.tParser, nil
	}
	return testConfig
}

func TestConsumer(t *testing.T) {
	Convey("test consumer fail", t, func() {
		config := getTestConfig(t)
		config.parser = "unknown"
		dps := make(chan *datapoint.Datapoint, 10)
		evts := make(chan *event.Event, 10)
		_, err := newConsumer(&config.config, 0, nil, dps, evts)
		So(err, ShouldNotBeNil)
		config.client.setError(nil)
		config.parser = telegrafParser
	})
	Convey("test consumer", t, func() {
		config := getTestConfig(t)
		dps := make(chan *datapoint.Datapoint, 10)
		evts := make(chan *event.Event, 10)
		c, err := newConsumer(&config.config, 0, []string{"topic"}, dps, evts)
		So(err, ShouldBeNil)
		So(c, ShouldNotBeNil)
		Convey("test err", func() {
			config.tcluster.errs <- errors.New("blarg")
			for atomic.LoadInt64(&c.stats.numErrs) == 0 {
				runtime.Gosched()
			}
		})
		Convey("test msgs", func() {
			config.tcluster.msgs <- getMessage()
			for atomic.LoadInt64(&c.stats.numMessages) == 0 {
				runtime.Gosched()
			}
			config.tParser.setError(errors.New("nope"))
			config.tcluster.msgs <- getMessage()
			for atomic.LoadInt64(&c.stats.numParseErrs) == 0 {
				runtime.Gosched()
			}
			config.tParser.setError(nil)
		})
		Convey("test datapoints", func() {
			So(len(c.Datapoints()), ShouldEqual, 4)
		})
		Reset(func() {
			if err := c.close(); err != nil {
				t.Fatal(err)
			}
		})
	})
}

func getMessage() *sarama.ConsumerMessage {
	return &sarama.ConsumerMessage{
		Value: []byte{109, 101, 116, 114, 105, 99, 66, 44, 104, 111, 115, 116, 61, 109, 121, 104, 111, 115, 116, 52, 55, 57, 57, 44, 101, 110, 118, 61, 116, 101, 115, 116, 32, 118, 97, 108, 49, 61, 56, 48, 57, 48, 44, 118, 97, 108, 50, 61, 56, 48, 56, 54, 44, 118, 97, 108, 51, 61, 51, 50, 51, 49, 55, 44, 118, 97, 108, 52, 61, 51, 48, 55, 55, 53, 44, 118, 97, 108, 53, 61, 49, 57, 56, 49, 55, 32, 49, 53, 50, 51, 50, 57, 55, 49, 50, 55, 56, 54, 51, 49, 51, 53, 50, 49, 56},
	}
}
