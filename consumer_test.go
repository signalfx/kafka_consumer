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
	msgs     chan *sarama.ConsumerMessage
	errs     chan error
	err      error
	mu       sync.Mutex
	consumer *consumer
}

type saramaSession struct {
}

func (s saramaSession) Claims() map[string][]int32 {
	panic("not implemented")
}

func (s saramaSession) MemberID() string {
	panic("not implemented")
}

func (s saramaSession) GenerationID() int32 {
	panic("not implemented")
}

func (s saramaSession) MarkOffset(topic string, partition int32, offset int64, metadata string) {
	panic("not implemented")
}

func (s saramaSession) ResetOffset(topic string, partition int32, offset int64, metadata string) {
	panic("not implemented")
}

func (s saramaSession) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {
}

func (s saramaSession) Context() context.Context {
	panic("not implemented")
}

type saramaClaim struct {
	msgs chan *sarama.ConsumerMessage
}

func (s *saramaClaim) Topic() string {
	panic("not implemented")
}

func (s *saramaClaim) Partition() int32 {
	panic("not implemented")
}

func (s *saramaClaim) InitialOffset() int64 {
	panic("not implemented")
}

func (s *saramaClaim) HighWaterMarkOffset() int64 {
	panic("not implemented")
}

func (s *saramaClaim) Messages() <-chan *sarama.ConsumerMessage {
	return s.msgs
}

func (t *testCluster) Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if err := t.consumer.ConsumeClaim(&saramaSession{}, &saramaClaim{msgs: testConfig.tcluster.msgs}); err != nil {
				panic(err)
			}
		}
	}
}

func (t *testCluster) setError(err error) {
	t.mu.Lock()
	t.err = err
	t.mu.Unlock()
}

func (t *testCluster) Close() error {
	return t.err
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
		_, err := newConsumer(context.Background(), &config.config, 0, nil, dps, evts)
		So(err, ShouldNotBeNil)
		config.client.setError(nil)
		config.parser = telegrafParser
	})
	Convey("test consumer", t, func() {
		config := getTestConfig(t)
		dps := make(chan *datapoint.Datapoint, 10)
		evts := make(chan *event.Event, 10)
		c, err := newConsumer(context.Background(), &config.config, 0, []string{"topic"}, dps, evts)
		config.tcluster.consumer = c
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
		Value: []byte("metricB,host=myhost4799,env=test val1=8090,val2=8086,val3=32317,val4=30775,val5=19817 1523297127863135218"),
	}
}
