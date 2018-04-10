package main

import (
	"errors"
	"flag"
	"github.com/Shopify/sarama"
	"github.com/influxdata/telegraf/logger"
	"github.com/influxdata/telegraf/plugins/parsers"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/httpdebug"
	"github.com/signalfx/sarama-cluster"
	"log"
	"net"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

const version = "0.1.4"

type clusterConsumer interface {
	MarkOffset(*sarama.ConsumerMessage, string)
	Close() error
	Messages() <-chan *sarama.ConsumerMessage
	Errors() <-chan error
}

type saramaClient interface {
	Close() error
	Topics() ([]string, error)
}

type config struct {
	kafkaBroker           string
	consumerGroup         string
	topicPattern          string
	sfxEndpoint           string
	sfxToken              string
	offset                string
	logFile               string
	debugServer           string
	refreshInterval       time.Duration
	numDrainThreads       int
	channelSize           int
	batchSize             int
	useHashing            bool
	debug                 bool
	sendMetrics           bool
	metricInterval        time.Duration
	newClientConstructor  func(addrs []string, conf *sarama.Config) (saramaClient, error)
	newClusterConstructor func(addrs []string, groupID string, topics []string, config *cluster.Config) (clusterConsumer, error)
	parserConstructor     func() (parsers.Parser, error)
}

var errorRequiredOptions = errors.New("options KafkaBroker and SfxToken are required")

var instanceConfig *config

func getConfig() (*config, error) {
	if instanceConfig != nil {
		return instanceConfig, nil
	}
	kafkaBroker := flag.String("KafkaBroker", "", "Kafka Broker to connect to (required to be set)")
	consumerGroup := flag.String("KafkaGroup", "default_kafka_consumer_group", "Kafka Consumer Group to be part")
	topicPattern := flag.String("KafkaTopicPattern", "", "Kafka Topic Pattern to listen on")
	sfxEndpoint := flag.String("SfxEndpoint", "https://ingest.aws1.signalfx.com", "SignalFx endpoint to talk to")
	sfxToken := flag.String("SfxToken", "", "SignalFx Ingest API Token to use (required to be set)")
	offset := flag.String("KafkaOffsetMode", "newest", "Whether to start from reading oldest offset, or newest")
	debug := flag.Bool("Debug", false, "Turn debug on")
	logFile := flag.String("LogFile", "", "Log file to use (default stdout)")
	refreshInterval := flag.Duration("RefreshInterval", time.Second*10, "Refresh interval for kafka topics")
	numDrainThreads := flag.Int("NumDrainThreads", 10, "Number of threads draining to SignalFx")
	channelSize := flag.Int("ChannelSize", 100000, "Channel size per drain to SignalFx")
	batchSize := flag.Int("BatchSize", 5000, "Max batch size to send to SignalFx")
	useHashing := flag.Bool("UseHashing", true, "Has the datapoint to a particular channel")
	debugServer := flag.String("DebugServer", "", "Put up a debug server at the address specified")
	sendMetrics := flag.Bool("SendMetrics", true, "Self report metrics")
	flag.Parse()
	c := &config{
		kafkaBroker:     *kafkaBroker,
		consumerGroup:   *consumerGroup,
		topicPattern:    *topicPattern,
		sfxEndpoint:     *sfxEndpoint,
		sfxToken:        *sfxToken,
		offset:          *offset,
		debug:           *debug,
		logFile:         *logFile,
		refreshInterval: *refreshInterval,
		numDrainThreads: *numDrainThreads,
		channelSize:     *channelSize,
		batchSize:       *batchSize,
		useHashing:      *useHashing,
		debugServer:     *debugServer,
		sendMetrics:     *sendMetrics,
		metricInterval:  time.Second * 10,
	}
	instanceConfig = c
	return c, c.postConfig()
}

func (c *config) postConfig() error {
	if c.kafkaBroker == "" || c.sfxToken == "" {
		return errorRequiredOptions
	}
	c.newClientConstructor = func(addrs []string, conf *sarama.Config) (saramaClient, error) {
		return sarama.NewClient(addrs, conf)
	}
	c.parserConstructor = parsers.NewInfluxParser
	c.newClusterConstructor = func(addrs []string, groupID string, topics []string, config *cluster.Config) (clusterConsumer, error) {
		return cluster.NewConsumer(addrs, groupID, topics, config)
	}
	return nil
}

func (c *config) getClusterConsumer(valid []string, offset string) (clusterConsumer, error) {
	clusterConfig := cluster.NewConfig()
	clusterConfig.Consumer.Return.Errors = true
	clusterConfig.Consumer.Offsets.Initial = c.getOffset()
	clusterConsumer, err := c.newClusterConstructor(
		[]string{c.kafkaBroker},
		c.consumerGroup,
		valid,
		clusterConfig,
	)
	log.Printf("I! Topics being monitored: %s", valid)
	return clusterConsumer, err
}

func (c *config) getOffset() int64 {
	switch strings.ToLower(c.offset) {
	case "oldest", "":
		return sarama.OffsetOldest
	case "newest":
	default:
	}
	return sarama.OffsetNewest
}

func (c *config) getTopicList(regexed *regexp.Regexp) (valid []string, err error) {
	clientConfig := sarama.NewConfig()
	clientConfig.Consumer.Offsets.Initial = c.getOffset()
	client, err := c.newClientConstructor([]string{c.kafkaBroker}, clientConfig)
	if err == nil {
		defer func() {
			logIfErr("E! error closing client! %s", client.Close())
		}()

		topics, err := client.Topics()
		if err == nil {
			valid = make([]string, 0)
			for _, k := range topics {
				if regexed.Match([]byte(k)) {
					valid = append(valid, k)
				}
			}
			sort.Strings(valid)
		}
	}
	return
}

type kafkaConsumer struct {
	done                chan struct{}
	f                   *signalfxForwarder
	c                   *consumer
	debugServer         *httpdebug.Server
	debugServerListener net.Listener
	sigs                chan os.Signal
}

func start(config *config) (k *kafkaConsumer, err error) {
	f := newSignalFxForwarder(config)

	var c *consumer
	c, err = newConsumer(config, f.dps, f.evts)
	if err == nil {
		done := make(chan struct{})
		logger.SetupLogging(c.config.debug, false, c.config.logFile)
		k = &kafkaConsumer{
			f:    f,
			c:    c,
			done: done,
			sigs: make(chan os.Signal, 1),
		}
		if config.debugServer != "" {
			listener, err := net.Listen("tcp", config.debugServer)
			if err == nil {
				k.debugServerListener = listener
				k.debugServer = httpdebug.New(&httpdebug.Config{
					Logger:        nil,
					ExplorableObj: k,
				})
				go func() {
					logIfErr("Finished listening on debug server %s", k.debugServer.Serve(listener))
				}()
			}
			logIfErr("E! cannot setup debug server %s", err)
		}
		if config.sendMetrics {
			go k.metrics(config.metricInterval)
		}
	}
	return k, err
}

func (k *kafkaConsumer) metrics(t time.Duration) {
	for {
		select {
		case <-k.done:
			return
		case <-time.After(t):
			dps := k.Datapoints()
			for _, d := range dps {
				k.f.dps <- d
			}
		}
	}
}

func (k *kafkaConsumer) wait() {
	signal.Notify(k.sigs, syscall.SIGTERM)
	go func() {
		sig := <-k.sigs
		log.Printf("I! Caught the %s signal, draining consumer", sig)
		k.c.close()
		log.Printf("I! Done draining consumer, now draining forwarder")
		k.f.close()
		close(k.done)
	}()
	log.Println("I! Awaiting metrics")
	<-k.done
	log.Println("I! Exiting")
}

func (k *kafkaConsumer) Datapoints() []*datapoint.Datapoint {
	dps := k.f.Datapoints()
	dps = append(dps, k.c.Datapoints()...)
	return dps
}

var mainInstance *kafkaConsumer
var setup int64

func logIfErr(format string, err error) {
	if err != nil {
		log.Printf(format, err.Error())
	}
}

func main() {
	log.Printf("I! Running version %s", version)
	config, err := getConfig()
	logIfErr("E! Unable to initialize config: %s", err)
	mainInstance, err = start(config)
	logIfErr("E! Unable to create instance config: %s", err)
	atomic.StoreInt64(&setup, 1)
	mainInstance.wait()
}
