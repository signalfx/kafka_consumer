package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/influxdata/telegraf/logger"
	"github.com/influxdata/telegraf/plugins/parsers"
	"github.com/rcrowley/go-metrics"
	"github.com/rcrowley/go-metrics/exp"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/event"
	"github.com/signalfx/golib/httpdebug"
	"github.com/signalfx/golib/sfxclient"
	"github.com/signalfx/telegraf/plugins/outputs/signalfx"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

const version = "0.2"

type consumerGroup interface {
	Close() error
	Errors() <-chan error
}

type saramaClient interface {
	Close() error
	Topics() ([]string, error)
}

const (
	jsonParser     = "json"
	telegrafParser = "telegraf"
)

const (
	signalfxOutput = "signalfx"
	nullOutput     = "null"
	stdoutOutput   = "stdout"
)

type config struct {
	kafkaBroker   string
	consumerGroup string
	topicPattern  string
	sfxEndpoint   string
	sfxToken      string
	offset        string
	logFile       string
	debugServer   string
	parser        string
	writer        string
	//refreshInterval       time.Duration
	numDrainThreads       int
	channelSize           int
	batchSize             int
	useHashing            bool
	debug                 bool
	sendMetrics           bool
	metricInterval        time.Duration
	newClientConstructor  func(addrs []string, conf *sarama.Config) (saramaClient, error)
	newClusterConstructor func(addrs []string, groupID string, config *sarama.Config) (sarama.ConsumerGroup, error)
	parserConstructor     func() (parsers.Parser, error)
	numConsumers          uint
}

var errorRequiredOptions = errors.New("options KafkaBroker and SfxToken are required")

var instanceConfig *config

var hostname string

var pid = strconv.Itoa(os.Getpid())

func init() {
	var err error
	hostname, err = os.Hostname()
	if err != nil {
		log.Fatalf("unable to determine hostname: %s", err)
	}
}

func getConfig() (*config, error) {
	if instanceConfig != nil {
		return instanceConfig, nil
	}
	kafkaBroker := flag.String("KafkaBroker", "", "Kafka Broker to connect to (required to be set)")
	consumerGroup := flag.String("KafkaGroup", "default_kafka_consumer_group", "Kafka Consumer Group to be part")
	topicPattern := flag.String("KafkaTopicPattern", "", "Kafka Topic Pattern to listen on")
	sfxEndpoint := flag.String("SfxEndpoint", "https://ingest.us0.signalfx.com", "SignalFx endpoint to talk to")
	sfxToken := flag.String("SfxToken", "", "SignalFx Ingest API Token to use (required to be set)")
	offset := flag.String("KafkaOffsetMode", "newest", "Whether to start from reading oldest offset, or newest")
	debug := flag.Bool("Debug", false, "Turn debug on")
	logFile := flag.String("LogFile", "", "Log file to use (default stdout)")
	//refreshInterval := flag.Duration("RefreshInterval", time.Second*10, "Refresh interval for kafka topics")
	numDrainThreads := flag.Int("NumDrainThreads", 10, "Number of threads draining to SignalFx")
	channelSize := flag.Int("ChannelSize", 100000, "Channel size per drain to SignalFx")
	batchSize := flag.Int("BatchSize", 5000, "Max batch size to send to SignalFx")
	useHashing := flag.Bool("UseHashing", true, "Hash the datapoint to a particular channel")
	debugServer := flag.String("DebugServer", "", "Put up a debug server at the address specified")
	sendMetrics := flag.Bool("SendMetrics", true, "Self report metrics")
	parser := flag.String("Parser", "json", "Parser for incoming messages (json or telegraf)")
	writer := flag.String("Writer", "signalfx", "Location to send metrics (null, stdout, or signalfx)")
	numConsumers := flag.Uint("NumConsumers", 1, "Default number of Kafka consumers to run concurrently")

	flag.Parse()
	c := &config{
		kafkaBroker:   *kafkaBroker,
		consumerGroup: *consumerGroup,
		topicPattern:  *topicPattern,
		sfxEndpoint:   *sfxEndpoint,
		sfxToken:      *sfxToken,
		offset:        *offset,
		debug:         *debug,
		logFile:       *logFile,
		//refreshInterval: *refreshInterval,
		numDrainThreads: *numDrainThreads,
		channelSize:     *channelSize,
		batchSize:       *batchSize,
		useHashing:      *useHashing,
		debugServer:     *debugServer,
		sendMetrics:     *sendMetrics,
		metricInterval:  time.Second * 10,
		parser:          *parser,
		writer:          *writer,
		numConsumers:    *numConsumers,
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
	c.newClusterConstructor = func(addrs []string, groupID string, config *sarama.Config) (sarama.ConsumerGroup, error) {
		return sarama.NewConsumerGroup(addrs, groupID, config)
	}
	return nil
}

func (c *config) getParser() (parser, error) {
	switch c.parser {
	case jsonParser:
		return &json{}, nil
	case telegrafParser:
		tparse, err := c.parserConstructor()
		if err != nil {
			return nil, err
		}
		return &telegrafP{parser: tparse, sfx: signalfx.NewSignalFx()}, nil
	}

	return nil, fmt.Errorf("unknown parser %s", c.parser)
}

func (c *config) getWriter(dps chan *datapoint.Datapoint, evts chan *event.Event) (writer, error) {
	switch c.writer {
	case stdoutOutput:
		return newStdoutWriter(dps, evts), nil
	case nullOutput:
		return newNullWriter(dps, evts), nil
	case signalfxOutput:
		return newSignalFxForwarder(c, dps, evts), nil
	}

	return nil, fmt.Errorf("unknown writer %s", c.writer)
}

func (c *config) getConsumerGroup(offset string, clientId string) (sarama.ConsumerGroup, error) {
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.ClientID = clientId
	kafkaConfig.Version = sarama.V0_10_2_0
	kafkaConfig.Consumer.Return.Errors = true
	kafkaConfig.Consumer.Offsets.Initial = c.getOffset()
	kafkaConfig.MetricRegistry = metrics.DefaultRegistry
	clusterConsumer, err := c.newClusterConstructor(
		[]string{c.kafkaBroker},
		c.consumerGroup,
		kafkaConfig,
	)
	return clusterConsumer, err
}

func (c *config) getOffset() int64 {
	// NOTE: this doesn't reset offsets for existing consumer group.
	switch strings.ToLower(c.offset) {
	case "oldest", "":
		return sarama.OffsetOldest
	case "newest":
	default:
	}
	return sarama.OffsetNewest
}

func (c *config) getTopicList() ([]string, error) {
	clientConfig := sarama.NewConfig()
	clientConfig.Consumer.Offsets.Initial = c.getOffset()
	client, err := c.newClientConstructor([]string{c.kafkaBroker}, clientConfig)
	if err != nil {
		return nil, err
	}

	defer func() {
		logIfErr("E! error closing client! %s", client.Close())
	}()

	regexed, err := regexp.Compile(c.topicPattern)
	if err != nil {
		return nil, fmt.Errorf("unable to compiled topic pattern %s: %s", c.topicPattern, err)
	}

	topics, err := client.Topics()
	if err != nil {
		return nil, err
	}

	valid := make([]string, 0)
	for _, k := range topics {
		if regexed.Match([]byte(k)) {
			valid = append(valid, k)
		}
	}
	sort.Strings(valid)

	return valid, nil
}

// configureHTTPSink with auth and endpoints from config
func (c *config) configureHTTPSink(sink *sfxclient.HTTPSink) {
	sink.AuthToken = c.sfxToken
	sink.DatapointEndpoint = c.sfxEndpoint + "/v2/datapoint"
	sink.EventEndpoint = c.sfxEndpoint + "/v2/event"
}

type kafkaConsumer struct {
	done                chan struct{}
	writer              writer
	consumers           []*consumer
	debugServer         *httpdebug.Server
	debugServerListener net.Listener
	sigs                chan os.Signal
	internalMetrics     *sfxclient.HTTPSink
	cancel              context.CancelFunc
}

func start(config *config) (*kafkaConsumer, error) {
	dps := make(chan *datapoint.Datapoint, config.channelSize*config.numDrainThreads)
	evts := make(chan *event.Event, config.channelSize)
	ctxt, cancel := context.WithCancel(context.Background())

	defer func() {
		// Cancel on error (non-nil).
		if cancel != nil {
			cancel()
		}
	}()

	topics, err := config.getTopicList()
	if err != nil {
		return nil, err
	}

	var consumers []*consumer
	for i := 0; i < int(config.numConsumers); i++ {
		c, err := newConsumer(ctxt, config, i, topics, dps, evts)
		if err != nil {
			for _, c := range consumers {
				c.close()
			}
			return nil, err
		}
		consumers = append(consumers, c)
	}

	writer, err := config.getWriter(dps, evts)
	if err != nil {
		return nil, err
	}

	done := make(chan struct{})
	logger.SetupLogging(config.debug, false, config.logFile)

	k := &kafkaConsumer{
		writer:          writer,
		consumers:       consumers,
		done:            done,
		sigs:            make(chan os.Signal, 1),
		internalMetrics: createInternalMetrics(config),
		cancel:          cancel,
	}
	if config.debugServer != "" {
		listener, err := net.Listen("tcp", config.debugServer)
		if err == nil {
			k.debugServerListener = listener
			k.debugServer = httpdebug.New(&httpdebug.Config{
				Logger:        nil,
				ExplorableObj: k,
			})
			k.debugServer.Mux.Handle("/debug/metrics", exp.ExpHandler(metrics.DefaultRegistry))

			go func() {
				logIfErr("Finished listening on debug server %s", k.debugServer.Serve(listener))
			}()
		}
		logIfErr("E! cannot setup debug server %s", err)
	}
	if config.sendMetrics {
		go k.metrics(config.metricInterval, dps)
	}
	// Mark as nil so it doesn't get canceled in the defer.
	cancel = nil
	return k, nil
}

func createInternalMetrics(c *config) *sfxclient.HTTPSink {
	tr := &http.Transport{
		IdleConnTimeout:    30 * time.Second,
		DisableCompression: true,
	}
	client := &http.Client{
		Transport: tr,
		Timeout:   5 * time.Second,
	}

	sink := sfxclient.NewHTTPSink()
	sink.Client = client
	c.configureHTTPSink(sink)
	return sink
}

func (k *kafkaConsumer) metrics(t time.Duration, dps chan *datapoint.Datapoint) {
	for {
		select {
		case <-k.done:
			return
		case <-time.After(t):
			metricDps := k.Datapoints()
			for _, dp := range metricDps {
				if dp.Dimensions == nil {
					dp.Dimensions = map[string]string{}
				}
				dp.Dimensions["host"] = hostname
				dp.Dimensions["pid"] = pid
			}
			err := k.internalMetrics.AddDatapoints(context.TODO(), metricDps)
			logIfErr("E! Error sending internal metrics: %s", err)
		}
	}
}

func (k *kafkaConsumer) wait() {
	signal.Notify(k.sigs, syscall.SIGTERM)
	signal.Notify(k.sigs, syscall.SIGINT)
	go func() {
		sig := <-k.sigs
		log.Printf("I! Caught the %s signal, cancelling consumers context", sig)
		k.cancel()

		log.Printf("I! Draining consumers")
		for i, c := range k.consumers {
			if err := c.close(); err != nil {
				log.Printf("E! Failed closing consumer %d: %s", i, err)
			}
		}
		log.Printf("I! Done draining consumer, now draining forwarder")
		k.writer.close()
		close(k.done)
	}()
	log.Println("I! Awaiting metrics")
	<-k.done
	log.Println("I! Exiting")
}

func (k *kafkaConsumer) Datapoints() []*datapoint.Datapoint {
	dps := k.writer.Datapoints()
	for _, c := range k.consumers {
		dps = append(dps, c.Datapoints()...)
	}
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
	if err != nil {
		os.Exit(1)
	}

	mainInstance, err = start(config)
	logIfErr("E! Unable to create instance config: %s", err)
	if err != nil {
		os.Exit(1)
	}

	atomic.StoreInt64(&setup, 1)
	mainInstance.wait()
}
