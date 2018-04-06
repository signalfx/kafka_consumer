package main

import (
	"errors"
	"flag"
	"github.com/Shopify/sarama"
	"github.com/influxdata/telegraf/logger"
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
	"syscall"
	"time"
)

const version = "0.1.3"

type config struct {
	kafkaBroker     string
	consumerGroup   string
	topicPattern    string
	sfxEndpoint     string
	sfxToken        string
	offset          string
	logFile         string
	debugServer     string
	refreshInterval time.Duration
	numDrainThreads int
	channelSize     int
	batchSize       int
	useHashing      bool
	debug           bool
	sendMetrics     bool
}

var errorRequiredOptions = errors.New("options KafkaBroker and SfxToken are required")

func getConfig() (*config, error) {
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
	c := config{
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
	}
	if c.kafkaBroker == "" || c.sfxToken == "" {
		return nil, errorRequiredOptions
	}
	return &c, nil
}

func (c *config) getClusterConsumer(valid []string, offset string) (*cluster.Consumer, error) {
	clusterConfig := cluster.NewConfig()
	clusterConfig.Consumer.Return.Errors = true
	clusterConfig.Consumer.Offsets.Initial = c.getOffset()
	clusterConsumer, err := cluster.NewConsumer(
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

func (c *config) getTopicList(regexed *regexp.Regexp) ([]string, error) {
	clientConfig := sarama.NewConfig()
	clientConfig.Consumer.Offsets.Initial = c.getOffset()
	client, err := sarama.NewClient([]string{c.kafkaBroker}, clientConfig)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Printf("E! error closing client! %s", err.Error())
		}
	}()

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

type kafkaConsumer struct {
	done                chan struct{}
	f                   *signalfxForwarder
	c                   *consumer
	debugServer         *httpdebug.Server
	debugServerListener net.Listener
}

func start(config *config) *kafkaConsumer {
	f := newSignalFxForwarder(config)

	c, err := newConsumer(config, f)
	if err != nil {
		log.Fatalf("E! Unable to create consumer: %s", err.Error())
	}
	done := make(chan struct{})
	logger.SetupLogging(c.config.debug, false, c.config.logFile)
	k := &kafkaConsumer{
		f:    f,
		c:    c,
		done: done,
	}
	if config.debugServer != "" {
		listener, err := net.Listen("tcp", config.debugServer)
		if err != nil {
			log.Fatalf("E! cannot setup debug server %s", err.Error())
		}
		k.debugServerListener = listener
		k.debugServer = httpdebug.New(&httpdebug.Config{
			Logger:        nil,
			ExplorableObj: k,
		})
		go func() {
			err := k.debugServer.Serve(listener)
			if err != nil {
				log.Printf("Finished listening on debug server %s", err.Error())
			}
		}()
	}
	if config.sendMetrics {
		go k.metrics()
	}
	return k
}

func (k *kafkaConsumer) metrics() {
	for {
		select {
		case <-k.done:
			return
		case <-time.After(time.Second * 10):
			dps := k.Datapoints()
			for _, d := range dps {
				k.f.dps <- d
			}
		}
	}
}

func (k *kafkaConsumer) wait() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)
	go func() {
		sig := <-sigs
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

func main() {
	log.Printf("I! Running version %s", version)
	config, err := getConfig()
	if err != nil {
		log.Fatalf("E! Unable to initialize config: %s", err.Error())
	}
	k := start(config)
	k.wait()
}
