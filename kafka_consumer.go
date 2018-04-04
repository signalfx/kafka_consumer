package main

import (
	"errors"
	"flag"
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/influxdata/telegraf/logger"
	"log"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strings"
	"syscall"
	"time"
)

type config struct {
	kafkaBroker     string
	consumerGroup   string
	topicPattern    string
	sfxEndpoint     string
	sfxToken        string
	offset          string
	logFile         string
	refreshInterval time.Duration
	numDrainThreads int
	channelSize     int
	batchSize       int
	useHashing      bool
	debug           bool
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
	}
	if c.kafkaBroker == "" || c.sfxToken == "" {
		return nil, errorRequiredOptions
	}
	return &c, nil
}

func (c *config) getClusterConsumer(valid []string, offset string) (*cluster.Consumer, error) {
	clusterConfig := cluster.NewConfig()
	clusterConfig.Consumer.Return.Errors = true
	switch strings.ToLower(offset) {
	case "oldest", "":
		clusterConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	case "newest":
		clusterConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	default:
		log.Printf("WARNING: Kafka offset specified invalid '%s', using 'newest'\n", c.offset)
		clusterConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	}
	clusterConsumer, err := cluster.NewConsumer(
		[]string{c.kafkaBroker},
		c.consumerGroup,
		valid,
		clusterConfig,
	)
	log.Printf("I! Topics being monitored: %s", valid)
	return clusterConsumer, err
}

func getTopicList(kafkaBroker string, regexed *regexp.Regexp) ([]string, error) {
	clientConfig := sarama.NewConfig()
	client, err := sarama.NewClient([]string{kafkaBroker}, clientConfig)
	if err != nil {
		return nil, err
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

func main() {
	config, err := getConfig()
	if err != nil {
		log.Fatalf("E! Unable to initialize config: %s", err.Error())
	}

	f := newSignalFxForwarder(config)

	c, err := newConsumer(config, f)
	if err != nil {
		log.Fatalf("E! Unable to create consumer: %s", err.Error())
	}
	done := make(chan struct{})
	logger.SetupLogging(c.config.debug, false, c.config.logFile)
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		log.Printf("I! Caught the %s signal, draining consumer", sig)
		c.close()
		log.Printf("I! Done draining consumer, now draining forwarder")
		f.close()
		close(done)
	}()
	log.Println("I! Awaiting metrics")
	<-done
	log.Println("I! Exiting")
}
