# Kafka consumer of telegraf data to SignalFx

```
Usage of ./kafka_consumer:
  -BatchSize int
    	Max batch size to send to SignalFx (default 5000)
  -ChannelSize int
    	Channel size per drain to SignalFx (default 100000)
  -Debug
    	Turn debug on
  -KafkaBroker string
    	Kafka Broker to connect to (required to be set)
  -KafkaGroup string
    	Kafka Consumer Group to be part (default "default_kafka_consumer_group")
  -KafkaOffsetMode string
    	Whether to start from reading oldest offset, or newest (default "newest")
  -KafkaTopicPattern string
    	Kafka Topic Pattern to listen on
  -LogFile string
    	Log file to use (default stdout)
  -NumDrainThreads int
    	Number of threads draining to SignalFx (default 10)
  -RefreshInterval duration
    	Refresh interval for kafka topics (default 10s)
  -SfxEndpoint string
    	SignalFx endpoint to talk to (default "https://ingest.aws1.signalfx.com")
  -SfxToken string
    	SignalFx Ingest API Token to use (required to be set)
  -UseHashing
    	Has the datapoint to a particular channel (default true)
```
