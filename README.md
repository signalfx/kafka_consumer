>ℹ️&nbsp;&nbsp;SignalFx was acquired by Splunk in October 2019. See [Splunk SignalFx](https://www.splunk.com/en_us/investor-relations/acquisitions/signalfx.html) for more information.

## Kafka consumer of Telegraf or JSON data to SignalFx

```
Usage of ./kafka_consumer:
  -BatchSize int
    	Max batch size to send to SignalFx (default 5000)
  -ChannelSize int
    	Channel size per drain to SignalFx (default 100000)
  -Debug
    	Turn debug on
  -DebugServer string
    	Put up a debug server at the address specified
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
  -NumConsumers uint
    	Default number of Kafka consumers to run concurrently (default 1)
  -NumDrainThreads int
    	Number of threads draining to SignalFx (default 10)
  -Parser string
    	Parser for incoming messages (json or telegraf) (default "json")
  -SendMetrics
    	Self report metrics (default true)
  -SfxEndpoint string
    	SignalFx endpoint to talk to (default "https://ingest.us0.signalfx.com")
  -SfxToken string
    	SignalFx Ingest API Token to use (required to be set)
  -UseHashing
    	Hash the datapoint to a particular channel (default true)
  -Writer string
    	Location to send metrics (null, stdout, or signalfx) (default "signalfx")
```
