package kafka.producer;

import static java.lang.Thread.sleep;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

public class MockDatapointKafkaProducer {
    private final static String NUM_METRICS = "KAFKA_CONSUMER_NUM_METRICS";
    private final static String BROKER_IP = "KAFKA_CONSUMER_BROKER_IP";
    private final static String TOPIC_NAME = "KAFKA_CONSUMER_TOPIC_NAME";
    private final static String NUM_THREADS = "KAFKA_CONSUMER_NUM_THREADS";
    private final static String LINGER_MS = "KAFKA_CONSUMER_LINGER_MS";
    private final static String PRODUCER_BATCH_SIZE = "KAFKA_CONSUMER_PRODUCER_BATCH_SIZE";
    private static String brokerIP = "localhost";
    private static String topicName = "benchmark2";
    private static int concurrency = 100;
    private static int batcheSize = 100000;

    public static void main(String args[]) {
        AtomicInteger totalSent = new AtomicInteger(0);

        DataGenerator dataGenerator = configureDataGenerator();
        Properties producerProperties = getProducerProperties();
        List<List<MockedDatapoint>> batchedValues = getMockedDatapoints(dataGenerator);

        long startTimeMillis = System.currentTimeMillis();
        ObjectMapper objectMapper = new ObjectMapper();
        AtomicLong timestamp = new AtomicLong(System.currentTimeMillis());
        ExecutorService executorService = new ThreadPoolExecutor(concurrency, concurrency, 0L,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());

        while (true) {
            for (List<MockedDatapoint> batch : batchedValues) {
                executorService.execute(() -> {
                    Producer producer = new KafkaProducer(producerProperties);
                    for (MockedDatapoint md : batch) {
                        try {
                            md.setTimestamp(timestamp.get());
                            md.setValue(Math.random() * 10);
                            String valueAsString = objectMapper.writeValueAsString(md);
                            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(
                                    topicName, valueAsString);
                            producer.send(producerRecord, ((recordMetadata, e) -> {
                                totalSent.incrementAndGet();
                            }));
                        } catch (JsonProcessingException e) {
                            e.printStackTrace();
                        }
                    }
                    producer.close();
                });
            }

            try {
                sleep(90000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            System.out.printf("Sent %d messages in %d milliseconds\n", totalSent.get(),
                    System.currentTimeMillis() - startTimeMillis);

//            timestamp.addAndGet(60000);

//            while (System.currentTimeMillis() < timestamp.get()) {
//                try {
//                    sleep(200);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }

//            System.out.printf("Sent %d messages in %d milliseconds\n", totalSent.get(),
//                    System.currentTimeMillis() - startTimeMillis);
        }
    }

    private static Properties getProducerProperties() {
        Properties props = new Properties();

        if (System.getenv(BROKER_IP) != null) {
            brokerIP = System.getenv(BROKER_IP);
        }
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, String.format("%s:9092", brokerIP));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

        if (System.getenv(PRODUCER_BATCH_SIZE) != null) {
            batcheSize = Integer.parseInt(System.getenv(PRODUCER_BATCH_SIZE));
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, batcheSize);
        }

        if (System.getenv(LINGER_MS) != null) {
            long lingerMillis = Long.parseLong(System.getenv(LINGER_MS));
            props.put(ProducerConfig.LINGER_MS_CONFIG, lingerMillis);
        }

        System.out.printf("PRODUCER PROPERTIES:\n %s\n", props.toString());

        return props;
    }

    private static DataGenerator configureDataGenerator() {
        int numMetrics = 25;
        int numDimensions = 12;
        int numDimCombinations = 1000;
        int metricNameLength = 64;
        int dimNameLength = 32;
        int dimValueLength = 128;

        // Check ENV for configurations
        if (System.getenv(NUM_METRICS) != null) {
            numMetrics = Integer.parseInt(System.getenv(NUM_METRICS));
        }

        if (System.getenv(BROKER_IP) != null) {
            brokerIP = System.getenv(NUM_METRICS);
        }

        if (System.getenv(TOPIC_NAME) != null) {
            topicName = System.getenv(TOPIC_NAME);
        }
        if (System.getenv(NUM_THREADS) != null) {
            concurrency = Integer.parseInt(System.getenv(NUM_THREADS));
        }

        System.out.printf("CONFIGURATION:\n %d MTSs\n", numMetrics * numDimCombinations);

        return new DataGenerator(numMetrics, numDimensions,
                numDimCombinations, metricNameLength, dimNameLength, dimValueLength);
    }

    private static List<List<MockedDatapoint>> getMockedDatapoints(
            DataGenerator dataGenerator) {

        List<MockedDatapoint> mockedDatapoints = dataGenerator.getMockedDatapoints();

        List<List<MockedDatapoint>> batchedValues = Lists.partition(mockedDatapoints, batcheSize);

        return batchedValues;
    }

}
