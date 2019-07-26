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
    private final static String NUM_MTSES = "KAFKA_PRODUCER_NUM_MTSES";
    private final static String BROKER_IP = "KAFKA_PRODUCER_BROKER_IP";
    private final static String TOPIC_NAME = "KAFKA_PRODUCER_TOPIC_NAME";
    private final static String NUM_THREADS = "KAFKA_PRODUCER_NUM_THREADS";
    private final static String LINGER_MS = "KAFKA_PRODUCER_LINGER_MS";
    private final static String PRODUCER_BATCH_SIZE = "KAFKA_PRODUCER_BATCH_SIZE";
    private final static String REPORTING_INTERVAL = "KAFKA_PRODUCER_REPORTING_INTERVAL_MILLIS";
    private static String brokerIP = "localhost";
    private static String topicName = "benchmark2";
    private static int concurrency = 100;
    private static int batcheSize = 100000;
    private static long reportingIntervalInMillis = 60000;

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
                sleep(reportingIntervalInMillis);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            System.out.printf("Sent %d messages in %d milliseconds\n", totalSent.get(),
                    System.currentTimeMillis() - startTimeMillis);

            timestamp.addAndGet(reportingIntervalInMillis);

            while (System.currentTimeMillis() < timestamp.get()) {
                try {
                    sleep(200);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
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
        System.out.printf("TOPIC NAME:\n %s\n", topicName);

        return props;
    }

    private static DataGenerator configureDataGenerator() {
        int numMTSs = 250000;
        int numDimensions = 12;
        int metricNameLength = 64;
        int dimNameLength = 32;
        int dimValueLength = 128;

        // Check ENV for configurations
        if (System.getenv(NUM_MTSES) != null) {
            numMTSs = Integer.parseInt(System.getenv(NUM_MTSES));
        }

        if (System.getenv(NUM_MTSES) != null) {
            numMTSs = Integer.parseInt(System.getenv(NUM_MTSES));
        }

        if (System.getenv(TOPIC_NAME) != null) {
            topicName = System.getenv(TOPIC_NAME);
        }

        if (System.getenv(NUM_THREADS) != null) {
            concurrency = Integer.parseInt(System.getenv(NUM_THREADS));
        }

        if (System.getenv(REPORTING_INTERVAL) != null) {
            reportingIntervalInMillis = Long.parseLong(System.getenv(REPORTING_INTERVAL));
        }

        System.out.printf(
                "CONFIGURATION:\n NUMBER OF MTSs: %d\n NUMBER OF METRICS: %d\n NUMBER OF DIMENSIONS PER MTS: %d\n REPORTING INTERVAL IN MILLISECONDS: %d\n",
                numMTSs, numMTSs, numDimensions, reportingIntervalInMillis);
        System.out.printf("CONCURRENCY:\n %d\n", concurrency);

        return new DataGenerator(numMTSs, numDimensions, metricNameLength, dimNameLength,
                dimValueLength);
    }

    private static List<List<MockedDatapoint>> getMockedDatapoints(
            DataGenerator dataGenerator) {

        List<MockedDatapoint> mockedDatapoints = dataGenerator.getMockedDatapoints();

        List<List<MockedDatapoint>> batchedValues = Lists.partition(mockedDatapoints, batcheSize);

        return batchedValues;
    }

}
