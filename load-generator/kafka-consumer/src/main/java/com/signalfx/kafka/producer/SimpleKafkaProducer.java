package com.signalfx.kafka.producer;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SimpleKafkaProducer {

    public static void main(String args[]) {
        int maxDatapoints = Integer.MAX_VALUE;
        int maxDatapointsPerSec = 500000;
        int batchSize = 100000;
        String topicName = "test";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        Producer producer = new KafkaProducer(props);
        ObjectMapper objectMapper = new ObjectMapper();

        int i = 0;
        while (i < maxDatapoints) {
            long startTime = System.currentTimeMillis();
            Set<String> mockedDatapoints = getMockedDataBatch(i, batchSize, objectMapper);
            mockedDatapoints.forEach(mockedDatapoint -> {
                ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(
                        topicName, mockedDatapoint);
                producer.send(producerRecord);
            });
//            try {
//                sleep(1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
            System.out.printf("Time taken: %d to send %d mesasages\n", System.currentTimeMillis() - startTime, batchSize);
            i += batchSize;
        }
        producer.close();
    }

    private static Set<String> getMockedDataBatch(int i, int batchSize,
                                                  ObjectMapper objectMapper) {
        Set<String> out = new HashSet<>();

        for (int j = 0; j < batchSize; j++) {
            try {
                String mockedDatapointString = objectMapper
                        .writeValueAsString(getMockedDatapoint(i));
                out.add(mockedDatapointString);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }

        return out;
    }

    private static MockedDatapoint getMockedDatapointhHeler(int i) {
        MockedDatapoint out = getMockedDatapoint(i);
        return out;
    }

    private static MockedDatapoint getMockedDatapoint(int i) {
        MockedDatapoint md = new MockedDatapoint();
        Map<String, String> dimensions = getMockedDimensions();

        md.setMetricName("metric" + i);
        md.setTimestamp(System.currentTimeMillis());
        md.setValue(System.currentTimeMillis());
        md.setDimensions(dimensions);

        return md;
    }

    private static Map<String, String> getMockedDimensions() {
        Map<String, String> out = new HashMap<>();
        boolean useLetters = true;
        boolean useNumbers = true;
        int maxDimensions = 12;

        for (int i = 0; i < maxDimensions; i++) {
            // Generate random string of lengths 32 and 128 for dim names and values with max sizes
            String dimName = RandomStringUtils.random(32, useLetters, useNumbers);
            String dimVaue = RandomStringUtils.random(128, useLetters, useNumbers);
            out.put(dimName, dimVaue);
        }

        return out;
    }

}