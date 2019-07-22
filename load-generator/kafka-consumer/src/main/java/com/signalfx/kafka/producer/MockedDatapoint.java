package com.signalfx.kafka.producer;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(Include.NON_NULL)
public class MockedDatapoint {
    private String metricName;
    private double value;
    private long timestamp;
    private Map<String, String> dimensions;

    @JsonProperty("m")
    public String getMetricName() {
        return metricName;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }

    public MockedDatapoint withMetricName(String metricName) {
        setMetricName(metricName);
        return this;
    }

    @JsonProperty("v")
    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public MockedDatapoint withMetricName(double value) {
        setValue(value);
        return this;
    }

    @JsonProperty("t")
    public double getTimestamp() {
        return value;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public MockedDatapoint withTimestamp(long timestamp) {
        setTimestamp(timestamp);
        return this;
    }

    @JsonProperty("dims")
    public Map<String, String> getDimesions() {
        return dimensions;
    }

    public void setDimensions(Map<String, String> dimensions) {
        this.dimensions = dimensions;
    }

    public MockedDatapoint withDimensions(Map<String, String> dimensions) {
        setDimensions(dimensions);
        return this;
    }
}
