package kafka.producer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

public class DataGenerator {
    private static int NUM_METRICS = 2500;
    private List<String> metricNames;
    private List<Map<String, String>> dimensionCombinations;
    private List<MockedDatapoint> mockedDatapoints;
    private String metricNamePadding;
    private String dimNamePadding;
    private String dimValuePadding;

    public DataGenerator(int numMTSs, int numDimensions, int metricNameLength, int dimNameLength,
                         int dimValueLength) {
        this.metricNamePadding = generatePadding(metricNameLength);
        this.dimNamePadding = generatePadding(dimNameLength);
        this.dimValuePadding = generatePadding(dimValueLength);
        this.metricNames = generateMetricNames();
        this.dimensionCombinations = generateDimensionCombination(numDimensions, numMTSs);
        this.mockedDatapoints = generateMockedDatapoints();
    }

    private String generatePadding(int paddingLength) {
        return String.join("", Collections.nCopies(paddingLength / 2, "_"));
    }

    private List<MockedDatapoint> generateMockedDatapoints() {
        List<MockedDatapoint> out = new ArrayList<>();
        metricNames.forEach(metricName -> {
            dimensionCombinations.forEach(dimensionCombination -> {
                MockedDatapoint md = new MockedDatapoint();
                md.setMetricName(metricName);
                md.setDimensions(dimensionCombination);

                out.add(md);
            });
        });
        return out;
    }

    @VisibleForTesting
    protected List<String> generateMetricNames() {
        List<String> out = new ArrayList<>();

        for (int i = 0; i < NUM_METRICS; i++) {
            out.add(String.format("kafka_consumer_metric_name_%d_%s", i, metricNamePadding));
        }

        return out;
    }

    @VisibleForTesting
    protected List<Map<String, String>> generateDimensionCombination(int numDimensions,
                                                                     int numMTSs) {
        int numCombinations = numMTSs / NUM_METRICS;
        List<Map<String, String>> out = new ArrayList<>();

        Map<String, String> seed = new HashMap<>();
        for (int i = 0; i < numDimensions; i++) {
            seed.put(String.format("kafka_consumer_dimension_name_%d_%s", i, dimNamePadding),
                    String.format("kafka_consumer_dimension_value_%d_%s", i, dimValuePadding));
        }

        out.add(seed);
        int i = 0;

        while(out.size() < numCombinations) {
            String changingDim = String
                    .format("kafka_consumer_dimension_name_%d_%s", 0, dimNamePadding);
            Map<String, String> newCombination = new HashMap<>();
            for (Map.Entry<String, String> entry : seed.entrySet()) {
                if (entry.getKey().equals(changingDim)) {
                    newCombination.put(changingDim,
                            String.format("kafka_consumer_dimension_value_%d_%s_%s", 0,
                                    dimNamePadding, i));
                } else {
                    newCombination.put(entry.getKey(), entry.getValue());
                }
            }

            out.add(newCombination);
            i++;
        }

        return out;
    }

    public List<MockedDatapoint> getMockedDatapoints() {
        return mockedDatapoints;
    }
}
