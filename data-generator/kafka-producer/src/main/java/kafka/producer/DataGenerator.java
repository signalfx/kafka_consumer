package kafka.producer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

public class DataGenerator {
    private List<String> metricNames;
    private Map<String, String> dimensionCombination;
    private List<MockedDatapoint> mockedDatapoints;
    private String metricNamePadding;
    private String dimNamePadding;
    private String dimValuePadding;

    public DataGenerator(int numMTSs, int numDimensions, int metricNameLength, int dimNameLength,
                         int dimValueLength) {
        this.metricNamePadding = generatePadding(metricNameLength);
        this.dimNamePadding = generatePadding(dimNameLength);
        this.dimValuePadding = generatePadding(dimValueLength);
        this.metricNames = generateMetricNames(numMTSs);
        this.dimensionCombination = generateDimensionCombination(numDimensions);
        this.mockedDatapoints = generateMockedDatapoints();
    }

    private String generatePadding(int paddingLength) {
        return String.join("", Collections.nCopies(paddingLength / 2, "_"));
    }

    private List<MockedDatapoint> generateMockedDatapoints() {
        List<MockedDatapoint> out = new ArrayList<>();
        metricNames.forEach(metricName -> {
            MockedDatapoint md = new MockedDatapoint();
            md.setMetricName(metricName);
            md.setDimensions(dimensionCombination);

            out.add(md);
        });
        return out;
    }

    @VisibleForTesting
    protected List<String> generateMetricNames(int numMetrics) {
        List<String> out = new ArrayList<>();

        for (int i = 0; i < numMetrics; i++) {
            out.add(String.format("kafka_consumer_metric_name_%d_%s", i, metricNamePadding));
        }

        return out;
    }

    @VisibleForTesting
    protected Map<String, String> generateDimensionCombination(int numDimensions) {
        Map<String, String> out = new HashMap<>();

        for (int i = 0; i < numDimensions; i++) {
            out.put(String.format("kafka_consumer_dimension_name_%d_%s", i, dimNamePadding),
                    String.format("kafka_consumer_dimension_value_%d_%s", i, dimValuePadding));
        }

        return out;
    }

    public List<MockedDatapoint> getMockedDatapoints() {
        return mockedDatapoints;
    }
}
