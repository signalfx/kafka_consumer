package kafka.producer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.lang3.RandomStringUtils;

public class DataGenerator {
    private List<String> metricNames;
    private List<Map<String, String>> dimCombinations;
    private List<MockedDatapoint> mockedDatapoints;
    private int metricNameLength;
    private int dimNameLength;
    private int dimValueLength;

    public DataGenerator(int numMetrics, int numDimensions, int numDimCombinations,
                         int metricNameLength, int dimNameLength, int dimValueLength) {
        this.metricNameLength = metricNameLength;
        this.dimNameLength = dimNameLength;
        this.dimValueLength = dimValueLength;
        this.metricNames = generateMetricNames(numMetrics);
        this.dimCombinations = generateDimensionCombinations(numDimensions, numDimCombinations);
        this.mockedDatapoints = generateMockedDatapoints();
    }

    private List<MockedDatapoint> generateMockedDatapoints() {
        List<MockedDatapoint> out = new ArrayList<>();
        metricNames.forEach(metricName -> {
            dimCombinations.forEach(dimCombination -> {
                MockedDatapoint md = new MockedDatapoint();
                md.setMetricName(metricName);
                md.setDimensions(dimCombination);

                out.add(md);
            });
        });
        return out;
    }

    @VisibleForTesting
    protected List<String> generateMetricNames(int numMetrics) {
        return generateStrings(numMetrics, metricNameLength);
    }

    private List<Map<String, String>> generateDimensionCombinations(int numDimensions,
                                                                    int numDimCombinations) {
        List<String> dimKeys = generateDimensionNames(numDimensions);
        List<Map<String, String>> out = new ArrayList<>();

        Map<String, String> seed = generateSeedDimensionCombination(dimKeys);

        out.add(seed);

        // generate multiple combinations of dimension sets by just creating multiple values for the
        // first dimension key created
        String changingDimKey = dimKeys.get(0);
        out.addAll(generateCombinationsFromSeed(changingDimKey, seed, numDimCombinations));

        return out;
    }

    @VisibleForTesting
    protected List<Map<String, String>> generateCombinationsFromSeed(String seedDimKey,
                                                                     Map<String, String> seed,
                                                                     int numDimCombinations) {
        List<Map<String, String>> out = new ArrayList<>();

        while (out.size() < numDimCombinations - 1) {
            Map<String, String> combination = new HashMap<>();
            seed.forEach((dimKey, dimVal) -> {
                if (dimKey.equals(seedDimKey)) {
                    combination.put(dimKey, generateDimensionValue(true, true));
                } else {
                    combination.put(dimKey, dimVal);
                }
            });
            out.add(combination);
        }
        return out;
    }

    @VisibleForTesting
    protected List<String> generateDimensionNames(int numDimensions) {
        return generateStrings(numDimensions, dimNameLength);
    }

    @VisibleForTesting
    protected Map<String, String> generateSeedDimensionCombination(List<String> dimKeys) {
        Map<String, String> out = new HashMap<>();

        dimKeys.forEach(dimKey -> {
            out.put(dimKey, generateDimensionValue(true, true));
        });

        return out;
    }

    @VisibleForTesting
    protected List<String> generateStrings(int numElements, int sizeOfEachElement) {
        List<String> out = new ArrayList<>();

        while (out.size() < numElements) {
            out.add(generateRandomString(sizeOfEachElement, true, true));
        }

        return out;
    }

    private String generateDimensionValue(boolean useLetters, boolean useNumbers) {
        return generateRandomString(dimValueLength, useLetters, useNumbers);
    }

    private String generateRandomString(int stringLength, boolean useLetters, boolean useNumbers) {
        String metricName = RandomStringUtils
                .random((int) Math.random() * ((stringLength - stringLength / 2) + 1)
                        + stringLength / 2, useLetters, useNumbers);
        return metricName;
    }

    public List<MockedDatapoint> getMockedDatapoints() {
        return mockedDatapoints;
    }
}
