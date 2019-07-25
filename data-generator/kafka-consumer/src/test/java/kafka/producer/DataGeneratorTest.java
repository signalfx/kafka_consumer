package kafka.producer;

import static org.hamcrest.CoreMatchers.is;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;


import kafka.producer.DataGenerator;

public class DataGeneratorTest {
    @Test
    public void testMakingMetricNames() {
        int numMetrics = 10;
        DataGenerator dataGenerator = new DataGenerator(numMetrics, 10, 10, 64,
                32, 128);
        int actualSize = dataGenerator.generateMetricNames(numMetrics).size();

        Assert.assertThat(actualSize, is(numMetrics));

    }

    @Test
    public void testMakingDimensionNames() {
        int numDimensions = 12;
        DataGenerator dataGenerator = new DataGenerator(10, numDimensions, 10, 64,
                32, 128);
        int actualSize = dataGenerator.generateDimensionNames(numDimensions).size();

        Assert.assertThat(actualSize, is(numDimensions));

    }

    @Test
    public void testMakingDimensionSeedCombination() {
        int numDimensions = 12;
        DataGenerator dataGenerator = new DataGenerator(10, numDimensions, 10, 64,
                32, 128);
        int actualSize = dataGenerator.generateSeedDimensionCombination(dimNameMaker(numDimensions))
                .size();

        Assert.assertThat(actualSize, is(numDimensions));

    }

    @Test
    public void testMakingCombinationsFromSeed() {
        int numDimensions = 12;
        int numDimCombinations = 500;
        DataGenerator dataGenerator = new DataGenerator(10, numDimensions, numDimCombinations,
                64, 32, 128);
        int actualSize = dataGenerator
                .generateCombinationsFromSeed("0", dimCombinationsMaker(numDimensions),
                        numDimCombinations).size();

        Assert.assertThat(actualSize, is(numDimCombinations - 1));
    }

    @Test
    public void testMakingMockedDatapoints() {
        int numMetrics = 1000;
        int numDimensions = 12;
        int numDimCombinations = 500;
        DataGenerator dataGenerator = new DataGenerator(numMetrics, numDimensions, numDimCombinations,
                64, 32, 128);
        int actualSize = dataGenerator.getMockedDatapoints().size();

        Assert.assertThat(actualSize, is(numDimCombinations * numMetrics));
    }

    private Map<String, String> dimCombinationsMaker(int numDimensions) {
        Map<String, String> out = new HashMap<>();
        List<String> dimNames = dimNameMaker(numDimensions);

        dimNames.forEach(dimName -> {
            out.put(dimName, dimName);
        });

        return out;
    }

    private List<String> dimNameMaker(int numDimensions) {
        List<String> out = new ArrayList<>();
        for (int i = 0; i < numDimensions; i++) {
            out.add(Integer.toString(i));
        }
        return out;
    }

}
