package kafka.producer;

import static org.hamcrest.CoreMatchers.is;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class DataGeneratorTest {
    @Test
    public void testMaking2500MTS() {
        DataGenerator dataGenerator = new DataGenerator(2500, 10, 10, 32, 128);
        List<String> out = dataGenerator.getMockedDatapoints().stream().map(m -> m.getMetricName())
                .collect(Collectors.toList());

        Assert.assertThat(out, is(dataGenerator.generateMetricNames()));

    }

    @Test
    public void testMaking25000MTS() {
        DataGenerator dataGenerator = new DataGenerator(25000, 10, 10, 32, 128);
        Set<String> out = dataGenerator.getMockedDatapoints().stream().map(m -> m.getMetricName())
                .collect(Collectors.toSet());
        int numMTSs = dataGenerator.getMockedDatapoints().size();

        Assert.assertThat(out,
                is(dataGenerator.generateMetricNames().stream().collect(Collectors.toSet())));

        Assert.assertThat(numMTSs, is(25000));

    }

    @Test
    public void testMakingDimensionCombinations_1() {
        DataGenerator dataGenerator = new DataGenerator(10, 1, 64,
                10, 10);
        List<Map<String, String>> out = dataGenerator.generateDimensionCombination(1, 2500);
        Map<String, String> expected = new HashMap<>();

        Assert.assertThat(out, is(ImmutableList.of(ImmutableMap
                .of("kafka_consumer_dimension_name_0______",
                        "kafka_consumer_dimension_value_0______"))));

    }

    @Test
    public void testMakingDimensionCombinations_2() {
        DataGenerator dataGenerator = new DataGenerator(10, 1, 64,
                10, 10);
        Set<Map<String, String>> out = dataGenerator.generateDimensionCombination(1, 5000).stream()
                .collect(Collectors.toSet());

        Assert.assertThat(out, is(ImmutableSet.of(ImmutableMap
                .of("kafka_consumer_dimension_name_0______",
                        "kafka_consumer_dimension_value_0______"), ImmutableMap
                .of("kafka_consumer_dimension_name_0______",
                        "kafka_consumer_dimension_value_0_______0"))));

    }

    @Test
    public void testMakingDimensionCombination_2() {
        DataGenerator dataGenerator = new DataGenerator(10, 1, 64,
                6, 6);
        Set<Map<String, String>> out = dataGenerator.generateDimensionCombination(1, 7500).stream()
                .collect(Collectors.toSet());

        Assert.assertThat(out, is(ImmutableSet.of(ImmutableMap
                        .of("kafka_consumer_dimension_name_0____", "kafka_consumer_dimension_value_0____"),
                ImmutableMap.of("kafka_consumer_dimension_name_0____",
                        "kafka_consumer_dimension_value_0_____0"), ImmutableMap
                        .of("kafka_consumer_dimension_name_0____",
                                "kafka_consumer_dimension_value_0_____1"))));

    }

    @Test
    public void testMakingMockedDatapoints() {
        int numMTSs = 5000;
        int numDimensions = 12;
        DataGenerator dataGenerator = new DataGenerator(numMTSs, numDimensions,
                64, 32, 128);
        int actualSize = dataGenerator.getMockedDatapoints().size();

        Assert.assertThat(actualSize, is(numMTSs));
    }

}
