package kafka.producer;

import static org.hamcrest.CoreMatchers.is;

import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class DataGeneratorTest {
    @Test
    public void testMakingMetricNames_1() {
        DataGenerator dataGenerator = new DataGenerator(1, 10, 10, 32, 128);
        List<String> out = dataGenerator.generateMetricNames(1);

        Assert.assertThat(out, is(ImmutableList.of("kafka_consumer_metric_name_0______")));

    }

    @Test
    public void testMakingMetricNames_2() {
        DataGenerator dataGenerator = new DataGenerator(4, 10, 4, 32, 128);
        List<String> out = dataGenerator.generateMetricNames(4);

        Assert.assertThat(out, is(ImmutableList
                .of("kafka_consumer_metric_name_0___", "kafka_consumer_metric_name_1___",
                        "kafka_consumer_metric_name_2___", "kafka_consumer_metric_name_3___")));

    }

    @Test
    public void testMakingDimensionCombination_1() {
        DataGenerator dataGenerator = new DataGenerator(10, 1, 64,
                10, 10);
        Map<String, String> out = dataGenerator.generateDimensionCombination(1);

        Assert.assertThat(out, is(ImmutableMap.of("kafka_consumer_dimension_name_0______",
                "kafka_consumer_dimension_value_0______")));

    }

    @Test
    public void testMakingDimensionCombination_2() {
        DataGenerator dataGenerator = new DataGenerator(10, 1, 64,
                6, 6);
        Map<String, String> out = dataGenerator.generateDimensionCombination(3);

        Assert.assertThat(out, is(ImmutableMap
                .of("kafka_consumer_dimension_name_0____", "kafka_consumer_dimension_value_0____",
                        "kafka_consumer_dimension_name_1____",
                        "kafka_consumer_dimension_value_1____",
                        "kafka_consumer_dimension_name_2____",
                        "kafka_consumer_dimension_value_2____")));

    }

    @Test
    public void testMakingMockedDatapoints() {
        int numMTSs = 1000;
        int numDimensions = 12;
        DataGenerator dataGenerator = new DataGenerator(numMTSs, numDimensions,
                64, 32, 128);
        int actualSize = dataGenerator.getMockedDatapoints().size();

        Assert.assertThat(actualSize, is(numMTSs));
    }

}
