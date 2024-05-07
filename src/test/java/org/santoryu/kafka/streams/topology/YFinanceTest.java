package org.santoryu.kafka.streams.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;

import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.santoryu.kafka.streams.model.YFinanceModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;


public class YFinanceTest {
    private static final Logger logger = LoggerFactory.getLogger(YFinanceTest.class.getName());

    TopologyTestDriver testDriver;
    private TestInputTopic<String, YFinanceModel> inputTopic;
    private TestOutputTopic<String, YFinanceModel> outputTopic;
    private TestOutputTopic<String, YFinanceModel> outputRejectedTopic;


    @BeforeEach
    void setup() {
        Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "test-streams-app");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "test:1234");
//        Properties props = StreamConfiguration.getConfiguration();

        Topology topology = YFinanceTopology.buildTopology();

        testDriver = new TopologyTestDriver(topology, props);

        inputTopic = testDriver.createInputTopic(YFinanceTopology.SOURCE_TOPIC, Serdes.String().serializer(), YFinanceTopology.yFinanceModelSerde.serializer());
        outputTopic = testDriver.createOutputTopic(YFinanceTopology.TARGET_TOPIC, Serdes.String().deserializer(), YFinanceTopology.yFinanceModelSerde.deserializer());
        outputRejectedTopic = testDriver.createOutputTopic(YFinanceTopology.REJECTED_DATA_TOPIC, Serdes.String().deserializer(), YFinanceTopology.yFinanceModelSerde.deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    @Test
    void testTopology() {
        // Define a series of adjusted close prices for the test data.
        double[] adjCloses = {1, 1.1, 1.2, 1.3, 1.1, 1.4, 1.35, 1.25, 1.1, 1.3, 1.35};

        // Create a list of YFinanceModel objects from the defined adjClose prices.
        // Each model represents a unique record akin to a stock data entry.
        List<YFinanceModel> models = Arrays.stream(adjCloses)
                .mapToObj(adjClose -> YFinanceModel.builder()
                        .ticker("test-ticker")
                        .dateTime(new Date())
                        .adjClose(adjClose)
                        .build())
                .collect(Collectors.toList());

        // Pipe each YFinanceModel object into the Kafka input topic.
        models.forEach(yFinanceRecord -> inputTopic.pipeInput(yFinanceRecord.getTicker(), yFinanceRecord));

        KeyValueStore<String, LinkedList<Double>> keyValueStore = testDriver.getKeyValueStore(YFinanceTopology.STATE_STORE);

        // Read values until the 10th message
        YFinanceModel tenthRecord = null;
        for (int i = 0; i < 10; i++) {
            tenthRecord = outputTopic.readValue();
        }

        assertEquals(tenthRecord.getAdjClose(), 1.3);
        assertEquals(tenthRecord.getLastTenPrices(), Arrays.asList(1.1, 1.25, 1.35, 1.4, 1.1, 1.3, 1.2, 1.1, 1.0));
        // Verify that moving averages are not calculated/set before 10 records are available.
        assertNull(tenthRecord.getMoving_avg_10());
        assertNull(tenthRecord.getMoving_avg_20());

        YFinanceModel lastRecord = outputTopic.readValue();
        assertEquals(lastRecord.getAdjClose(), 1.35);
        // Assert the last ten prices are correctly updated in the final record.
        assertEquals(lastRecord.getLastTenPrices(), Arrays.asList(1.3, 1.1, 1.25, 1.35, 1.4, 1.1, 1.3, 1.2, 1.1, 1.0));
        // Assert that the moving average over 10 prices is calculated correctly.
        assertEquals(lastRecord.getMoving_avg_10(), 1.21);
        assertNull(lastRecord.getMoving_avg_20());

        // Verify the correctness of the stored ticker data in the KeyValue store.
        assertEquals(keyValueStore.get(lastRecord.getTicker().toUpperCase()), Arrays.asList(1.35, 1.3, 1.1, 1.25, 1.35, 1.4, 1.1, 1.3, 1.2, 1.1, 1.0));

        assertTrue(outputRejectedTopic.isEmpty());
    }

    @Test
    void testTopologyWhenRejection() {
        List.of(
                YFinanceModel.builder().ticker("test-ticker").dateTime(new Date()).adjClose(1).build(),
                YFinanceModel.builder().ticker("test-ticker").dateTime(new Date()).adjClose(2).build(),
                YFinanceModel.builder().ticker("test-ticker").dateTime(new Date()).adjClose(1.2).build()
        ).forEach(yFinanceRecord -> inputTopic.pipeInput(yFinanceRecord.getTicker(), yFinanceRecord));

        KeyValueStore<String, LinkedList<Double>> keyValueStore = testDriver.getKeyValueStore(YFinanceTopology.STATE_STORE);

        YFinanceModel firstRecord = outputTopic.readValue();
        assertEquals(firstRecord.getAdjClose(), 1);
        assertFalse(outputTopic.isEmpty());

        YFinanceModel secondRecord = outputRejectedTopic.readValue();
        assertEquals(secondRecord.getAdjClose(), 2);
        assertTrue(outputRejectedTopic.isEmpty());

        YFinanceModel thirdRecord = outputTopic.readValue();
        assertEquals(thirdRecord.getAdjClose(), 1.2);
        assertEquals(thirdRecord.getLastTenPrices(), Arrays.asList(1.0));
        assertTrue(outputTopic.isEmpty());

        assertEquals(keyValueStore.get(thirdRecord.getTicker().toUpperCase()), Arrays.asList(1.2, 1.0));
    }
}
