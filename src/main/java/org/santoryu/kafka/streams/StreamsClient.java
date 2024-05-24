package org.santoryu.kafka.streams;

import org.apache.kafka.streams.*;
import org.santoryu.kafka.streams.config.StreamConfiguration;
import org.santoryu.kafka.streams.topology.YFinanceTopology;

import java.util.Properties;

public class StreamsClient {
    public static void main(String[] args) {
        // create Configuration
        Properties props = StreamConfiguration.getConfiguration();
        // build Topology
        Topology topology = YFinanceTopology.buildTopology();
        // create Kafka Streams
        KafkaStreams kafkaStreams = new KafkaStreams(topology, props);
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        // start Kafka Streams
        kafkaStreams.start();
    }
}
