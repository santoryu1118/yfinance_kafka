package org.santoryu.kafka.streams.config;

import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class StreamConfiguration {

    static String HOST_NAME = "localhost:9092";
    static String APP_ID = "streams-app";
    static String STATE_DIR = "/Users/jaeyeolee/IdeaProjects/yfinance_kafka/src/main/java/org/santoryu/kafka/streams/stateStore";
    public static Properties getConfiguration() {
        Properties props = new Properties();
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, HOST_NAME);
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        props.setProperty(StreamsConfig.STATE_DIR_CONFIG, STATE_DIR);

        return props;
    }
}
