package org.santoryu.kafka.streams;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerClient {
    private static final Logger logger = LoggerFactory.getLogger(ProducerClient.class.getName());

    private static <K, V> void produceMessages(KafkaProducer<K, V> kafkaProducer, String topicName) {
        Integer partition = null;
        Long currentTime = System.currentTimeMillis();
        K key = null;
        V value = (V) "test value";
        ProducerRecord<K, V> producerRecord = new ProducerRecord<K, V>(topicName, partition, currentTime, key, value);

        kafkaProducer.send(producerRecord, (metadata, exception) -> {
            if (exception == null) {
                logger.info("\n ###### record metadata received ##### \n" +
                        "partition:" + metadata.partition() + "\n" +
                        "offset:" + metadata.offset() + "\n" +
                        "timestamp:" + metadata.timestamp());
            } else {
                logger.error("exception error from broker " + exception.getMessage());
            }
        });
    }

    public static void main(String[] args) {
        String topicName = "test";
        String hostName = "localhost:9092";

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, hostName);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);
        produceMessages(kafkaProducer, topicName);
        kafkaProducer.close();
    }
}
