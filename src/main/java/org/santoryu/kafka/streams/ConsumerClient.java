package org.santoryu.kafka.streams;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Properties;


public class ConsumerClient<K extends Serializable, V extends Serializable> {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerClient.class.getName());


    private final KafkaConsumer<K, V> kafkaConsumer;
    private final List<String> topics;

    public ConsumerClient(Properties consumerProps, List<String> topics) {
        this.kafkaConsumer = new KafkaConsumer<K, V>(consumerProps);
        this.topics = topics;
    }

    private void initConsumer() {
        this.kafkaConsumer.subscribe(this.topics);
        shutdownHookToRuntime(this.kafkaConsumer);
    }

    private void closeConsumer() {
        this.kafkaConsumer.close();
    }

    private void shutdownHookToRuntime(KafkaConsumer<K, V> kafkaConsumer) {
        // main thread 참조 변수
        Thread mainThread = Thread.currentThread();

        // main thread 종료 시 유언을 남기어, 별도의 thread로 KafkaConsumer wakeup() 메소드 호출함
        // https://studyandwrite.tistory.com/552#recentComments
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                logger.info("main program exits by wakeup call");
                kafkaConsumer.wakeup();

                // main thread 죽을때까지 대기
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public void pollConsumes(long durationMillis, String commitMode) {
        try {
            while (true) {
                ConsumerRecords<K, V> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(durationMillis));
                processRecords(consumerRecords);
                if (commitMode.equals("sync")) {
                    pollCommitSync(consumerRecords);
                } else {
                    pollCommitAsync();
                }
            }
        } catch (WakeupException e) {
            logger.error("Wakeup exception called");
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            this.kafkaConsumer.commitSync();
            logger.info("consumer closing after committing sync");
            closeConsumer();
        }
    }

    private void pollCommitSync(ConsumerRecords<K, V> consumerRecords) {
        try {
            if (consumerRecords.count() > 0) {
                this.kafkaConsumer.commitSync();
                logger.info("Commited Sync");
            }
        } catch (CommitFailedException e) {
            logger.error(e.getMessage());
        }
    }

    private void pollCommitAsync() {
        this.kafkaConsumer.commitAsync((offset, exception) -> {
            if (exception != null) {
                logger.error("failed to commit offset {}, error : {}", offset, exception.getMessage());
            }
        });
    }

    private void processRecords(ConsumerRecords<K, V> consumerRecords) {
        consumerRecords.forEach(record ->
                logger.info("record key: {}, partition: {}, record.offset: {}, record.value: {}",
                        record.key(), record.partition(), record.offset(), record.value()
                ));
    }


    public static void main(String[] args) {
//        List<String> topicList = List.of("postgres.public.movies");
        List<String> topicList = List.of("test-target-topic");
        String hostName = "localhost:9092";
        String groupId = "test-group";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, hostName);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        ConsumerClient<String, String> consumerClient = new ConsumerClient<String, String>(props, topicList);
        consumerClient.initConsumer();

        long durationMillis = 1000L;
        String commitMode = "async";
        consumerClient.pollConsumes(durationMillis, commitMode);

        consumerClient.closeConsumer();

    }

}


