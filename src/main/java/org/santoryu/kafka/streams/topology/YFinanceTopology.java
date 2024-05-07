package org.santoryu.kafka.streams.topology;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.santoryu.kafka.streams.StreamsClient;
import org.santoryu.kafka.streams.model.YFinanceModel;
import org.santoryu.kafka.streams.serdes.JsonSerdes;
import org.santoryu.kafka.streams.serdes.LinkedListSerdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class YFinanceTopology {
    private static final Logger logger = LoggerFactory.getLogger(YFinanceTopology.class.getName());
    public final static Serde<YFinanceModel> yFinanceModelSerde = JsonSerdes.createSerde(YFinanceModel.class);
    static Serde<LinkedList<Double>> linkedListDoubleSerde = LinkedListSerdes.createLinkedListSerde(Double.class);

    static String STATE_STORE = "myProcessorState";
    static String SOURCE_TOPIC = "postgres.public.yfinance";
    static String TARGET_TOPIC = "success.yfinance";
    static String REJECTED_DATA_TOPIC = "fail.yfinance";

    public static OptionalDouble averageOfFirstNElements(List<Double> list, int n) {
        if (list.size() < n) {
            return OptionalDouble.empty(); // Not enough elements to calculate the average
        }
        return list.stream()
                .limit(n)
                .mapToDouble(Double::doubleValue)
                .average();
    }

    private static void setMovingAverage(YFinanceModel model, int elementsCount, List<Double> recentValues) {
        if (recentValues.size() < elementsCount) return;
        OptionalDouble movingAverage = averageOfFirstNElements(recentValues, elementsCount);
        logger.info("movingAverage for count: {}, {}", elementsCount, movingAverage);

        if (movingAverage.isPresent()) {
            double movingAverageDouble = movingAverage.getAsDouble();
            switch (elementsCount) {
                case 10:
                    model.setMoving_avg_10(movingAverageDouble);
                    break;
                case 20:
                    model.setMoving_avg_20(movingAverageDouble);
                    break;
                case 50:
                    model.setMoving_avg_50(movingAverageDouble);
                    break;
                default:
                    logger.info("Unsupported average length: {}", elementsCount);
            }
        }
    }

    private static class YFinanceProcessor implements Processor<String, YFinanceModel, String, YFinanceModel> {
        // KIn, VIn, KOut, VOut
        // https://github.com/confluentinc/kafka-streams-examples/blob/master/src/main/java/io/confluent/examples/streams/microservices/InventoryService.java
        // https://docs.confluent.io/platform/current/streams/javadocs/javadoc/org/apache/kafka/streams/kstream/KStream.html#processValues-org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier-org.apache.kafka.streams.kstream.Named-java.lang.String...-

        private ProcessorContext<String, YFinanceModel> processorContext;
        private KeyValueStore<String, LinkedList<Double>> keyValueStore;

        @Override
        public void init(ProcessorContext<String, YFinanceModel> context) {
            this.processorContext = context;
            this.keyValueStore = context.getStateStore(STATE_STORE);
        }

        @Override
        public void process(Record<String, YFinanceModel> record) {
            logger.info("record: {}", record);
            String key = record.key();
            YFinanceModel value = record.value();

//            keyValueStore.put(key, null);  // statestore reset하고 싶을 시

            // Store에 저장되어 있던 최근 adjClose value
            LinkedList<Double> recentValues = keyValueStore.get(key);
            if (recentValues == null) {
                recentValues = new LinkedList<>();
            }

            logger.info("recentValues before: {}", recentValues);
            // 현재 record 의 adjClose value
            Double currentAdjClose = record.value().getAdjClose();

            // 둘의 차이 (%)
            double percentChange = recentValues.isEmpty() || recentValues.getFirst() == 0.0
                    ? 0.0
                    : (currentAdjClose / recentValues.getFirst() - 1) * 100;
            logger.info("percentChange : {}", percentChange);

            // remove the outlier : Check if the absolute percent change is within the desired range
            if (Math.abs(percentChange) > 30) {
                logger.info("Outlier Data: {}", value);
                processorContext.forward(record.withValue(value), REJECTED_DATA_TOPIC);
                return;
            }

            YFinanceModel newYFinanceValue = YFinanceModel.builder(value).build();

            // Determine the number of elements to include in the sublist (10 or the list's size, whichever is smaller)
            int endIndex = Math.min(recentValues.size(), 10);
            // Create a sublist containing the first 10 elements (or fewer if the original list is smaller)
            List<Double> first10Elements = new ArrayList<>(recentValues.subList(0, endIndex));
            logger.info("first10Elements : {}", first10Elements);
            newYFinanceValue.setLastTenPrices(first10Elements);

            setMovingAverage(newYFinanceValue, 10, recentValues);
            setMovingAverage(newYFinanceValue, 20, recentValues);
            setMovingAverage(newYFinanceValue, 50, recentValues);

            if (recentValues.size() > 50) {
                recentValues.removeLast();
            }
            recentValues.addFirst(currentAdjClose);
            logger.info("recentValues after: {}", recentValues);

            // statestore 에 최신 currentAdjClose 값 저장
            keyValueStore.put(key, recentValues);
            logger.info("Valid Data: {}", newYFinanceValue);
            // Forward the record downstream
            processorContext.forward(record.withValue(newYFinanceValue), TARGET_TOPIC);
        }

        @Override
        public void close() {
            Processor.super.close();
        }
    }

    public static Topology buildTopology() {
        // Create KeyValue StoreBuilder
        StoreBuilder<KeyValueStore<String, LinkedList<Double>>> keyValueStoreStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(STATE_STORE), Serdes.String(), linkedListDoubleSerde);

        // create StreamBuilder
        StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(keyValueStoreStoreBuilder);

        KStream<String, YFinanceModel> ks0 = builder.stream(SOURCE_TOPIC,
                Consumed.with(Serdes.String(), yFinanceModelSerde)
                        .withName("source-processor")
                        .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST));

        // select ticker as record key from value
        ks0 = ks0.selectKey((key, value) -> value.getTicker().toUpperCase(), Named.as("select-key-processor"));

        // ::new : method reference notation
        ks0 = ks0.process(YFinanceProcessor::new, Named.as("YFinanceProcessor"), STATE_STORE);
//        ks0.print(Printed.<String, YFinanceModel>toSysOut().withLabel("info"));

        // Sending valid data to the target topic
        ks0.to(TARGET_TOPIC, Produced.with(Serdes.String(), yFinanceModelSerde).withName(TARGET_TOPIC));

        // Sending outlier data to the rejected data topic
        ks0.to(REJECTED_DATA_TOPIC, Produced.with(Serdes.String(), yFinanceModelSerde).withName(REJECTED_DATA_TOPIC));

        // build Topology
        return builder.build();
    }

}

