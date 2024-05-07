package org.santoryu.kafka.streams.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.santoryu.kafka.streams.model.YFinanceModel;

public class JsonSerdes {

    public static <T> Serde<T> createSerde(Class<T> clazz) {
        JsonSerializer<T> serializer = new JsonSerializer<>();
        JsonDeserializer<T> deserializer = new JsonDeserializer<>(clazz);
        return Serdes.serdeFrom(serializer, deserializer);
    }

    public static YFinanceWrapSerde YFinanceSerde() {
        return new YFinanceWrapSerde(new JsonSerializer<>(), new JsonDeserializer<>(YFinanceModel.class));
    }

    public final static class YFinanceWrapSerde extends WrapSerde<YFinanceModel> {
        private YFinanceWrapSerde(JsonSerializer<YFinanceModel> serializer, JsonDeserializer<YFinanceModel> deserializer) {
            super(serializer, deserializer);
        }
    }

    private static class WrapSerde<T> implements Serde<T> {
        private final JsonSerializer<T> serializer;
        private final JsonDeserializer<T> deserializer;

        private WrapSerde(JsonSerializer<T> serializer, JsonDeserializer<T> deserializer) {
            this.serializer = serializer;
            this.deserializer = deserializer;
        }

        @Override
        public JsonSerializer<T> serializer() {
            return serializer;
        }

        @Override
        public JsonDeserializer<T> deserializer() {
            return deserializer;
        }
    }
}
