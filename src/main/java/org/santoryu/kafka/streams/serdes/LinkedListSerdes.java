package org.santoryu.kafka.streams.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.LinkedList;

public class LinkedListSerdes {

    private static final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    public static <T> Serde<LinkedList<T>> createLinkedListSerde(Class<T> clazz) {
        LinkedListSerializer<T> serializer = new LinkedListSerializer<>();
        LinkedListDeserializer<T> deserializer = new LinkedListDeserializer<>(clazz);
        return Serdes.serdeFrom(serializer, deserializer);
    }

    public static class LinkedListSerializer<T> implements Serializer<LinkedList<T>> {

        @Override
        public byte[] serialize(String topic, LinkedList<T> data) {
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException("Error serializing list", e);
            }
        }
    }

    public static class LinkedListDeserializer<T> implements Deserializer<LinkedList<T>> {

        private final Class<T> tClass;

        public LinkedListDeserializer(Class<T> tClass) {
            this.tClass = tClass;
        }

        @Override
        public LinkedList<T> deserialize(String topic, byte[] data) {
            try {
                CollectionType listType = objectMapper.getTypeFactory().constructCollectionType(LinkedList.class, tClass);
                return objectMapper.readValue(data, listType);
            } catch (Exception e) {
                throw new RuntimeException("Error deserializing list", e);
            }
        }
    }
}
