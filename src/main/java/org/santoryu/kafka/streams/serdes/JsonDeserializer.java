package org.santoryu.kafka.streams.serdes;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;
import org.santoryu.kafka.streams.topology.YFinanceTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class JsonDeserializer<T> implements Deserializer<T> {
    ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    private Class<T> deseralizeClass;

    public JsonDeserializer(Class<T> deseralizeClass) {
        this.deseralizeClass = deseralizeClass;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        T classObject = null;

        try {
            classObject = objectMapper.readValue(data, deseralizeClass);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return classObject;
    }
}
