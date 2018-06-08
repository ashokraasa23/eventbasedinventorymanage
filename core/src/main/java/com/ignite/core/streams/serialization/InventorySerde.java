package com.ignite.core.streams.serialization;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;

import java.util.Map;

/**
 * Created by ashok on 4/24/2018.
 */
public class InventorySerde implements Serde {
    @Override
    public void configure(Map map, boolean b) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer serializer() {
        return new JsonSerializer();
    }

    @Override
    public Deserializer deserializer() {
        return new JsonDeserializer();
    }
}
