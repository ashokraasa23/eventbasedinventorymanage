package com.iginite.streams.cassandrafinal;

import com.fasterxml.jackson.databind.JsonNode;
import com.ignite.core.streams.eventmapper.CassandraMapper;
import com.ignite.core.streams.serialization.InventorySerde;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

/**
 * Created by ashok on 6/5/2018.
 */
public class CassandraUpdateStream {

    //String serverIp = "127.0.0.1";
    //String keyspace = "eventbasedinventory";
    //Cluster cluster =Cluster.builder().addContactPoint(serverIp).build();
    //Session session = cluster.connect(keyspace);
    //private static Cluster cluster =Cluster.builder().addContactPoint(serverIp).withRetryPolicy(DefaultRetryPolicy.INSTANCE).withPort(9042).build();

    public static void main(String[] args) {

        Properties config = new Properties();

        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.99.100:9092");
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "CassandraUpdateStream");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, InventorySerde.class.getName());
        //config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, JsonNode> finalEvent = builder.stream("inventory-mapper-topic6");

        KStream<String, JsonNode> finalSaleEvent = finalEvent.mapValues(finalData -> new CassandraMapper().newCassandraMapper(finalData));


        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.cleanUp();
        streams.start();

        // print the topology
        System.out.println(streams.toString());

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
