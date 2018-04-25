package com.ignite.streams.kafkaproducers;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by ashok on 4/24/2018.
 */
public class StoreReceiveEventInput {

    public static void main(String[] args) {

        Properties props = new Properties();

        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.99.100:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        //Leverage idempotent producer from Kafka 0.11
        //props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // ensure we don't push duplicates..

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        int i = 10;
        while (i>0) {
            producer.send(jsonInputGenerator(i));
            i-=5;
        }

        producer.close();
    }


    public static ProducerRecord<String, String> jsonInputGenerator(int item) {
        //Creates an empty Json {}
        ObjectNode jsonInput = JsonNodeFactory.instance.objectNode();
        //Generates random integer value...
        Integer store = ThreadLocalRandom.current().nextInt(1, 9999);
        Integer receiveQty = ThreadLocalRandom.current().nextInt(1, 10);
        Integer transactionID = ThreadLocalRandom.current().nextInt(1, 1000);
        String eventType = "Store Receiving";
        //Instant.now() is used to get time using Java 8
        Instant now = Instant.now();

        jsonInput.put("Item" , item);
        jsonInput.put("Store", store);
        jsonInput.put("ReceiveQty", receiveQty);
        jsonInput.put("TimeStamp", now.toString());
        jsonInput.put("EventType", eventType);
        jsonInput.put("TransactionID", transactionID);

        return new ProducerRecord<String,String>("itemreceive-event-input", jsonInput.toString());
    }
}
