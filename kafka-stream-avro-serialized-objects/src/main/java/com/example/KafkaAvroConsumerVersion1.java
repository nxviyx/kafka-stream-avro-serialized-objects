package com.example;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class KafkaAvroConsumerVersion1 {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        properties.put("group.id", "group one");
        properties.put("auto.commit.enable", "false");
        properties.put("auto.offset.reset", "earliest");

        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
        properties.setProperty("schema.registry.url", "http://localhost:8081");
        properties.setProperty("specific.avro.reader", "true");

        KafkaConsumer<String, Employee> kafkaConsumer = new KafkaConsumer<>(properties);
        String topic = "Topic-A";
        kafkaConsumer.subscribe(Collections.singleton(topic));

        System.out.println("[ CONSUMER ]- Waiting for data...");

        while (true){
            System.out.println("[ CONSUMER ]- Polling");
            ConsumerRecords<String, Employee> records = kafkaConsumer.poll(1000);

            for (ConsumerRecord<String, Employee> record : records){
                Employee employee = record.value();
                System.out.println("[ CONSUMER ]- "+employee);
            }

            kafkaConsumer.commitSync();
        }
    }
}
