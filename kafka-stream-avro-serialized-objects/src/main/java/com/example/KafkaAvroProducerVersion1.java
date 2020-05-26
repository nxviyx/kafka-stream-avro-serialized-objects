package com.example;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaAvroProducerVersion1 {
    public static void main(String[] args) {
        Properties properties = new Properties();

        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("acks", "all");
        properties.put("retries", "10");
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", KafkaAvroSerializer.class.getName());
        properties.put("schema.registry.url", "http://localhost:8081");

        KafkaProducer<String, Employee> kafkaProducer = new KafkaProducer<String, Employee>(properties);

        String topic = "Topic-A";

        Employee employee = Employee.newBuilder()
                .setEmployeeId("1")
                .setEmployeeFirstName("Joey")
                .setEmployeeLastName("Tribbiani")
                .setEmployeeJoinDate("27-06-2020")
                .setEmployeePosition("Accountant")
                .setEmployeeAddress("Colombo-10")
                .build();

        ProducerRecord<String, Employee> producerRecord = new ProducerRecord<String, Employee>( topic, employee );

        System.out.println("[ PRODUCER ] - "+employee);
        kafkaProducer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
                if (exception == null) {
                    System.out.println(" [BROKER] - Successfully received the details as: \n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp()+ "\n");
                } else {
                    exception.printStackTrace();
                }
            }
        });

        kafkaProducer.flush();
        kafkaProducer.close();
    }
}