package com.example;

import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class ProducerJson {
    public static class Product {
        private Integer id;
        private String name;

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    public static void main(String[] args) {
        // Настройки для подключения к Kafka и Schema Registry
        Properties props = new Properties();
/*
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        props.put(KafkaJsonSchemaSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
*/
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka1:9092,kafka2:9092,kafka3:9092");
        props.put(KafkaJsonSchemaSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://schema-registry:8081");


        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class.getName());
        props.put("acks", "all");
        props.put("retries", 3);
        try (Producer<String, Product> producer = new KafkaProducer<>(props)) {
            Random random = new Random();
            int i;
            for (i = 0; i < 100; i++) {
                // Создание JSON-сообщения
                Product jsonMessage = new Product();
                int productId = random.nextInt(1000);
                jsonMessage.setId(productId);
                jsonMessage.setName("Product-" + productId);

                // Отправка сообщения в Kafka
                ProducerRecord<String, Product> record = new ProducerRecord<>("my_topic",
                        UUID.randomUUID().toString(),
                        jsonMessage);
                producer.send(record).get();
            }
        } catch (Exception e) {
            System.out.println("An exception occurred: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
