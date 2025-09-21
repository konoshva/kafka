package com.example;

import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class BatchMessageConsumerJson {
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
        // Настройка консьюмера
        Properties props = new Properties();
/*
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");  // Адрес брокера Kafka
        props.put(KafkaJsonSchemaDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
*/
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka1:9092,kafka2:9092,kafka3:9092");  // Адрес брокера Kafka
        props.put(KafkaJsonSchemaDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://schema-registry:8081");

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group2");        // Уникальный идентификатор группы
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class.getName());
        props.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE,
                "com.example.BatchMessageConsumerJson$Product");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");        // Начало чтения с самого начала
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");           // Автоматический коммит смещений
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "6000");           // Время ожидания активности от консьюмера
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1000);                   // Минимальное количество байт в пакете
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);                // Максимальное время сборки данных продьюсером

        try(KafkaConsumer<String, Product> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("my_topic"));
            while (true) {
                ConsumerRecords<String, Product> records = consumer.poll(Duration.ofMillis(100));  // Получение сообщений
                if (!records.isEmpty()) {
                    for (ConsumerRecord<String, Product> record : records) {
                        Product product = record.value();
                        try {
                            System.out.printf(
                                    "Обрабатываю сообщение: key = %s, value = %s, partition = %d, offset = %d, product.id = %s, product.name = %s%n",
                                    record.key(), record.value(), record.partition(), record.offset(), product.id, product.name);
                           //long inducted_error = (long) 1 / (record.offset() % (long) 20); //Для тестирования ошибок
                        } catch (Exception e) {
                            System.out.println("An exception occurred: " + e.getMessage());
                        }
                    }
                    consumer.commitSync();
                    System.out.println("Фикcация");
                }
            }
        } catch (Exception e) {
            System.out.println("An exception occurred: " + e.getMessage());
            e.printStackTrace();
        }
    }
}