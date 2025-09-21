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
        Properties props = new Properties();
/*      // Настройки для локальной отладки
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        props.put(KafkaJsonSchemaSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
*/
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka1:9092,kafka2:9092,kafka3:9092"); //адреса брокеров Kafka, к которым будет подключаться продьюсер
        props.put(KafkaJsonSchemaSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://schema-registry:8081"); //URL Schema Registry, который используется для хранения схемы JSON

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); //Задает строковый сериализатор для ключей
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class.getName()); //Задает JSON сериализатор с использованием Schema Registry для значений
        props.put("acks", "all"); //Продьюсер будет ждать подтверждения от всех реплик и только после этого будет считать сообщение отправленным
        props.put("retries", 3); //Будет трижды пытаться повторно отправить соообщение в случае ошибки
        try (Producer<String, Product> producer = new KafkaProducer<>(props)) { //В случае ошибки ресурс producer автоматически освобождается
            Random random = new Random();
            int i;
            for (i = 0; i < 100; i++) {
                // Создание product
                Product product = new Product();
                int productId = random.nextInt(1000);
                product.setId(productId);
                product.setName("Product-" + productId);

                // Отправка product-сообщения в Kafka
                ProducerRecord<String, Product> record = new ProducerRecord<>(
                        "my_topic",
                        UUID.randomUUID().toString(),
                        product);
                producer.send(record).get(); //отправляет сообщение синхронно, т.е. ждёт подтверждения доставки
                System.out.printf(
                        "Отправил сообщение: key = %s, product.id = %s, product.name = %s%n",
                        record.key(), product.id, product.name);
            }
        } catch (Exception e) {
            System.out.println("An exception occurred: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
