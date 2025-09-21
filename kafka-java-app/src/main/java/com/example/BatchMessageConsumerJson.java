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
        Properties props = new Properties();
/*      // Настройки для локальной отладки
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        props.put(KafkaJsonSchemaDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
*/
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka1:9092,kafka2:9092,kafka3:9092"); //адреса брокеров Kafka, к которым будет подключаться консьюмер
        props.put(KafkaJsonSchemaDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://schema-registry:8081"); //URL Schema Registry, который используется для хранения схемы JSON

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group2"); //Уникальный идентификатор для консьюмер-группы
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); //Задает строковый сериализатор для ключей
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class.getName()); //Задает JSON сериализатор с использованием Schema Registry для значений
        props.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE,
                "com.example.BatchMessageConsumerJson$Product"); //Говорит десериализатору в объект какого типа преобразовать полученный JSON
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //Если нет сохранённого смещения для группы, то начинать чтение с самого старого доступного сообщения
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); //Не коммитить сообщения автоматически
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "6000"); //Срок пассивности консьюмера, по истечению которого он считается "мертвым" и Kafka начинает ребалансировку
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1000); //Минимальный пакет в ответ на запрос будет не меньше 1000 байт при условии, что задержка не превысит 0.5 сек.
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500); //Максимальное время сборки данных в ответ на запрос (с целью достичь 1000 байт в пакете) не превысит 0.5 сек.

        try(KafkaConsumer<String, Product> consumer = new KafkaConsumer<>(props)) { //В случае ошибки ресурс consumer автоматически освобождается
            consumer.subscribe(Collections.singletonList("my_topic"));
            while (true) {
                ConsumerRecords<String, Product> records = consumer.poll(Duration.ofMillis(100));  //Получение сообщений
                if (!records.isEmpty()) { //На случай, если полученный пакет сообщений будет пустым
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