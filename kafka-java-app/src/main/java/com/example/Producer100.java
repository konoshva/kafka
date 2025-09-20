package com.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer100 {
    public static void main(String[] args) {
        // Конфигурация продюсера – адрес сервера, сериализаторы для ключа и значения.
        Properties props = new Properties();
        //props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka1:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Создание продюсера
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        int i;
        for (i = 0; i < 100; i++) {
            // Отправка сообщения
            ProducerRecord<String, String> record = new ProducerRecord<>("my_topic", "key-" + i, "message-" + i);
            producer.send(record);
            try {
                // Задержка на 100 миллисекунд
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // Закрытие продюсера
        producer.close();
    }
}
