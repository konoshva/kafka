import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer100 {
    public static void main(String[] args) {
        // Конфигурация продюсера – адрес сервера, сериализаторы для ключа и значения.
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Создание продюсера
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        int i;
        for (i = 0; i < 100; i++) {
            // Отправка сообщения
            ProducerRecord<String, String> record = new ProducerRecord<>("my_topic", "key-" + i, "message" + i);
            producer.send(record);
        }

        // Закрытие продюсера
        producer.close();
    }
}
