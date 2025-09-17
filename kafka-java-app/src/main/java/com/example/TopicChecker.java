package com.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import java.util.Properties;
import java.util.Set;

public class TopicChecker {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka1:9092");  // Адрес брокера Kafka
        // Создание AdminClient
        try (AdminClient adminClient = AdminClient.create(props)) {
            String topicToCheck = "my_topic";
            Set<String> topicNames;
            do {
                // Получение списка топиков
                ListTopicsResult topics = adminClient.listTopics();
                KafkaFuture<Set<String>> futureTopics = topics.names();
                topicNames = futureTopics.get();
                Thread.sleep(2000);
                System.out.println("Check of the topic existence");
            } while (!topicNames.contains(topicToCheck));
            System.out.println("The topic exists!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
