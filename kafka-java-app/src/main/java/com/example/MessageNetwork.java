package com.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.BooleanSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.HashMap;
import java.util.Map;


public class MessageNetwork {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String BLOCKED_USERS_TOPIC = "blocked_users";
    private static final String BLOCKED_WORDS_TOPIC = "blocked_words";
    private static final String MESSAGES_TOPIC = "messages";
    private static final String FILTERED_MESSAGES_TOPIC = "filtered_messages";
    private static final String INPUT_TOPIC = "word-count-input";
    private static final String OUTPUT_TOPIC = "word-count-output";
    private static final String BLOCKED_USERS_STORE = "blocked-users-store";

    private static final Pattern PATTERN = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);

    public static void main(String[] args) {
        // Создаем топики
        createTopics();

        // Загружаем тестовые данные
        loadTestData();

        // Настройка Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "message-network");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> inputBlockedUsers = builder.stream(
                BLOCKED_USERS_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String()));

        KTable<String, Boolean> blockedUsers = inputBlockedUsers
                .mapValues("blocked"::equals)
                .toTable(
                    Materialized.<String, Boolean>as(Stores.persistentKeyValueStore(BLOCKED_USERS_STORE))
                            .withKeySerde(Serdes.String())
                            .withValueSerde(Serdes.Boolean())
                );


        // Запускаем приложение
        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        try {
            // Очищаем локальные state store перед запуском (для тестирования)
            streams.cleanUp();

            streams.start();
            System.out.println("Приложение запущено");

            // Демонстрация запросов к state store
            Thread.sleep(15000); // Даем время на обработку сообщений
            queryStateStore(streams);

        } catch (Throwable e) {
            System.err.println("Ошибка при запуске приложения: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Выполняет запросы к state store для получения статистики слов
     */
    private static void queryStateStore(KafkaStreams streams) {
        // Ожидаем, пока состояние потока не станет RUNNING
        try {
            // Ждем, пока streams не перейдет в состояние RUNNING
            waitForStateStoreToBeReady(streams);

            System.out.println("\nТекущие результаты из state store:");

            ReadOnlyKeyValueStore<String, Long> keyValueStore = streams.store(
                    StoreQueryParameters.fromNameAndType(
                            BLOCKED_USERS_STORE, QueryableStoreTypes.keyValueStore())
            );

            keyValueStore.all().forEachRemaining(pair ->
                    System.out.println(pair.key + ": " + pair.value)
            );

        } catch (Exception e) {
            System.err.println("Ошибка при чтении state store: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Ожидает, пока state store не будет готово к запросам
     */
    private static void waitForStateStoreToBeReady(KafkaStreams streams) throws InterruptedException {
        // Максимальное время ожидания и интервал проверки
        final long MAX_WAIT_MS = 60000; // 60 секунд
        final long RETRY_INTERVAL_MS = 1000; // 1 секунда

        long startTime = System.currentTimeMillis();
        long endTime = startTime + MAX_WAIT_MS;

        // Проверяем состояние потока с интервалом
        while (System.currentTimeMillis() < endTime) {
            if (streams.state() == KafkaStreams.State.RUNNING) {
                // Пробуем получить доступ к хранилищу
                try {
                    streams.store(StoreQueryParameters.fromNameAndType(
                            BLOCKED_USERS_STORE, QueryableStoreTypes.keyValueStore()));
                    System.out.println("State store готово к запросам");
                    return; // Хранилище готово
                } catch (Exception e) {
                    // Хранилище еще не готово, продолжаем ожидание
                    System.out.println("Ожидание готовности state store... (" +
                            (System.currentTimeMillis() - startTime) / 1000 + " сек)");
                }
            } else {
                System.out.println("Ожидание перехода потока в состояние RUNNING... Текущее состояние: " +
                        streams.state());
            }

            // Ждем перед следующей проверкой
            Thread.sleep(RETRY_INTERVAL_MS);
        }

        throw new RuntimeException("Превышено время ожидания готовности state store");
    }

    /**
     * Создает топики для приложения
     */
    private static void createTopics() {
        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", BOOTSTRAP_SERVERS);

        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            NewTopic blockedWords = new NewTopic(BLOCKED_WORDS_TOPIC, 1, (short) 1);
            NewTopic blockedUsersTopic = new NewTopic(BLOCKED_USERS_TOPIC, 1, (short) 1);
            NewTopic messagesTopic = new NewTopic(MESSAGES_TOPIC, 1, (short) 1);
            NewTopic filteredmessagesTopic = new NewTopic(FILTERED_MESSAGES_TOPIC, 1, (short) 1);
            adminClient.createTopics(Arrays.asList(blockedUsersTopic, messagesTopic, filteredmessagesTopic,
                    blockedWords)).all().get();
            System.out.println("Топики созданы: " + BLOCKED_USERS_TOPIC + ", " + MESSAGES_TOPIC + ", " + FILTERED_MESSAGES_TOPIC);
        } catch (Exception e) {
            System.err.println("Ошибка при создании топиков (возможно, они уже существуют): " + e.getMessage());
        }
    }

    /**
     * Загружает тестовые данные во входной топик
     */
    private static void loadTestData() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        //key: "<Recepient>;<Sender>", value: {"blocked", "unblocked"}
        Map<String, String> blockedUsers = new HashMap<>();
        blockedUsers.put("Маша;Денис", "blocked");
        blockedUsers.put("Вася;Маша", "blocked");
        blockedUsers.put("Наташа;Лена", "blocked");
        blockedUsers.put("Петя;Юра", "blocked");
        blockedUsers.put("Петя;Игорь", "blocked");
        blockedUsers.put("Маша;Ярослав", "blocked");
        blockedUsers.put("Маша;Денис", "unblocked");
        String[] blockedWords = {
                "в", "из", "к", "у", "по", "из-за", "по-над", "под", "около", "вокруг", "перед", "возле", "до", "через",
                "по", "с", "в течение", "от", "со", "за", "в силу", "по случаю", "благодаря", "ввиду", "вследствие",
                "по причине", "для", "ради", "вроде", "подобно", "наподобие", "несмотря на", "вопреки"
        };
        //key: "<Sender>;<Addressee", value: "<Message>>
        Map<String, String> messages = new HashMap<>();
        blockedUsers.put("Маша;Денис", "blocked");
        blockedUsers.put("Вася;Маша", "blocked");
        blockedUsers.put("Наташа;Лена", "blocked");
        blockedUsers.put("Петя;Юра", "blocked");
        blockedUsers.put("Петя;Игорь", "blocked");
        blockedUsers.put("Маша;Ярослав", "blocked");
        blockedUsers.put("Маша;Денис", "unblocked");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            for (Map.Entry<String, String> entry : blockedUsers.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                producer.send(new ProducerRecord<>(BLOCKED_USERS_TOPIC, key, value));
                System.out.println("Отправлено тестовое сообщение (Топик: " + BLOCKED_USERS_TOPIC + ", Ключ: " + key +
                        ", Значение: " + value);
            }
            for (String word : blockedWords) {
                producer.send(new ProducerRecord<>(BLOCKED_WORDS_TOPIC, word, word));
                System.out.println("Отправлено тестовое сообщение (Топик: " + BLOCKED_WORDS_TOPIC + ", Ключ: " + word +
                        ", Значение: " + word);
            }

            producer.flush();
            System.out.println("Все тестовые сообщения отправлены");
        }
    }
}
