package com.example;

import com.examples.SalesCountProcessorAPI;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.BooleanSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.*;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Pattern;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.Objects;
import java.util.stream.Collectors;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class MessageNetwork {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String BLOCKED_USERS_TOPIC = "blocked_users";
    private static final String BLOCKED_WORDS_TOPIC = "blocked_words";
    private static final String MESSAGES_TOPIC = "messages";
    private static final String FILTERED_MESSAGES_TOPIC = "filtered_messages";
    private static final String BLOCKED_USERS_STORE = "blocked-users-store";

    private static final Pattern PATTERN = Pattern.compile("(?<=\\P{L})(?=\\p{L})|(?<=\\p{L})(?=\\P{L})",
            Pattern.UNICODE_CHARACTER_CLASS);

    private static String applicationId = "message-network";

    public static void main(String[] args) {
        if (args.length > 0 && args[0] != null && args[0].toLowerCase().equals("init")) {
            applicationId += "_" + UUID.randomUUID().toString();
        }
        try {
            createTopics();

            KafkaStreams blockedUsersStreams = buildStreamsAppForBlockedUsers();
            blockedUsersStreams.cleanUp();
            blockedUsersStreams.start();
            System.out.println("Приложение для обработки BlockedUsers запущено");
            waitUntilKafkaStreamsIsRunning(blockedUsersStreams);
            System.out.println("Загрузка тестовых данных для BlockedUsers...");
            loadBlockedUsersTestData();
            System.out.println("Ожидание готовности " + BLOCKED_USERS_STORE);
            waitForStateStoreToBeReady(blockedUsersStreams);
            queryStateStore(blockedUsersStreams);

            KafkaStreams streams = buildStreamsApplication(blockedUsersStreams);
            streams.start();
            System.out.println("Приложение " + applicationId + " запущено");
            System.out.println("Загрузка тестовых данных...");
            loadTestData();

        } catch (Throwable e) {
            System.err.println("Ошибка при запуске приложения: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static KafkaStreams buildStreamsAppForBlockedUsers() {
        // Настройка Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId + "_bu");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        builder.table(BLOCKED_USERS_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.<String, String>as(Stores.persistentKeyValueStore(BLOCKED_USERS_STORE))
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String()));

        return new KafkaStreams(builder.build(), props);
    }

    private static KafkaStreams buildStreamsApplication(KafkaStreams blockedUsersStreams) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        ReadOnlyKeyValueStore<String, String> blockedUsersStore =
                blockedUsersStreams.store(StoreQueryParameters.fromNameAndType(BLOCKED_USERS_STORE, QueryableStoreTypes.keyValueStore()));
        StreamsBuilder builder = new StreamsBuilder();

        GlobalKTable<String, String> blockedWords = builder.globalTable(BLOCKED_WORDS_TOPIC);

        KStream<String, String> inputMessages = builder.stream(
                MESSAGES_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String()));


        KStream<String, String> senderFilteredMessages = inputMessages
                .filter((K, V) -> blockedUsersStore.get(K) == null || "unblocked".equals(blockedUsersStore.get(K)));

        ObjectMapper mapper = new ObjectMapper();

        KStream<String, String> wordSplitMessages = senderFilteredMessages
                .mapValues(v -> {
                    ObjectNode node = mapper.createObjectNode();
                    node.put("original", v);
                    node.put("guid", UUID.randomUUID().toString());
                    return node.toString();
                })
                .flatMapValues(json -> {
                    try {
                        ObjectNode node = (ObjectNode) mapper.readTree(json);
                        String original = node.get("original").asText();
                        String guid = node.get("guid").asText();

                        // Разбиваем original на слова и разделители, сохраняя их
                        // Паттерн: последовательности букв/цифр/Unicode-слова OR последовательности не-слово-символов
                        List<String> parts = Arrays.stream(PATTERN.split(original))
                                .filter(s -> !s.isEmpty())
                                .collect(Collectors.toList());

                        // Для каждого фрагмента создаём JSON с тем же guid
                        return parts.stream().map(word -> {
                            ObjectNode out = mapper.createObjectNode();
                            out.put("guid", guid);
                            out.put("word", word);
                            return out.toString();
                        }).collect(Collectors.toList());

                    } catch (Exception e) {
                        // В случае ошибки парсинга — вернуть оригинальный json как одно значение
                        return List.of(json);
                    }
                });
        
        // Отправляем отфильтрованные сообщения в FILTERED_MESSAGES_TOPIC
        wordSplitMessages.to(FILTERED_MESSAGES_TOPIC, Produced.with(Serdes.String(), Serdes.String()));


        // Запускаем приложение
        return new KafkaStreams(builder.build(), props);
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

    private static void loadBlockedUsersTestData() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        //{"<Recipient>;<Sender>", "blocked"|"unblocked"}
        String[][] blockedUsers = {
                {"Маша;Денис", "blocked"},
                {"Маша;Ярослав", "blocked"},
                {"Маша;Денис", "unblocked"}
        };

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            for (String[] entry : blockedUsers) {
                producer.send(new ProducerRecord<>(BLOCKED_USERS_TOPIC, entry[0], entry[1]));
                System.out.println("Отправлено тестовое сообщение (Топик: " + BLOCKED_USERS_TOPIC +
                        ", Ключ: " + entry[0] + ", Значение: " + entry[1]);
            }
            producer.flush();
        }
    }

    private static void loadTestData() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        String[] blockedWords = {"дуб", "коза", "бред"};
        //{"<Recepient>;<Sender>", "<Message>"}
        String[][] messages = {
                {"Маша;Денис", "Из-за острова на стрежень, на простор речной волны выплывают расписные острогрудые челны"},
                {"Маша;Денис", "Выхожу один я на дорогу"},
                {"Денис;Маша", "не, это не Пушкин"},
                {"Денис;Маша", "нужно типа - у лукоморья дуб зеленый и т.п."},
                {"Маша;Денис", "Тогда жили-были старик со старухой у самого синего моря. Про золотую рыбку или злого птушка."},
                {"Маша;Денис", "...тут соседи беспокоить \nСтали старого царя, \nСтрашный бред ему творя"},
                {"Маша;Денис", "о! еще про Онегина скажи"},
                {"Денис;Маша", "прокатило :)"},
                {"Маша;Ярослав", "Привет!"},
                {"Наташа;Лена", "Мельдоний выдал: Иван сидел в кресле и в кепке. А в другом месте совсем уж пронзительное: коза кричала нечеловеческим голосом, но ее никто не слышал"},
                {"Лена;Наташа", "Кто кричал?"},
                {"Наташа;Лена", "Да коза: к-о-з-а"},
                {"Лена;Наташа", ":) Да уж, спец по лирике. Кто-то сдал?"},
                {"Наташа;Лена", "Будем переписывать на след. неделе. На завтра - Болконский и дуб на память. Почти две страницы. Застрелиться!!!"},
                {"Лена;Наташа", "завтра?! Ох... я ору"},
                {"Наташа;Лена", "Бесполезно, никто не услышит - Великий Мельдоний"}
        };

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            for (String word : blockedWords) {
                producer.send(new ProducerRecord<>(BLOCKED_WORDS_TOPIC, word, word));
                System.out.println("Отправлено тестовое сообщение (Топик: " + BLOCKED_WORDS_TOPIC + ", Ключ: " + word +
                        ", Значение: " + word);
            }
            for (String[] entry : messages) {
                producer.send(new ProducerRecord<>(MESSAGES_TOPIC, entry[0], entry[1]));
                System.out.println("Отправлено тестовое сообщение (Топик: " + MESSAGES_TOPIC +
                        ", Ключ: " + entry[0] + ", Значение: " + entry[1]);
            }

            producer.flush();
            System.out.println("Все тестовые сообщения отправлены");
        }
    }

    private static void waitUntilKafkaStreamsIsRunning(KafkaStreams streams) throws Exception {
        int maxRetries = 50;
        int retryIntervalMs = 2000;
        int attempt = 0;

        while (attempt < maxRetries) {
            if (streams.state() == KafkaStreams.State.RUNNING) {
                System.out.println("Kafka Streams успешно запущен");
                return;
            }

            System.out.println("Ожидание запуска Kafka Streams... Текущее состояние: " + streams.state());
            Thread.sleep(retryIntervalMs);
            attempt++;
        }

        throw new RuntimeException("Превышено время ожидания запуска Kafka Streams");
    }

}
