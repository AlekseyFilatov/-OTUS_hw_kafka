package hw5.kafka.otus;

import java.util.*;
import java.lang.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.concurrent.ExecutionException;
import java.time.Duration;


import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;


import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.errors.TopicExistsException;


public class objectKafka {

    public static Logger kafkalogger = LoggerFactory.getLogger("kafkaStreamResourceApp");
    public static void main(String[] args) throws Exception {

        kafkalogger.info("main start!");
        try (kafkaStreamResourceClass resource = new kafkaStreamResourceClass()) {
            resource.performOperation();
        }
        kafkalogger.info("main stop!");
    }
}

class kafkaStreamResourceClass implements AutoCloseable {

    private static String HOST = "localhost:9091";
    private Map<String, Object> adminConfig = Map.of(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, HOST);
    private Properties producerProperties;
    private KafkaProducer<String, String> producer_topics;
    public kafkaStreamResourceClass() {
        try {
            scalacontainer.zio.postgres.objectKafka.kafkalogger.info("kafkaStreamResourceClass: Acquired");

            producerProperties = new Properties();
            producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producer_topics = new KafkaProducer<>(producerProperties);

        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static final Map<String, Object> streamsConfig = Map.of(
            StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, HOST,
            StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName(),
            StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

    public static StreamsConfig createStreamsConfig(Consumer<Map<String, Object>> builder) {
        var map = new HashMap<>(streamsConfig);
        builder.accept(map);
        return new StreamsConfig(map);
    }

    public static StreamsConfig createStreamsConfig(String appId) {
        return createStreamsConfig(b -> b.put(StreamsConfig.APPLICATION_ID_CONFIG, appId));
    }

    public void performOperation() {
        try {
            objectKafka.kafkalogger.info("kafkaStreamResourceClass: Performing operation");
            recreateTopics(1, 1, "events");

            Serde<String> stringSerde = Serdes.String();

            objectKafka.kafkalogger.info("kafkaStreamResourceClass: stream produce");

            List<String> keyrecord = Arrays.asList("1", "2", "3", "4", "5");
            keyrecord.forEach(rec ->
                    producer_topics.send(
                            new ProducerRecord<>("events", String.valueOf(rec), String.valueOf(rec))));
            producer_topics.send(
                    new ProducerRecord<>("events", String.valueOf("1"), String.valueOf("1")));
            producer_topics.flush();

            objectKafka.kafkalogger.info("kafkaStreamResourceClass: stream consume");

            var builder = new StreamsBuilder();

            KTable<Windowed<String>, Long> keyCounts = builder
                    .stream("events", Consumed.with(stringSerde, stringSerde))
                    .groupBy((key, word) -> key)
                    .windowedBy(TimeWindows.of(Duration.ofMinutes(5)))
                    .count();

            keyCounts.toStream()
                    .foreach((key, count) -> scalacontainer.zio.postgres.objectKafka.kafkalogger.info("key: " + key + " -> " + count));

            var kafkaStreams = new KafkaStreams(builder.build(), this.createStreamsConfig("events"));
            objectKafka.kafkalogger.info("App Started");
            kafkaStreams.start();
            Thread.sleep(30000);
            //kafkaStreams.close();
            //objectKafka.kafkalogger.info("App Closed");

        } catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private void recreateTopics(int numPartitions, int replicationFactor, String... topics) {
        try (var client = Admin.create(this.adminConfig)) {
            client.deleteTopics(
                    Stream.of(topics)
                            .toList());
            objectKafka.kafkalogger.info("kafkaStreamResourceClass: recreateTopics - delete topics");
            client.createTopics(Stream.of(topics)
                    .map(it -> new NewTopic(it, numPartitions, (short) replicationFactor))
                    .toList());
            objectKafka.kafkalogger.info("kafkaStreamResourceClass: recreateTopics - create topics");
            Thread.sleep(2000);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close()  {
        try {
            scalacontainer.zio.postgres.objectKafka.kafkalogger.info("kafkaStreamResourceClass: Closed");
            producer_topics.close();
        }
        catch (Exception e){
            throw new RuntimeException(e);
        }
    }
}
