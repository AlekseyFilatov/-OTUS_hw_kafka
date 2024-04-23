package hw3.kafka.otus;

import java.util.*;
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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.errors.TopicExistsException;

public class objectKafka {

    public static Logger kafkalogger = LoggerFactory.getLogger("kafkaResourceApp");
    public static void main(String[] args) throws Exception {

        kafkalogger.info("main start!");
        try (kafkaResourceClass resource = new kafkaResourceClass()) {
            resource.performOperation();
        }
        kafkalogger.info("main stop!");
    }
}

class kafkaResourceClass implements AutoCloseable {

    private String HOST = "localhost:9091";
    private Map<String, Object> adminConfig = Map.of(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, HOST);
    private Properties producerProperties;
    private KafkaProducer<String, String> producer_topics;
    private Properties consumerProperties;
    private KafkaConsumer<String, String> consumer_topics;
    public kafkaResourceClass() {
        try {
            objectKafka.kafkalogger.info("kafkaResourceClass: Acquired");

            producerProperties = new Properties();
            producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            producerProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transaction_marker");
            producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producer_topics = new KafkaProducer<>(producerProperties);

            consumerProperties = new Properties();
            consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
            consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProperties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
            consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
            consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void performOperation() {
        try {
            objectKafka.kafkalogger.info("kafkaResourceClass: Performing operation");
            producer_topics.initTransactions();

            producer_topics.beginTransaction();
            for (int i = 1; i <= 5; i++) {
                ProducerRecord<String, String> record1 = new ProducerRecord<>("topic1", String.valueOf(i));
                ProducerRecord<String, String> record2 = new ProducerRecord<>("topic2", String.valueOf(i));
                producer_topics.send(record1);
                producer_topics.send(record2);
            }
            producer_topics.commitTransaction();
            producer_topics.flush();

            producer_topics.beginTransaction();
            for (int i = 1; i <= 2; i++) {
                ProducerRecord<String, String> record1 = new ProducerRecord<>("topic1", String.valueOf(i));
                ProducerRecord<String, String> record2 = new ProducerRecord<>("topic2", String.valueOf(i));
                producer_topics.send(record1);
                producer_topics.send(record2);
            }
            producer_topics.abortTransaction();
            producer_topics.flush();

            consumer_topics = new KafkaConsumer<>(consumerProperties);
            consumer_topics.subscribe(Arrays.asList("topic1", "topic2"));
            ConsumerRecords<String, String> records_topics = consumer_topics.poll(Duration.ofSeconds(10));

            for (ConsumerRecord<String, String> record : records_topics) {
                System.out.println(record.value());
            }
            consumer_topics.seekToBeginning(consumer_topics.assignment());
            Thread.sleep(1000);

            this.deleteTopics();

        } catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private void deleteTopics() {
        try (var client = Admin.create(this.adminConfig)) {
            client.deleteTopics(Arrays.asList("topic1","topic2"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close()  {
        try {
            objectKafka.kafkalogger.info("CustomResourceClass: Closed");
            producer_topics.close();
            consumer_topics.close();
        }
        catch (Exception e){
            throw new RuntimeException(e);
        }
    }
}