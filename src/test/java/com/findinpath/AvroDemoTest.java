package com.findinpath;

import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import com.findinpath.avro.BookmarkEvent;
import com.findinpath.testcontainers.KafkaContainer;
import com.findinpath.testcontainers.SchemaRegistryContainer;
import com.findinpath.testcontainers.ZookeeperContainer;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startables;

/**
 * This is a showcase on how to test the serialization/deserialization of AVRO messages over Apache
 * Kafka with the help of Docker containers (via
 * <a href="https://www.testcontainers.org/>testcontainers</a> library).
 *
 * For the test environment the following containers will be started:
 *
 * <ul>
 *   <li>Apache Zookeeper</li>
 *   <li>Apache Kafka</li>
 *   <li>Confluent Schema Registry</li>
 * </ul>
 *
 * Once the test environment is started, a <code>BookmarkEvent</code>
 * value type will be registered on the Confluent Schema Registry container
 * and also a Apache Kafka topic called <code></code>BookmarkEvents</code>
 * will be created.
 *
 * The {@link #demo()} test will simply verify whether a message
 * serialized in AVRO format can be successfully serialized and sent over Apache Kafka
 * in order to subsequently be deserialized and read by a consumer.
 */
public class AvroDemoTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(AvroDemoTest.class);
  private static final String TOPIC = "BookmarkEvents";
  private static final String URL = "https://www.findinpath.com";
  private static final long POLL_INTERVAL_MS = 100L;
  private static final long POLL_TIMEOUT_MS = 10_000L;

  private static Network network;

  private static ZookeeperContainer zookeeperContainer;
  private static KafkaContainer kafkaContainer;
  private static SchemaRegistryContainer schemaRegistryContainer;

  @BeforeAll
  public static void confluentSetup() throws Exception {
    network = Network.newNetwork();
    zookeeperContainer = new ZookeeperContainer()
        .withNetwork(network);
    kafkaContainer = new KafkaContainer(zookeeperContainer.getInternalUrl())
        .withNetwork(network);
    schemaRegistryContainer = new SchemaRegistryContainer(zookeeperContainer.getInternalUrl())
        .withNetwork(network);

    Startables
        .deepStart(Stream.of(zookeeperContainer, kafkaContainer, schemaRegistryContainer))
        .join();

    createTopics();
    registerSchemaRegistryTypes();
  }

  /**
   * This demo test simply verifies whether an AVRO encoded record is successfully serialized and
   * sent over Apache Kafka by a producer in order to subsequently be deserialized and read by a
   * consumer.
   */
  @Test
  public void demo() {

    final UUID userUuid = UUID.randomUUID();
    final BookmarkEvent bookmarkEvent = new BookmarkEvent(userUuid.toString(), URL,
        Instant.now().toEpochMilli());

    produce(TOPIC, bookmarkEvent);
    LOGGER.info(
        String.format("Successfully sent 1 BookmarkEvent message to the topic called %s", TOPIC));

    var consumerRecords = dumpTopic(TOPIC, 1, POLL_TIMEOUT_MS);
    LOGGER.info(String.format("Retrieved %d consumer records from the topic %s",
        consumerRecords.size(), TOPIC));

    assertThat(consumerRecords.size(), equalTo(1));
    assertThat(consumerRecords.get(0).key(), equalTo(bookmarkEvent.getUserUuid()));
    assertThat(consumerRecords.get(0).value(), equalTo(bookmarkEvent));
  }

  private static void produce(String topic, BookmarkEvent bookmarkEvent) {
    try (KafkaProducer<String, BookmarkEvent> producer = createBookmarkEventKafkaProducer()) {
      final ProducerRecord<String, BookmarkEvent> record = new ProducerRecord<>(
          topic, bookmarkEvent.getUserUuid(), bookmarkEvent);
      producer.send(record);
      producer.flush();
    } catch (final SerializationException e) {
      LOGGER.error(String.format(
          "Serialization exception occurred while trying to send message %s to the topic %s",
          bookmarkEvent, topic), e);
    }
  }

  private static KafkaProducer<String, BookmarkEvent> createBookmarkEventKafkaProducer() {
    final Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.RETRIES_CONFIG, 0);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
        schemaRegistryContainer.getUrl());
    props.put(KafkaAvroSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY,
        TopicNameStrategy.class.getName());

    return new KafkaProducer<>(props);
  }

  private List<ConsumerRecord<String, BookmarkEvent>> dumpTopic(
      String topic,
      int minMessageCount,
      long pollTimeoutMillis) {

    List<ConsumerRecord<String, BookmarkEvent>> consumerRecords = new ArrayList<>();
    var consumerGroupId = UUID.randomUUID().toString();
    try (final KafkaConsumer<String, BookmarkEvent> consumer = createBookmarkEventKafkaConsumer(
        consumerGroupId)) {

      // assign the consumer to all the partitions of the topic
      var topicPartitions = consumer.partitionsFor(TOPIC).stream()
          .map(partitionInfo -> new TopicPartition(TOPIC, partitionInfo.partition()))
          .collect(Collectors.toList());
      consumer.assign(topicPartitions);

      var start = System.currentTimeMillis();
      while (true) {
        final ConsumerRecords<String, BookmarkEvent> records = consumer
            .poll(Duration.ofMillis(POLL_INTERVAL_MS));

        records.forEach(consumerRecords::add);
        if (consumerRecords.size() >= minMessageCount) {
          break;
        }

        if (System.currentTimeMillis() - start > pollTimeoutMillis) {
          throw new IllegalStateException(
              String.format(
                  "Timed out while waiting for %d messages from the %s. Only %d messages received so far.",
                  minMessageCount, topic, consumerRecords.size()));
        }
      }
    }
    return consumerRecords;
  }

  private static KafkaConsumer<String, BookmarkEvent> createBookmarkEventKafkaConsumer(
      String consumerGroupId) {
    final Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
        schemaRegistryContainer.getUrl());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
    props.put(KafkaAvroSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY,
        TopicNameStrategy.class.getName());

    return new KafkaConsumer<>(props);
  }


  private static AdminClient createAdminClient() {
    var properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());

    return KafkaAdminClient.create(properties);
  }

  private static void registerSchemaRegistryTypes() throws IOException, RestClientException {
    // the type BookmarkEvent needs to be registered manually in the Confluent schema registry
    // to setup the tests.
    LOGGER.info("Registering manually in the Schema Registry the types used in the tests");
    var schemaRegistryClient = new CachedSchemaRegistryClient(
        schemaRegistryContainer.getUrl(), 1000);
    schemaRegistryClient
        .register(BookmarkEvent.getClassSchema().getFullName(), BookmarkEvent.getClassSchema());
  }

  private static void createTopics() throws InterruptedException, ExecutionException {
    try (var adminClient = createAdminClient()) {
      short replicationFactor = 1;
      int partitions = 1;

      LOGGER.info("Creating topics in Apache Kafka");
      adminClient.createTopics(
          singletonList(new NewTopic(TOPIC, partitions, replicationFactor))
      ).all().get();
    }
  }
}
