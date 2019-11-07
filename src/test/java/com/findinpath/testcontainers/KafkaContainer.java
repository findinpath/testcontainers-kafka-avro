package com.findinpath.testcontainers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.TestcontainersConfiguration;

import java.io.IOException;
import java.util.HashMap;

import static com.findinpath.testcontainers.Utils.CONFLUENT_PLATFORM_VERSION;
import static com.findinpath.testcontainers.Utils.getRandomFreePort;

public class KafkaContainer extends GenericContainer<KafkaContainer> {
    private static final int KAFKA_INTERNAL_PORT = 9092;

    private final String networkAlias = "kafka";
    private final int freePort;
    private final String bootstrapServers;

    public KafkaContainer(String zookeeperConnect) throws IOException {
        this(CONFLUENT_PLATFORM_VERSION, zookeeperConnect);
    }

    public KafkaContainer(String confluentPlatformVersion, String zookeeperConnect) throws IOException {
        super(getKafkaContainerImage(confluentPlatformVersion));

        this.freePort = getRandomFreePort();

        var env = new HashMap<String, String>();
        env.put("KAFKA_BROKER_ID", "1");
        env.put("KAFKA_ZOOKEEPER_CONNECT", zookeeperConnect);
        env.put("ZOOKEEPER_SASL_ENABLED", "false");
        env.put("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1");
        env.put("KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:29092,PLAINTEXT_HOST://0.0.0.0:" + KAFKA_INTERNAL_PORT);
        env.put("KAFKA_ADVERTISED_LISTENERS",
                "PLAINTEXT://" + networkAlias + ":29092,PLAINTEXT_HOST://" + getContainerIpAddress() + ":" + freePort);
        env.put("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT");
        env.put("KAFKA_SASL_ENABLED_MECHANISMS", "PLAINTEXT");
        env.put("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT");
        env.put("KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL", "PLAINTEXT");
        withEnv(env);
        withNetworkAliases(networkAlias);
        addFixedExposedPort(freePort, KAFKA_INTERNAL_PORT);


        this.bootstrapServers = getContainerIpAddress() + ":" + freePort;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    private static String getKafkaContainerImage(String confluentPlatformVersion) {
        return (String) TestcontainersConfiguration
                .getInstance().getProperties().getOrDefault(
                        "kafka.container.image",
                        "confluentinc/cp-kafka:" + confluentPlatformVersion
                );
    }
}