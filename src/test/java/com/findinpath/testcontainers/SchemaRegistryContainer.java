package com.findinpath.testcontainers;


import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.TestcontainersConfiguration;

import java.io.IOException;
import java.util.HashMap;

import static com.findinpath.testcontainers.Utils.CONFLUENT_PLATFORM_VERSION;
import static com.findinpath.testcontainers.Utils.getRandomFreePort;


public class SchemaRegistryContainer extends GenericContainer<SchemaRegistryContainer> {
    private static final int SCHEMA_REGISTRY_INTERNAL_PORT = 8081;

    private final int exposedPort;
    private final String serviceURL;

    public SchemaRegistryContainer(String zookeeperConnect) throws IOException {
        this(CONFLUENT_PLATFORM_VERSION, zookeeperConnect);
    }

    public SchemaRegistryContainer(String confluentPlatformVersion, String zookeeperConnect) throws IOException {
        super(getSchemaRegistryContainerImage(confluentPlatformVersion));
        exposedPort = getRandomFreePort();

        var env = new HashMap<String, String>();
        env.put("SCHEMA_REGISTRY_KAFKASTORE_CONNECTION_URL", zookeeperConnect);
        env.put("SCHEMA_REGISTRY_HOST_NAME", "localhost");

        withEnv(env);
        addFixedExposedPort(exposedPort, SCHEMA_REGISTRY_INTERNAL_PORT);
        waitingFor(Wait.forHttp("/subjects"));

        this.serviceURL = "http://" + getContainerIpAddress() + ":" + exposedPort;
    }

    public String getServiceURL() {
        return serviceURL;
    }

    public int getExposedPort() {
        return exposedPort;
    }

    private static String getSchemaRegistryContainerImage(String confluentPlatformVersion) {
        return (String) TestcontainersConfiguration
                .getInstance().getProperties().getOrDefault(
                        "schemaregistry.container.image",
                        "confluentinc/cp-schema-registry:" + confluentPlatformVersion
                );
    }
}

