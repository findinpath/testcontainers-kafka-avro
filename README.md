Production/Consumption of Apache Kafka AVRO messages with testcontainers library
=======================================================================================

This is a showcase on how to test the serialization/deserialization 
of [AVRO](https://avro.apache.org/) messages over Apache
Kafka with the help of [docker](https://www.docker.com/) containers 
(via [testcontainers](https://www.testcontainers.org/) library).
 
The [testcontainers](https://www.testcontainers.org/) library already
offers a [Kafka](https://www.testcontainers.org/modules/kafka/) module
for interacting with [Apache Kafka](https://kafka.apache.org/), but
there is not, at the moment, a testcontainers module for the whole
Confluent environment (Confluent Schema Registry container support is
missing from the module previously mentioned).
As a side note, the containers used do not use the default ports exposed
by default in the artifacts (e.g. : Apache Zookeeper 2181, Apache Kafka 9092,
Confluent Schema Registry 8081), but rather free ports available on the
test machine avoiding therefor possible conflicts with already running
services on the test machine. 

This project provides a functional prototype on how to setup the whole
Confluent environment (including Confluent Schema Registry) via testcontainers.
 
 
For the test environment the following containers will be started:
 
- Apache Zookeeper
- Apache Kafka
- Confluent Schema Registry

 
Once the test environment is started, a <code>BookmarkEvent</code>
value type will be registered on the Confluent Schema Registry container
and also a Apache Kafka topic called <code></code>BookmarkEvents</code>
will be created.
 
The test demo will simply verify whether a message serialized in AVRO 
format can be successfully serialized and sent over Apache Kafka
in order to subsequently be deserialized and read by a consumer.


Use 

```bash
mvn clean install
```

for trying out the project.

The file [testcontainers.properties](src/test/resources/testcontainers.properties) can be
used for overriding the default [docker](https://www.docker.com/) images used for the containers needed in setting 
up the Confluent test environment.
