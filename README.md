# xk6-kafka

[![GitHub Workflow Status](https://img.shields.io/github/workflow/status/mostafa/xk6-kafka/Build%20and%20publish%20xk6-kafka?logo=github)](https://github.com/mostafa/xk6-kafka/actions) [![Docker Pulls](https://img.shields.io/docker/pulls/mostafamoradian/xk6-kafka?logo=docker)](https://hub.docker.com/r/mostafamoradian/xk6-kafka)

This k6 extension provides the ability to load test Kafka using a producer. You can send many messages with each connection to Kafka. These messages are an array of objects containing a key and a value. There is also a consumer for testing purposes, that is, to make sure you send the correct data to Kafka, but it is not meant to be used for testing Kafka under load. There is support for producing and consuming messages in many formats using various serializers and deserializers. It can fetch schema from Schema Registry and also accepts hard-coded schema. Compression is also supported.

The real purpose of this extension is to test Apache Kafka and the system you've designed that uses Apache Kafka. So, you can test your consumers, and hence your system, by auto-generating messages and sending them to your system via Apache Kafka.

To build the source, you should have the latest version of Go installed. The latest version should match [k6](https://github.com/grafana/k6#build-from-source) and [xk6](https://github.com/grafana/xk6#requirements). I recommend you to have [gvm](https://github.com/moovweb/gvm) installed.

If you want to learn more about the extension, visit [How to Load Test Your Kafka Producers and Consumers using k6](https://k6.io/blog/load-test-your-kafka-producers-and-consumers-using-k6/) article on the k6 blog.

## Supported Features

- Produce/consume messages as String, ByteArray, JSON, and Avro format (custom schema)
- Authentication with SASL PLAIN and SCRAM
- Create and list topics
- Support for user-provided Avro key and value schemas
- Support for loading Avro schemas from Schema Registry
- Support for byte array for binary data (from binary protocols)
- Support consumption from all partitions with a group ID
- Support Kafka message compression: Gzip, Snappy, Lz4 & Zstd

## The Official Docker Image

Since [v0.8.0](https://github.com/mostafa/xk6-kafka/releases/tag/v0.8.0), there is an [official Docker image](https://hub.docker.com/r/mostafamoradian/xk6-kafka) plus binaries in the assets. Before running your script, make sure to make it available to the container by mounting a volume (a directory) or passing it via stdin.

```bash
docker run --rm -i mostafamoradian/xk6-kafka:latest run - <scripts/test_json.js
```

## Build from Source

The k6 binary can be built on various platforms, and each platform has its own set of requirements. The following shows how to build k6 binary with this extension on GNU/Linux distributions.

### Prerequisites

- [gvm](https://github.com/moovweb/gvm) for easier installation and management of Go versions on your machine
- [Git](https://git-scm.com/) for cloning the project
- [xk6](https://github.com/grafana/xk6) for building k6 binary with extensions

### Install and build

Feel free the first two steps if you already have Go installed.

1. Install gvm by following its [installation guide](https://github.com/moovweb/gvm#installing).
2. Install the latest version of Go using gvm. You need Go 1.4 installed for bootstrapping into higher Go versions, as explained [here](https://github.com/moovweb/gvm#a-note-on-compiling-go-15).
3. Install `xk6`:

    ```shell
    go install go.k6.io/xk6/cmd/xk6@latest
    ```

4. Build the binary:

    ```shell
    xk6 build --with github.com/mostafa/xk6-kafka@latest
    ```

**Note:** you can always use the latest version of k6 to build the extension, but the earliest version of k6 that supports extensions via xk6 is v0.32.0. The xk6 is constantly evolving, so some APIs may not be backward compatible.

## Examples

There are lots of examples in the [script](./scripts/) directory that show how to use various features of the extension.

## Run and Test

You can start testing your own environment right away, but it takes some time to develop the script, so it would better to test your script against a development environment, and then start testing your own environment.

### Development environment

I recommend the [fast-data-dev](https://github.com/lensesio/fast-data-dev) Docker image by Lenses.io, a Kafka setup for development that includes Kafka, Zookeeper, Schema Registry, Kafka-Connect, Landoop Tools, 20+ connectors. It is relatively easy to set up if you have Docker installed. Just monitor Docker logs to have a working setup before attempting to test because the initial setup, leader election, and test data ingestion take time.

1. Run the Kafka environment and expose the container ports:

    ```bash
    sudo docker run \
        --detach --rm \
        --name lensesio \
        -p 2181:2181 \
        -p 3030:3030 \
        -p 8081-8083:8081-8083 \
        -p 9581-9585:9581-9585 \
        -p 9092:9092 \
        -e ADV_HOST=127.0.0.1 \
        lensesio/fast-data-dev
    ```

2. After running the command, visit [localhost:3030](http://localhost:3030) to get into the fast-data-dev environment.

3. You can run the command to see the container logs:

    ```bash
    sudo docker logs -f -t lensesio
    ```

> If you have errors running the Kafka development environment, refer to the [fast-data-dev documentation](https://github.com/lensesio/fast-data-dev).

### The xk6-kafka API

All the exported functions are available by importing them from `k6/x/kafka`. They are subject to change in the future versions when a new major version is released. These are the exported functions:

<details>
    <summary>The JavaScript API</summary>

```typescript
/**
 * Create a new Writer object for writing messages to Kafka.
 *
 * @constructor
 * @param   {[string]}  brokers     An array of brokers.
 * @param   {string}    topic       The topic to write to.
 * @param   {string}    auth:       The authentication credentials for SASL PLAIN/SCRAM.
 * @param   {string}    compression The Compression algorithm.
 * @returns {object}    A Writer object.
 */
function writer(brokers: [string], topic: string, auth: string, compression: string) => object {}

/**
 * Write a sequence of messages to Kafka.
 *
 * @function
 * @param   {object}    writer      The writer object created with the writer constructor.
 * @param   {[object]}  messages    An array of message objects containing an optional key and a value.
 * @param   {string}    keySchema   An optional Avro schema for the key.
 * @param   {string}    valueSchema An optional Avro schema for the value.
 * @returns {string}    A string containing the error.
 */
function produce(writer: object, messages: [object], keySchema: string, valueSchema: string) => string {}

/**
 * Write a sequence of messages to Kafka with a specific serializer/deserializer.
 *
 * @function
 * @param   {object}    writer              The writer object created with the writer constructor.
 * @param   {[object]}  messages            An array of message objects containing an optional key and a value.
 * @param   {string}    configurationJson   Serializer, deserializer and schemaRegistry configuration.
 * @param   {string}    keySchema           An optional Avro schema for the key.
 * @param   {string}    valueSchema         An optional Avro schema for the value.
 * @returns {string}    A string containing the error.
 */
function produceWithConfiguration(writer: object, messages: [object], configurationJson: string, keySchema: string, valueSchema: string) => string {}

/**
 * Create a new Reader object for reading messages from Kafka.
 *
 * @constructor
 * @param   {[string]}  brokers     An array of brokers.
 * @param   {string}    topic       The topic to read from.
 * @param   {number}    partition   The partition.
 * @param   {number}    groupID     The group ID.
 * @param   {number}    offset      The offset to begin reading from.
 * @param   {string}    auth        Authentication credentials for SASL PLAIN/SCRAM.
 * @returns {object}    A Reader object.
 */
function reader(brokers: [string], topic: string, partition: number, groupID: string, offset: number, auth: string) => object {}

/**
 * Read a sequence of messages from Kafka.
 *
 * @function
 * @param   {object}    reader      The reader object created with the reader constructor.
 * @param   {number}    limit       How many messages should be read in one go, which blocks. Defaults to 1.
 * @param   {string}    keySchema   An optional Avro schema for the key.
 * @param   {string}    valueSchema An optional Avro schema for the value.
 * @returns {string}    A string containing the error.
 */
function consume(reader: object, limit: number, keySchema: string, valueSchema: string) => string {}

/**
 * Read a sequence of messages from Kafka.
 *
 * @function
 * @param   {object}    reader              The reader object created with the reader constructor.
 * @param   {number}    limit               How many messages should be read in one go, which blocks. Defaults to 1.
 * @param   {string}    configurationJson   Serializer, deserializer and schemaRegistry configuration.
 * @param   {string}    keySchema           An optional Avro schema for the key.
 * @param   {string}    valueSchema         An optional Avro schema for the value.
 * @returns {string}    A string containing the error.
 */
function consumeWithConfiguration(reader: object, limit: number, configurationJson: string, keySchema: string, valueSchema: string) => string {}

/**
 * Create a topic in Kafka. It does nothing if the topic exists.
 *
 * @function
 * @param   {string}    address             The broker address.
 * @param   {string}    topic               The topic name.
 * @param   {number}    partitions          The number of partitions.
 * @param   {number}    replicationFactor   The replication factor in a clustered setup.
 * @param   {string}    compression         The compression algorithm.
 * @param   {string}    auth                Authentication credentials for SASL PLAIN/SCRAM.
 * @returns {string}    A string containing the error.
 */
function createTopic(address: string, topic: string, partitions: number, replicationFactor: number, compression: string, auth: string) => string {}

/**
 * Delete a topic from Kafka. It raises an error if the topic doesn't exist.
 *
 * @function
 * @param   {string}    address             The broker address.
 * @param   {string}    topic               The topic name.
 * @param   {string}    auth                Authentication credentials for SASL PLAIN/SCRAM.
 * @returns {string}    A string containing the error.
 */
function deleteTopic(address: string, topic: string, auth: string) => string {}

/**
 * List all topics in Kafka.
 *
 * @function
 * @param   {string}    address The broker address.
 * @param   {string}    auth    Authentication credentials for SASL PLAIN/SCRAM.
 * @returns {string}    A nested list of strings containing a list of topics and the error (if any).
 */
function listTopics(address: string, auth: string) => [[string], string] {}
```

</details>

### k6 Test Script

The example scripts are available as `test_<format/feature>.js` with more code and commented sections in the [scripts](./scripts/) directory. The scripts usually have 4 parts:

1. The __imports__ at the top show the exported functions from the Go extension and k6.
2. The __Avro schema__ defines a key and a value schema that are used by both producer and consumer, according to the [Avro schema specification](https://avro.apache.org/docs/current/spec.html). These are defined in the [test_avro.js](./scripts/test_avro.js) script.
3. The __message producer__:
    1. The `writer` function is used to open a connection to the bootstrap servers. The first argument is an array of strings that signifies the bootstrap server addresses, and the second is the topic you want to write to. You can reuse this writer object to produce as many messages as possible. This object is created in init code and is reused in the exported default function.
    2. The `produce` function sends a list of messages to Kafka. The first argument is the `producer` object, and the second is the list of messages (with key and value). The third and the fourth arguments are the key schema and value schema in Avro format if Avro format is used. The values are treated as normal strings if the schema are not passed to the function for either the key or the value. Use an empty string, `""` if either of the schema is Avro and the other will be a string. You can use the `produceWithConfiguration` function to pass separate serializer, deserializer, and schema registry settings, as shown in the [test_avro_with_schema_registry js](./scripts/test_avro_with_schema_registry.js) script. The produce function returns an `error` if it fails. The check is optional, but `error` being `undefined` means that `produce` function successfully sent the message.
    3. The `producer.close()` function closes the `producer` object (in `tearDown`).
4. The __message consumer__:
    1. The `reader` function is used to open a connection to the bootstrap servers. The first argument is an array of strings that signifies the bootstrap server addresses, and the second is the topic you want to read from. This object is created in init code and is reused in the exported default function.
    2. The `consume` function is used to read a list of messages from Kafka. The first argument is the `consumer` object, and the second is the number of messages to read in one go. The third and the fourth arguments are the key schema and value schema in Avro format, if Avro format is used. The values are treated as normal strings if the schema are not passed to the function for either the key or the value. Use an empty string, `""` if either of the schema is Avro and the other will be a string. You can use the `consumeWithConfiguration` function to pass separate serializer, deserializer, and schema registry settings, as shown in the [test_avro_with_schema_registry js](./scripts/test_avro_with_schema_registry.js) script. The consume function returns an empty array if it fails. The check is optional, but it checks to see if the length of the message array is exactly 10.
    3. The `consumer.close()` function closes the `consumer` object (in `tearDown`).

You can run k6 with the Kafka extension using the following command:

```bash
./k6 run --vus 50 --duration 60s scripts/test_json.js
```

And here's the test result output:

```bash

          /\      |‾‾| /‾‾/   /‾‾/
     /\  /  \     |  |/  /   /  /
    /  \/    \    |     (   /   ‾‾\
   /          \   |  |\  \ |  (‾)  |
  / __________ \  |__| \__\ \_____/ .io

  execution: local
     script: scripts/test_json.js
     output: -

  scenarios: (100.00%) 1 scenario, 50 max VUs, 1m30s max duration (incl. graceful stop):
           * default: 50 looping VUs for 1m0s (gracefulStop: 30s)


running (1m00.1s), 00/50 VUs, 13598 complete and 0 interrupted iterations
default ✓ [======================================] 50 VUs  1m0s

     ✓ is sent
     ✓ 10 messages returned

     █ teardown

     checks.........................: 100.00% ✓ 1373398      ✗ 0
     data_received..................: 0 B     0 B/s
     data_sent......................: 0 B     0 B/s
     iteration_duration.............: avg=220.82ms min=28.5µs med=215.94ms max=987.93ms p(90)=248.22ms p(95)=258.71ms
     iterations.....................: 13598   226.195625/s
     kafka.reader.dial.count........: 50      0.831724/s
     kafka.reader.error.count.......: 0       0/s
     kafka.reader.fetches.count.....: 50      0.831724/s
     kafka.reader.message.bytes.....: 27 MB   444 kB/s
     kafka.reader.message.count.....: 136030  2262.787977/s
     kafka.reader.rebalance.count...: 0       0/s
     kafka.reader.timeouts.count....: 0       0/s
     kafka.writer.dial.count........: 132     2.195751/s
     kafka.writer.error.count.......: 0       0/s
     kafka.writer.message.bytes.....: 595 MB  9.9 MB/s
     kafka.writer.message.count.....: 2719600 45239.125059/s
     kafka.writer.rebalance.count...: 0       0/s
     kafka.writer.write.count.......: 2719600 45239.125059/s
     vus............................: 50      min=50         max=50
     vus_max........................: 50      min=50         max=50
```

### Troubleshooting

To avoid getting the following error while running the test:

```bash
Failed to write message: [5] Leader Not Available: the cluster is in the middle of a leadership election and there is currently no leader for this partition and hence it is unavailable for writes
```

You can now use `createTopic` function to create topics in Kafka. The `scripts/test_topics.js` script shows how to list topics on all Kakfa partitions and also how to create a topic.

You always have the option to create it using `kafka-topics` command:

```bash
$ docker exec -it lensesio bash
(inside container)$ kafka-topics --create --topic xk6_kafka_avro_topic --bootstrap-server localhost:9092
(inside container)$ kafka-topics --create --topic xk6_kafka_json_topic --bootstrap-server localhost:9092
```

If you want to test SASL authentication, have a look at [this commit message](https://github.com/mostafa/xk6-kafka/pull/3/commits/403fbc48d13683d836b8033eeeefa48bf2f25c6e), where I describe how to run a test environment.

## Contributions, Issues and Feedback

I'd be thrilled to receive contributions and feedback on this piece of software. You're always welcome to create an issue if you find one (or many). I'd do my best to address the issues.

## Disclaimer

This was a proof of concept, but seems to be used by some companies nowadays, but it still isn't supported by the k6 team, rather by [me](https://github.com/mostafa) personally, and the APIs may change in the future. USE AT YOUR OWN RISK!

This work is licensed under the [Apache License 2.0](https://github.com/mostafa/xk6-kafka/blob/master/LICENSE).
