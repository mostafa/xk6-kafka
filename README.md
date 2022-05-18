# xk6-kafka

[![GitHub Workflow Status](https://img.shields.io/github/workflow/status/mostafa/xk6-kafka/Build%20and%20publish%20xk6-kafka?logo=github)](https://github.com/mostafa/xk6-kafka/actions) [![Docker Pulls](https://img.shields.io/docker/pulls/mostafamoradian/xk6-kafka?logo=docker)](https://hub.docker.com/r/mostafamoradian/xk6-kafka)

The xk6-kafka project is a [k6 extension](https://k6.io/docs/extensions/guides/what-are-k6-extensions/) that enables k6 users to load test Apache Kafka using a producer and possibly a consumer for debugging.

The real purpose of this extension is to test the system you meticulously designed to use Apache Kafka. So, you can test your consumers, and hence your system, by auto-generating messages and sending them to your system via Apache Kafka.

You can send many messages with each connection to Kafka. These messages are arrays of objects containing a key and a value in various serialization formats, passed via configuration objects. Various serialization formats, including strings, JSON, binary, Avro and JSONSchema, are supported. Avro schema and JSONSchema can either be fetched from Schema Registry or hard-code directly in the script. SASL PLAIN/SCRAM authentication and message compression are also supported.

For debugging and testing purposes, a consumer is available to make sure you send the correct data to Kafka.

If you want to learn more about the extension, see the [article](https://k6.io/blog/load-test-your-kafka-producers-and-consumers-using-k6/) explaining how to load test your Kafka producers and consumers using k6 on the k6 blog.

## Supported Features

- Produce/consume messages as [String](https://github.com/mostafa/xk6-kafka/blob/main/scripts/test_json.js), [stringified JSON](https://github.com/mostafa/xk6-kafka/blob/main/scripts/test_json.js), [ByteArray](https://github.com/mostafa/xk6-kafka/blob/main/scripts/test_bytes.js), [Avro](https://github.com/mostafa/xk6-kafka/blob/main/scripts/test_avro_with_schema_registry.js) and [JSONSchema](https://github.com/mostafa/xk6-kafka/blob/main/scripts/test_jsonschema_with_schema_registry.js) format
- Support for user-provided [Avro](https://github.com/mostafa/xk6-kafka/blob/main/scripts/test_avro.js) and [JSONSchema](https://github.com/mostafa/xk6-kafka/blob/main/scripts/test_jsonschema_with_schema_registry.js) key and value schemas in the script
- Authentication with [SASL PLAIN and SCRAM](https://github.com/mostafa/xk6-kafka/blob/main/scripts/test_sasl_auth.js)
- Create, list and delete [topics](https://github.com/mostafa/xk6-kafka/blob/main/scripts/test_topics.js)
- Support for loading Avro schemas from [Schema Registry](https://github.com/mostafa/xk6-kafka/blob/main/scripts/test_avro_with_schema_registry.js)
- Support for [byte array](https://github.com/mostafa/xk6-kafka/blob/main/scripts/test_bytes.js) for binary data (from binary protocols)
- Support consumption from all partitions with a group ID
- Support Kafka message compression: Gzip, [Snappy](https://github.com/mostafa/xk6-kafka/blob/main/scripts/test_json_with_snappy_compression.js), Lz4 & Zstd
- Support for sending messages with [no key](https://github.com/mostafa/xk6-kafka/blob/main/scripts/test_avro_no_key.js)
- Support for k6 [thresholds](https://github.com/mostafa/xk6-kafka/blob/e1a810d52112f05d7a66c12740d9885ebb64897e/scripts/test_json.js#L21-L27) on custom Kafka metrics
- Support for [headers](https://github.com/mostafa/xk6-kafka/blob/main/scripts/test_json.js) on produced and consumed messages

## Backward Compatibility Notice

If you want to keep up to date with the latest changes, please follow the [project board](https://github.com/users/mostafa/projects/1). Also, since [v0.9.0](https://github.com/mostafa/xk6-kafka/releases/tag/v0.9.0), the `main` branch is the development branch and usually has the latest changes and might be unstable. If you want to use the latest features, you might need to build your binary by following the [build from source](#build-from-source) instructions. In turn, the tagged releases and the Docker images are more stable.

I make no guarantee to keep the API stable, as this project is in active development unless I release a major version. The best way to keep up with the changes is to follow [the xk6-kafka API](#the-xk6-kafka-api) and look at the [scripts](https://github.com/mostafa/xk6-kafka/blob/main/scripts/) directory.

## CycloneDX SBOM

Since [v0.9.0](https://github.com/mostafa/xk6-kafka/releases/tag/v0.9.0), CycloneDX SBOMs will be generated for [go.mod](go.mod) and it can be accessed from the latest build of GitHub Actions for a tagged release, for example, [this one](https://github.com/mostafa/xk6-kafka/actions/runs/2275475853). The artifacts are only kept for 90 days.

## The Official Docker Image

Since [v0.8.0](https://github.com/mostafa/xk6-kafka/releases/tag/v0.8.0), there is an [official Docker image](https://hub.docker.com/r/mostafamoradian/xk6-kafka) plus binaries in the assets. Before running your script, make sure to make it available to the container by mounting a volume (a directory) or passing it via stdin.

```bash
docker run --rm -i mostafamoradian/xk6-kafka:latest run - <scripts/test_json.js
```

## Build from Source

The k6 binary can be built on various platforms, and each platform has its own set of requirements. The following shows how to build k6 binary with this extension on GNU/Linux distributions.

### Prerequisites

To build the source, you should have the latest version of Go installed. The latest version should match [k6](https://github.com/grafana/k6#build-from-source) and [xk6](https://github.com/grafana/xk6#requirements). I recommend you to have [gvm](https://github.com/moovweb/gvm) installed.

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

There are lots of examples in the [script](https://github.com/mostafa/xk6-kafka/blob/main/scripts/) directory that show how to use various features of the extension.

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

All the exported functions are available by importing them from `k6/x/kafka`. They are subject to change in the future versions when a new major version is released. These are the exported functions and they always reflect the latest changes on the `main` branch.

<details>
    <summary>The JavaScript API</summary>

```typescript
/**
 * Create a new Writer object for writing messages to Kafka.
 *
 * @constructor
 * @param   {[string]}          brokers     An array of brokers.
 * @param   {string}            topic       The topic to write to.
 * @param   {string}            auth:       The authentication credentials for SASL PLAIN/SCRAM.
 * @param   {string}            compression The Compression algorithm.
 * @returns {[object, error]}   An array of two objects: A Writer object and an error object.
 */
function writer(brokers: [string], topic: string, auth: string, compression: string) => [object, error] {}

/**
 * Write a sequence of messages to Kafka.
 *
 * @function
 * @param   {object}    writer      The writer object created with the writer constructor.
 * @param   {[object]}  messages    An array of message objects containing an optional key and a value.
 * @param   {string}    keySchema   An optional Avro/JSONSchema schema for the key.
 * @param   {string}    valueSchema An optional Avro/JSONSchema schema for the value.
 * @returns {object}    A error object.
 */
function produce(writer: object, messages: [object], keySchema: string, valueSchema: string) => error {}

/**
 * Write a sequence of messages to Kafka with a specific serializer/deserializer.
 *
 * @function
 * @param   {object}    writer              The writer object created with the writer constructor.
 * @param   {[object]}  messages            An array of message objects containing an optional key and a value.
 * @param   {string}    configurationJson   Serializer, deserializer and schemaRegistry configuration.
 * @param   {string}    keySchema           An optional Avro/JSONSchema schema for the key.
 * @param   {string}    valueSchema         An optional Avro/JSONSchema schema for the value.
 * @returns {object}    A error object.
 */
function produceWithConfiguration(writer: object, messages: [object], configurationJson: string, keySchema: string, valueSchema: string) => error {}

/**
 * Create a new Reader object for reading messages from Kafka.
 *
 * @constructor
 * @param   {[string]}          brokers     An array of brokers.
 * @param   {string}            topic       The topic to read from.
 * @param   {number}            partition   The partition.
 * @param   {number}            groupID     The group ID.
 * @param   {number}            offset      The offset to begin reading from.
 * @param   {string}            auth        Authentication credentials for SASL PLAIN/SCRAM.
 * @returns {[object, error]}   An array of two objects: A Reader object and an error object.
 */
function reader(brokers: [string], topic: string, partition: number, groupID: string, offset: number, auth: string) => [object, error] {}

/**
 * Read a sequence of messages from Kafka.
 *
 * @function
 * @param   {object}    reader      The reader object created with the reader constructor.
 * @param   {number}    limit       How many messages should be read in one go, which blocks. Defaults to 1.
 * @param   {string}    keySchema   An optional Avro/JSONSchema schema for the key.
 * @param   {string}    valueSchema An optional Avro/JSONSchema schema for the value.
 * @returns {object}    A error object.
 */
function consume(reader: object, limit: number, keySchema: string, valueSchema: string) => error {}

/**
 * Read a sequence of messages from Kafka.
 *
 * @function
 * @param   {object}    reader              The reader object created with the reader constructor.
 * @param   {number}    limit               How many messages should be read in one go, which blocks. Defaults to 1.
 * @param   {string}    configurationJson   Serializer, deserializer and schemaRegistry configuration.
 * @param   {string}    keySchema           An optional Avro/JSONSchema schema for the key.
 * @param   {string}    valueSchema         An optional Avro/JSONSchema schema for the value.
 * @returns {object}    A error object.
 */
function consumeWithConfiguration(reader: object, limit: number, configurationJson: string, keySchema: string, valueSchema: string) => error {}

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
 * @returns {object}    A error object.
 */
function createTopic(address: string, topic: string, partitions: number, replicationFactor: number, compression: string, auth: string) => error {}

/**
 * Delete a topic from Kafka. It raises an error if the topic doesn't exist.
 *
 * @function
 * @param   {string}    address             The broker address.
 * @param   {string}    topic               The topic name.
 * @param   {string}    auth                Authentication credentials for SASL PLAIN/SCRAM.
 * @returns {object}    A error object.
 */
function deleteTopic(address: string, topic: string, auth: string) => error {}

/**
 * List all topics in Kafka.
 *
 * @function
 * @param   {string}    address The broker address.
 * @param   {string}    auth    Authentication credentials for SASL PLAIN/SCRAM.
 * @returns {[[string]], error]}    A nested list of strings containing a list of topics and the error object (if any).
 */
function listTopics(address: string, auth: string) => [[string], error] {}
```

</details>

### k6 Test Script

The example scripts are available as `test_<format/feature>.js` with more code and commented sections in the [scripts](https://github.com/mostafa/xk6-kafka/blob/main/scripts/) directory. The scripts usually have 4 parts:

1. The __imports__ at the top show the exported functions from the Go extension and k6.
2. The __Avro schema__ defines a key and a value schema that are used by both producer and consumer, according to the [Avro schema specification](https://avro.apache.org/docs/current/spec.html). These are defined in the [test_avro.js](https://github.com/mostafa/xk6-kafka/blob/main/scripts/test_avro.js) script.
3. The __message producer__:
    1. The `writer` function is used to open a connection to the bootstrap servers. The first argument is an array of strings that signifies the bootstrap server addresses, and the second is the topic you want to write to. You can reuse this writer object to produce as many messages as possible. This object is created in init code and is reused in the exported default function.
    2. The `produce` function sends a list of messages to Kafka. The first argument is the `producer` object, and the second is the list of messages (with key and value). The third and the fourth arguments are the key schema and value schema in Avro format if Avro format is used. The values are treated as normal strings if the schema are not passed to the function for either the key or the value. Use an empty string, `""` if either of the schema is Avro and the other will be a string. You can use the `produceWithConfiguration` function to pass separate serializer, deserializer, and schema registry settings, as shown in the [test_avro_with_schema_registry.js](https://github.com/mostafa/xk6-kafka/blob/main/scripts/test_avro_with_schema_registry.js) script. The produce function returns an `error` if it fails. The check is optional, but `error` being `undefined` means that `produce` function successfully sent the message.
    3. The `producer.close()` function closes the `producer` object (in `tearDown`).
4. The __message consumer__:
    1. The `reader` function is used to open a connection to the bootstrap servers. The first argument is an array of strings that signifies the bootstrap server addresses, and the second is the topic you want to read from. This object is created in init code and is reused in the exported default function.
    2. The `consume` function is used to read a list of messages from Kafka. The first argument is the `consumer` object, and the second is the number of messages to read in one go. The third and the fourth arguments are the key schema and value schema in Avro format, if Avro format is used. The values are treated as normal strings if the schema are not passed to the function for either the key or the value. Use an empty string, `""` if either of the schema is Avro and the other will be a string. You can use the `consumeWithConfiguration` function to pass separate serializer, deserializer, and schema registry settings, as shown in the [test_avro_with_schema_registry js](https://github.com/mostafa/xk6-kafka/blob/main/scripts/test_avro_with_schema_registry.js) script. The consume function returns an empty array if it fails. The check is optional, but it checks to see if the length of the message array is exactly 10.
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
