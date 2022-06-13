# xk6-kafka

[![GitHub Workflow Status](https://img.shields.io/github/workflow/status/mostafa/xk6-kafka/Build%20and%20publish%20xk6-kafka?logo=github)](https://github.com/mostafa/xk6-kafka/actions) [![Docker Pulls](https://img.shields.io/docker/pulls/mostafamoradian/xk6-kafka?logo=docker)](https://hub.docker.com/r/mostafamoradian/xk6-kafka) [![Coverage Status](https://coveralls.io/repos/github/mostafa/xk6-kafka/badge.svg?branch=main)](https://coveralls.io/github/mostafa/xk6-kafka?branch=main)

The xk6-kafka project is a [k6 extension](https://k6.io/docs/extensions/guides/what-are-k6-extensions/) that enables k6 users to load test Apache Kafka using a producer and possibly a consumer for debugging.

The real purpose of this extension is to test the system you meticulously designed to use Apache Kafka. So, you can test your consumers, and hence your system, by auto-generating messages and sending them to your system via Apache Kafka.

You can send many messages with each connection to Kafka. These messages are arrays of objects containing a key and a value in various serialization formats, passed via configuration objects. Various serialization formats, including strings, JSON, binary, Avro and JSON Schema, are supported. Avro and JSON Schema can either be fetched from Schema Registry or hard-code directly in the script. SASL PLAIN/SCRAM authentication and message compression are also supported.

For debugging and testing purposes, a consumer is available to make sure you send the correct data to Kafka.

If you want to learn more about the extension, see the [article](https://k6.io/blog/load-test-your-kafka-producers-and-consumers-using-k6/) explaining how to load test your Kafka producers and consumers using k6 on the k6 blog.

## Supported Features

- Produce/consume messages as [String](https://github.com/mostafa/xk6-kafka/blob/main/scripts/test_json.js), [stringified JSON](https://github.com/mostafa/xk6-kafka/blob/main/scripts/test_json.js), [ByteArray](https://github.com/mostafa/xk6-kafka/blob/main/scripts/test_bytes.js), [Avro](https://github.com/mostafa/xk6-kafka/blob/main/scripts/test_avro_with_schema_registry.js) and [JSON Schema](https://github.com/mostafa/xk6-kafka/blob/main/scripts/test_jsonschema_with_schema_registry.js) format
- Support for user-provided [Avro](https://github.com/mostafa/xk6-kafka/blob/main/scripts/test_avro.js) and [JSON Schema](https://github.com/mostafa/xk6-kafka/blob/main/scripts/test_jsonschema_with_schema_registry.js) key and value schemas in the script
- Authentication with [SASL PLAIN and SCRAM](https://github.com/mostafa/xk6-kafka/blob/main/scripts/test_sasl_auth.js)
- Create, list and delete [topics](https://github.com/mostafa/xk6-kafka/blob/main/scripts/test_topics.js)
- Support for loading Avro schemas from [Schema Registry](https://github.com/mostafa/xk6-kafka/blob/main/scripts/test_avro_with_schema_registry.js)
- Support for [byte array](https://github.com/mostafa/xk6-kafka/blob/main/scripts/test_bytes.js) for binary data (from binary protocols)
- Support consumption from all partitions with a group ID
- Support Kafka message compression: Gzip, [Snappy](https://github.com/mostafa/xk6-kafka/blob/main/scripts/test_json_with_snappy_compression.js), Lz4 & Zstd
- Support for sending messages with [no key](https://github.com/mostafa/xk6-kafka/blob/main/scripts/test_avro_no_key.js)
- Support for k6 [thresholds](https://github.com/mostafa/xk6-kafka/blob/e1a810d52112f05d7a66c12740d9885ebb64897e/scripts/test_json.js#L21-L27) on custom Kafka metrics
- Support for [headers](https://github.com/mostafa/xk6-kafka/blob/main/scripts/test_json.js) on produced and consumed messages
- Lots of exported metrics, as shown in the result output of the [k6 test script](#k6-test-script)

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

## The Official Binaries

Since [v0.8.0](https://github.com/mostafa/xk6-kafka/releases/tag/v0.8.0), the binary version of xk6-kafka is built and published on each [release](https://github.com/mostafa/xk6-kafka/releases). For now, the binaries are only published for Linux, MacOS and Windows for `amd64` (`x86_64`) machines. If you want to see an official build for your machine, please build and test xk6-kafka from [source](#build-from-source) and then create an [issue](https://github.com/mostafa/xk6-kafka/issues/new) and I'll add it to the build pipeline and publish binaries on the next release.

## Build from Source

The k6 binary can be built on various platforms, and each platform has its own set of requirements. The following shows how to build k6 binary with this extension on GNU/Linux distributions.

### Prerequisites

To build the source, you should have the latest version of Go installed. The latest version should match [k6](https://github.com/grafana/k6#build-from-source) and [xk6](https://github.com/grafana/xk6#requirements). I recommend you to have [gvm](https://github.com/moovweb/gvm) installed.

- [gvm](https://github.com/moovweb/gvm) for easier installation and management of Go versions on your machine
- [Git](https://git-scm.com/) for cloning the project
- [xk6](https://github.com/grafana/xk6) for building k6 binary with extensions

### Install and build

Feel free to skip the first two steps if you already have Go installed.

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

### Build for development

If you want to add a feature or make a fix, clone the project and build it using the following commands. The xk6 here will force the build to use the local clone, instead of fetching the latest version from the repository. This enable you to update the code and test it locally.

```bash
git clone git@github.com:mostafa/xk6-kafka.git && cd xk6-kafka
xk6 build --with github.com/mostafa/xk6-kafka@latest=.
```

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

All the exported functions are available by importing them from `k6/x/kafka`. They are subject to change in the future versions when a new major version is released. The exported functions are available in the [`index.d.ts`](/index.d.ts) file and they always reflect the latest changes on the `main` branch. The generated documentation can be accessed at [`docs/README.md`](/docs/README.md).

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


running (1m00.1s), 00/50 VUs, 12064 complete and 0 interrupted iterations
default ✓ [======================================] 50 VUs  1m0s

     ✓ Messages are sent
     ✓ 10 messages are received
     ✓ Topic equals to xk6_kafka_json_topic
     ✗ Key is correct
      ↳  2% — ✓ 300 / ✗ 11764
     ✗ Value is correct
      ↳  4% — ✓ 500 / ✗ 11564
     ✓ Header equals {mykey: 'myvalue'}
     ✓ Time is past
     ✓ Partition is zero
     ✓ Offset is gte zero
     ✓ High watermark is gte zero

     █ teardown

     checks.........................: 98.22%  ✓ 1291648      ✗ 23328
     data_received..................: 0 B     0 B/s
     data_sent......................: 0 B     0 B/s
     iteration_duration.............: avg=248.98ms min=20.92µs med=246.77ms max=542.53ms p(90)=279.71ms p(95)=291.04ms
     iterations.....................: 12064   200.622726/s
     kafka.reader.dial.count........: 50      0.831493/s
     kafka.reader.dial.seconds......: avg=57.38µs  min=0s      med=0s       max=77.09ms  p(90)=0s       p(95)=0s
   ✓ kafka.reader.error.count.......: 0       0/s
     kafka.reader.fetch_bytes.max...: 1000000 min=1000000    max=1000000
     kafka.reader.fetch_bytes.min...: 1       min=1          max=1
     kafka.reader.fetch_wait.max....: 200ms   min=200ms      max=200ms
     kafka.reader.fetch.bytes.......: 0 B     0 B/s
     kafka.reader.fetch.size........: 0       0/s
     kafka.reader.fetches.count.....: 50      0.831493/s
     kafka.reader.lag...............: 7787    min=6515       max=11267
     kafka.reader.message.bytes.....: 24 MB   394 kB/s
     kafka.reader.message.count.....: 120690  2007.058751/s
     kafka.reader.offset............: 2440    min=11         max=2460
     kafka.reader.queue.capacity....: 1       min=1          max=1
     kafka.reader.queue.length......: 1       min=0          max=1
     kafka.reader.read.seconds......: avg=0s       min=0s      med=0s       max=0s       p(90)=0s       p(95)=0s
     kafka.reader.rebalance.count...: 0       0/s
     kafka.reader.timeouts.count....: 0       0/s
     kafka.reader.wait.seconds......: avg=24.61µs  min=0s      med=0s       max=62.83ms  p(90)=0s       p(95)=0s
     kafka.writer.acks.required.....: -1      min=-1         max=0
     kafka.writer.async.............: 0.00%   ✓ 0            ✗ 1206400
     kafka.writer.attempts.max......: 0       min=0          max=0
     kafka.writer.batch.bytes.......: 264 MB  4.4 MB/s
     kafka.writer.batch.max.........: 1       min=1          max=1
     kafka.writer.batch.size........: 1206400 20062.272574/s
     kafka.writer.batch.timeout.....: 0s      min=0s         max=0s
   ✓ kafka.writer.error.count.......: 0       0/s
     kafka.writer.message.bytes.....: 528 MB  8.8 MB/s
     kafka.writer.message.count.....: 2412800 40124.545147/s
     kafka.writer.read.timeout......: 0s      min=0s         max=0s
     kafka.writer.retries.count.....: 0       0/s
     kafka.writer.wait.seconds......: avg=0s       min=0s      med=0s       max=0s       p(90)=0s       p(95)=0s
     kafka.writer.write.count.......: 2412800 40124.545147/s
     kafka.writer.write.seconds.....: avg=1.18ms   min=94.91µs med=1.01ms   max=43.7ms   p(90)=1.46ms   p(95)=1.92ms
     kafka.writer.write.timeout.....: 0s      min=0s         max=0s
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
