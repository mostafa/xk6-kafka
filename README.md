# xk6-kafka

This project is a k6 extension that can be used to load test Kafka, using a producer. Per each connection to Kafka, many messages can be sent. These messages are an array of objects containing a key and a value. There is also a consumer for testing purposes, i.e. to make sure you send the correct data to Kafka. The consumer is not meant to be used for testing Kafka under load. The extension supports producing and consuming messages in Avro format, given a schema for key and/or value.

The real purpose of this extension is not only to test Apache Kafka, but also the system you've designed that uses Apache Kafka. So, you can test your consumers, and hence your system, by auto-generating messages and sending them to your system via Apache Kafka.

In order to build the source, you should have the latest version of Go (go1.15) installed. I recommend you to have [gvm](https://github.com/moovweb/gvm) installed.

## Supported Features

* Produce/consume messages in JSON and Avro format (custom schema)
* Authentication with SASL PLAIN and SCRAM
* Create and list topics
* Support for user-provided Avro key and value schemas
* Support for loading Avro schemas from Schema Registry
* Support to consume from all partitions with group ID

## Build

To build a `k6` binary with this extension, first ensure you have the prerequisites:

- [gvm](https://github.com/moovweb/gvm)
- [Git](https://git-scm.com/)

Then, install [xk6](https://github.com/k6io/xk6) and build your custom k6 binary with the Kafka extension:

1. Install `xk6`:
  ```shell
  $ go install github.com/k6io/xk6/cmd/xk6@latest
  ```

2. Build the binary:
  ```shell
  $ xk6 build --with github.com/mostafa/xk6-kafka@latest
  ```

Note: you can always use the latest version of k6 to build the extension, but the earliest version of k6 that supports extensions via xk6 is v0.32.0.

## Run & Test

First, you need to have your Kafka development environment setup. I recommend you to use [Lenses.io fast-data-dev Docker image](https://github.com/lensesio/fast-data-dev), which is a complete Kafka setup for development that includes: Kafka, Zookeeper, Schema Registry, Kafka-Connect, Landoop Tools, 20+ connectors. It is fairly easy to setup, if you have Docker installed. Just make sure to monitor Docker logs to have a working setup, before attempting to test. Initial setup, leader election and test data ingestion takes time.

### Development Environment

After running the following commands, visit [localhost:3030](http://localhost:3030) to get into the fast-data-dev environment.

```bash
$ sudo docker run -d --rm --name lensesio --net=host lensesio/fast-data-dev
$ sudo docker logs -f -t lensesio
```

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

If you want to test SASL authentication, have a look at [this commmit message](https://github.com/mostafa/xk6-kafka/pull/3/commits/216ee0cd4f69864cb259445819541ef34fe2f2dd), where I describe how to run a test environment.

### k6 Test

The following k6 test script is used to test this extension and Apache Kafka in turn. The script is available as `test_<format>.js` with more code and commented sections. The scripts have 4 parts:

1. The __imports__ at the top shows the exposed functions that are imported from k6 and the extension, `check` from k6 and the `writer`, `produce`, `reader`, `consume` from the extension using the `k6/x/kafka` extension loading convention.
2. The __Avro schema__ defines a key and a value schema that are used by both producer and consumer, according to the [Avro schema specification](https://avro.apache.org/docs/current/spec.html). These are defined in the `test_avro.js` script.
3. The __Avro/JSON message producer__:
    1. The `writer` function is used to open a connection to the bootstrap servers. The first argument is an array of strings that signifies the bootstrap server addresses and the second is the topic you want to write to. You can reuse this writer object to produce as many messages as you want. This object is created in init code and is reused in the exported default function.
    2. The `produce` function is used to send a list of messages to Kafka. The first argument is the `producer` object, the second is the list of messages (with key and value). The third and the fourth arguments are the key schema and value schema in Avro format, if Avro format is used. If the schema are not passed to the function for either the key or the value, the values are treated as normal strings. Use an empty string, `""` if either of the schema is Avro and the other is going to be a string.
    The produce function returns an `error` if it fails. The check is optional, but `error` being `undefined` means that `produce` function successfully sent the message.
    3. The `producer.close()` function closes the `producer` object (in `tearDown`).
4. The __Avro/JSON message consumer__:
    1. The `reader` function is used to open a connection to the bootstrap servers. The first argument is an array of strings that signifies the bootstrap server addresses and the second is the topic you want to reader from. This object is created in init code and is reused in the exported default function.
    2. The `consume` function is used to read a list of messages from Kafka. The first argument is the `consumer` object, the second is the number of messages to read in one go. The third and the fourth arguments are the key schema and value schema in Avro format, if Avro format is used. If the schema are not passed to the function for either the key or the value, the values are treated as normal strings. Use an empty string, `""` if either of the schema is Avro and the other is going to be a string.
    The consume function returns an empty array if it fails. The check is optional, but it checks to see if the length of the message array is exactly 10.
    3. The `consumer.close()` function closes the `consumer` object (in `tearDown`).

You can run k6 with the Kafka extension using the following command:

```bash
$ ./k6 run --vus 50 --duration 60s scripts/test_json.js
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


running (1m00.4s), 00/50 VUs, 6554 complete and 0 interrupted iterations
default ✓ [======================================] 50 VUs  1m0s

    ✓ is sent
    ✓ 10 messages returned

    checks.........................: 100.00% ✓ 661954 ✗ 0
    data_received..................: 0 B     0 B/s
    data_sent......................: 0 B     0 B/s
    iteration_duration.............: avg=459.31ms min=188.19ms med=456.26ms max=733.67ms p(90)=543.22ms p(95)=572.76ms
    iterations.....................: 6554    108.563093/s
    kafka.reader.dial.count........: 6554    108.563093/s
    kafka.reader.error.count.......: 0       0/s
    kafka.reader.fetches.count.....: 6554    108.563093/s
    kafka.reader.message.bytes.....: 6.4 MB  106 kB/s
    kafka.reader.message.count.....: 77825   1289.124612/s
    kafka.reader.rebalance.count...: 0       0/s
    kafka.reader.timeouts.count....: 0       0/s
    kafka.writer.dial.count........: 6554    108.563093/s
    kafka.writer.error.count.......: 0       0/s
    kafka.writer.message.bytes.....: 54 MB   890 kB/s
    kafka.writer.message.count.....: 655400  10856.309293/s
    kafka.writer.rebalance.count...: 6554    108.563093/s
    kafka.writer.write.count.......: 655400  10856.309293/s
    vus............................: 50      min=50   max=50
    vus_max........................: 50      min=50   max=50
```

## Disclaimer

This is a proof of concept, isn't supported by the k6 team, and may break in the future. USE AT YOUR OWN RISK!

This work is licensed under the [GNU Affero General Public License v3.0](https://github.com/mostafa/xk6-kafka/blob/master/LICENSE).
