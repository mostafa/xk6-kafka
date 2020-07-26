# Disclaimer

The [k6](https://github.com/loadimpact/k6) [plugin system](https://github.com/loadimpact/k6/issues/1353) is currently experimental. This plugin is a proof of concept, and it isn't supported by the k6 team, and may break in the future. USE IT AT YOUR OWN RISK!

This project is also a WIP, so it is not feature-complete, nor something to rely on. Over time, I'll try to add a better API that is more natural to both Go and JavaScript.

---

# k6-plugin-kafka

This project is a k6 plugin that can be used to load test Kafka, using a producer. Per each connection to Kafka, many messages can be sent, which is basically an array of objects containing a key and a value. There's also a consumer for testing purposes, e.g. to make sure you send the correct data to Kafka. The consumer is not meant to be used for testing Kafka under load. The plugin supports producing and consuming messages in Avro format, given a schema for key and/or value.

In order to build the source, you should have the latest version of Go installed, which I recommend you to have [gvm](https://github.com/moovweb/gvm), Go version manager, installed.

<!-- 
## Changelog

* v0.0.1
    - [feat] Added a slightly better API to work with the plugin
-->

## Build k6 from source (with plugin support)

This step will be removed once [the plugin support PR](https://github.com/loadimpact/k6/pull/1396) is merged and in production.

```bash
$ go get -d github.com/loadimpact/k6
$ cd $GOPATH/src/github.com/loadimpact/k6
$ git checkout -b andremedeiros-feature/plugins tags/v0.26.2
$ git pull -f https://github.com/andremedeiros/k6.git feature/plugins
$ make
```

## Build plugin from source

```bash
$ go get -d github.com/mostafa/k6-plugin-kafka
$ cd $GOPATH/src/github.com/mostafa/k6-plugin-kafka
$ ./build.sh
$ cp $GOPATH/src/github.com/loadimpact/k6/k6 $GOPATH/src/github.com/mostafa/k6-plugin-kafka
```

## Run & Test

First, you need to have your Kafka development environment setup. I recommend you to use [Lenses.io fast-data-dev Docker image](https://github.com/lensesio/fast-data-dev), which is a complete Kafka setup for development that includes: Kafka, Zookeeper, Schema Registry, Kafka-Connect, Landoop Tools, 20+ connectors. It is fairly easy to setup, if you have Docker installed. Just make sure to monitor Docker logs to have a working setup, before attempting to test.

### Development Environment

After running the following commands, visit [localhost:3030](http://localhost:3030) to get into the fast-data-dev environment.

```bash
$ sudo docker run -d --rm --name lensesio --net=host lensesio/fast-data-dev
$ sudo docker logs -f lensesio
```

### k6 Test

The following k6 test script is used to test this plugin and Apache Kafka in turn. The script is availale as `test.js` with more code and commented sections. The script has 4 parts:

1. The __imports__ at the top shows the exposed functions that are imported from k6 and the plugin, `check` from k6 and the `writer`, `produce`, `reader`, `consume` from the plugin using the `k6-plugin/kafka` plugin loading convention.
2. The __Avro schema__ defines a value schema that is used by both producer and consumer, according to the [Avro schema specification](https://avro.apache.org/docs/current/spec.html).
3. The __Avro message producer__:
    1. The `writer` function is used to open a connection to the bootstrap servers. The first arguments is an array of string that signifies the bootstrap server addresses and the second is the topic you want to write to. You can reuse this writer object to produce as many messages as you want.
    2. The `produce` function is used to send a list of messages to Kafka. The first argument is the `producer` object, the second is the list of messages (with key and value), the third and  the fourth are the key schema and value schema in Avro format. If the schema are not passed to the function, the values are treated as normal strings, as in the key schema, where an empty string, `""`, is passed.
    The produce function returns an `error` if it fails. The check is optional, but `error` being `undefined` means that `produce` function successfully sent the message.
    3. The `producer.close()` function closes the `producer` object.
4. The __Avro message consumer__:
    1. The `reader` function is used to open a connection to the bootstrap servers. The first arguments is an array of string that signifies the bootstrap server addresses and the second is the topic you want to reader from.
    2. The `consume` function is used to read a list of messages from Kafka. The first argument is the `consumer` object, the second is the number of messages to read in one go, the third and  the fourth are the key schema and value schema in Avro format. If the schema are not passed to the function, the values are treated as normal strings, as in the key schema, where an empty string, `""`, is passed.
    The consume function returns an empty array if it fails. The check is optional, but it checks to see if the length of the message array is exactly 10.
    3. The `consumer.close()` function closes the `consumer` object.

```javascript
import { check } from 'k6';
import { writer, produce, reader, consume } from 'k6-plugin/kafka';  // import kafka plugin

# Avro value schema
const value_schema = JSON.stringify({
    "type": "record",
    "name": "ModuleValue",
    "fields": [
        { "name": "name", "type": "string" },
        { "name": "version", "type": "string" },
        { "name": "author", "type": "string" },
        { "name": "description", "type": "string" }
    ]
});

export default function () {
    // Avro message producer
    const producer = writer(
        ["localhost:9092"],  // bootstrap servers
        "test-k6-plugin-topic",  // Kafka topic
    )

    for (let index = 0; index < 100; index++) {
        let error = produce(producer,
            [{
                key: "DA KEY!",
                value: JSON.stringify({
                    "name": "k6-plugin-kafka",
                    "version": "0.0.1",
                    "author": "Mostafa Moradian",
                    "description": "k6 Plugin to Load Test Apache Kafka"
                })
            }], "", value_schema);

        check(error, {
            "is sent": err => err == undefined
        });
    }
    producer.close();

    // Avro message consumer
    const consumer = reader(
        ["localhost:9092"],  // bootstrap servers
        "test-k6-plugin-topic",  // Kafka topic
    )

    // Read 10 messages only
    let messages = consume(consumer, 10, "", value_schema);
    check(messages, {
        "10 messages returned": msgs => msgs.length == 10
    })

    consumer.close();
}
```

You can run k6 with the kafka plugin using the following command:

```bash
$ ./k6 run --vus 1 --duration 10s --plugin=kafka.so test.js
```

And here's the test result output:

```bash

          /\      |‾‾|  /‾‾/  /‾/
     /\  /  \     |  |_/  /  / /
    /  \/    \    |      |  /  ‾‾\  
   /          \   |  |‾\  \ | (_) |
  / __________ \  |__|  \__\ \___/ .io

  execution: local
    plugins: Kafka
     output: -
     script: test.js

    duration: 1m0s, iterations: -
         vus: 50,   max: 50

    done [==========================================================] 1m0s / 1m0s

    ✓ is sent
    ✓ 10 messages returned

    checks.........................: 100.00% ✓ 195794 ✗ 0
    data_received..................: 0 B     0 B/s
    data_sent......................: 0 B     0 B/s
    iteration_duration.............: avg=1.54s min=1s med=1.51s max=2.46s p(90)=1.88s p(95)=1.98s
    iterations.....................: 1910    31.828668/s
    kafka.reader.dial.count........: 1930    32.161952/s
    kafka.reader.error.count.......: 16      0.266628/s
    kafka.reader.fetches.count.....: 1930    32.161952/s
    kafka.reader.message.bytes.....: 1.9 MB  31 kB/s
    kafka.reader.message.count.....: 22882   381.310771/s
    kafka.reader.rebalance.count...: 0       0/s
    kafka.reader.timeouts.count....: 0       0/s
    kafka.writer.dial.count........: 1957    32.611886/s
    kafka.writer.error.count.......: 0       0/s
    kafka.writer.message.bytes.....: 16 MB   265 kB/s
    kafka.writer.message.count.....: 193880  3230.85973/s
    kafka.writer.rebalance.count...: 1957    32.611886/s
    kafka.writer.write.count.......: 193880  3230.85973/s
    vus............................: 50      min=50   max=50
    vus_max........................: 50      min=50   max=50
```

### Known issue/bug

Just ignore the `context canceled` messages for now if you encountered them.
