# Disclaimer

The [k6](https://github.com/loadimpact/k6) [plugin system](https://github.com/loadimpact/k6/issues/1353) is currently experimental. This plugin is a proof of concept, and it isn't supported by the k6 team, and may break in the future. USE IT AT YOUR OWN RISK!

---

# k6-plugin-kafka

This project is a k6 plugin that can be used to load test Kafka, using a producer. Per each connection to Kafka, many messages can be sent, which is basically an array of objects containing key and value.

In order to build the source, you should have the latest version of Go installed. I recommend you to have [gvm: Go version manager](https://github.com/moovweb/gvm) installed.

This project is a WIP, so it is not feature-complete, nor something to rely on. USE IT AT YOUR OWN RISK. Over time, I'll try to add a better API, that is common to both Go and JavaScript.

<!-- 
## Changelog

* v0.0.1
    - [feat] Added a slightly better API to work with the plugin
-->

## Build k6 from source (with plugin support PR)

This step will be removed once [the plugin support PR](https://github.com/loadimpact/k6/pull/1396) is merged and in production.

```bash
$ go get -d github.com/loadimpact/k6
$ cd $GOPATH/src/github.com/loadimpact/k6
$ git checkout -b andremedeiros-feature/plugins master
$ git pull -f https://github.com/andremedeiros/k6.git feature/plugins
$ make
```

## Build plugin from source

```bash
$ go get -d github.com/mostafa/k6-plugin-kafka
$ cd $GOROOT/src/github.com/mostafa/k6-plugin-kafka
$ go build -mod=mod -buildmode=plugin -ldflags="-s -w" -o kafka.so
$ cp kafka.so test.js $GOPATH/src/github.com/loadimpact/k6/
```

## Run & Test

First, you need to have your Kafka development environment setup. I recommend you to use [Lenses.io fast-data-dev Docker image](https://github.com/lensesio/fast-data-dev), which is a complete Kafka setup for development that includes: Kafka, Zookeeper, Schema Registry, Kafka-Connect, Landoop Tools, 20+ connectors. It is fairly easy to setup, if you have Docker installed. Just make sure to monitor Docker logs to have a working setup, before attempting to test.

### Development Environment

Visit [localhost:3030](http://localhost:3030) to get into the fast-data-dev environment.

```bash
$ docker run -d --rm --name lensesio --net=host lensesio/fast-data-dev
$ docker logs -f lensesio
```

### k6 Test

The following k6 test script is pretty self-explanatory, but I'll explain them:

1. Import the exposed methods, namely `connect`, `produce` and `close`, using the `k6-plugin/kafka` convention.
2. `connect` to the bootstrap servers by passing their addresses (as array of string) and the topic you want to write to. You can reuse this server object to produce as many messages as you want, which is discussed next.
3. Send your list of messages to Kafka using the `produce` method by passing the server object and the list of message. It'll produce an `error` if it fails. So, the check is optional, but `error` being `undefined` means that `produce` successfully returned.
4. Close the connection.

```javascript
import { check } from 'k6';
import { connect, produce, close } from 'k6-plugin/kafka';  // import kafka plugin

export default function () {
    const server = connect(
        ["localhost:9092"],  // bootstrap servers
        "test-k6-plugin-topic",  // Kafka topic
    )

    let error = produce(server,
        [{
            key: "module-name",
            value: "k6-plugin-kafka"
        }, {
            key: "module-version",
            value: "0.0.1"
        }]);

    check(error, {
        "is sent": err => err == undefined
    });

    error = produce(server,
        [{
            key: "module-author",
            value: "Mostafa Moradian"
        }, {
            key: "module-purpose",
            value: "Kafka load testing"
        }]);

    check(error, {
        "is sent": err => err == undefined
    });

    close(server);
}
```

And here's the test result output for k6 version 0.26.2:

```bash
$ ./k6 run --vus 500 --duration 2m --plugin=kafka.so test.js


          /\      |‾‾|  /‾‾/  /‾/
     /\  /  \     |  |_/  /  / /
    /  \/    \    |      |  /  ‾‾\
   /          \   |  |‾\  \ | (_) |
  / __________ \  |__|  \__\ \___/ .io

  execution: local
    plugins: Kafka
     output: -
     script: test.js

    duration: 2m0s, iterations: -
         vus: 500,  max: 500

    done [==========================================================] 2m0s / 2m0s

    ✗ is sent
     ↳  97% — ✓ 55263 / ✗ 1556

    checks.........................: 97.25% ✓ 55125 ✗ 1556
    data_received..................: 0 B    0 B/s
    data_sent......................: 0 B    0 B/s
    iteration_duration.............: avg=2.1s min=138.92ms med=2.04s max=5.76s p(90)=2.17s p(95)=2.39s
    iterations.....................: 28280  235.643615/s
    kafka.writer.dial.count........: 27622  230.160818/s
    kafka.writer.error.count.......: 0      0/s
    kafka.writer.message.bytes.....: 2.9 MB 24 kB/s
    kafka.writer.message.count.....: 110250 918.660131/s
    kafka.writer.rebalance.count...: 28400  236.643517/s
    kafka.writer.write.count.......: 55125  459.330066/s
    vus............................: 500    min=500 max=500
    vus_max........................: 500    min=500 max=500
```

And here's the test result output for k6 version 0.27.0 (master branch):

```bash
./k6 run --vus 500 --duration 2m --plugin=kafka.so test.js

          /\      |‾‾|  /‾‾/  /‾/
     /\  /  \     |  |_/  /  / /
    /  \/    \    |      |  /  ‾‾\  
   /          \   |  |‾\  \ | (_) |
  / __________ \  |__|  \__\ \___/ .io

  execution: local
    plugins: Kafka
     output: -
     script: test.js

    duration: 2m0s, iterations: -
         vus: 500,

  execution: local
     script: test.js
     output: -

  scenarios: (100.00%) 1 executors, 500 max VUs, 2m30s max duration (incl. graceful stop):
           * default: 500 looping VUs for 2m0s (gracefulStop: 30s)


running (2m02.0s), 000/500 VUs, 28992 complete and 0 interrupted iterations
default ✓ [======================================] 500 VUs  2m0s


    ✗ is sent
     ↳  99% — ✓ 57636 / ✗ 348

    checks.........................: 99.39% ✓ 57636 ✗ 348
    data_received..................: 0 B    0 B/s
    data_sent......................: 0 B    0 B/s
    iteration_duration.............: avg=2.08s min=66.08ms med=2.05s max=4.21s p(90)=2.15s p(95)=2.31s
    iterations.....................: 28992  237.586486/s
    kafka.writer.dial.count........: 28818  236.160573/s
    kafka.writer.error.count.......: 0      0/s
    kafka.writer.message.bytes.....: 3.1 MB 25 kB/s
    kafka.writer.message.count.....: 115272 944.642293/s
    kafka.writer.rebalance.count...: 28992  237.586486/s
    kafka.writer.write.count.......: 57636  472.321147/s
    vus............................: 0      min=0   max=500
    vus_max........................: 500    min=500 max=500
```
