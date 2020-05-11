# k6-plugin-kafka

This project is a k6 plugin that can be used to load test Kafka, using a producer. Per each connection to Kafka, many messages can be sent, which is basically an array of objects containing key and value.

In order to build the source, you should have the latest version of Go installed. I recommend you to have [gvm: Go version manager](https://github.com/moovweb/gvm) installed.

This project is not feature-complete, nor something to rely on. USE IT AT YOUR OWN RISK.

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
$ go build -buildmode=plugin -ldflags="-s -w" -o kafka.so
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

The k6 test script is as follows:

```javascript
import { check } from "k6";
import { kafka } from "k6-plugin/kafka"; // import kafka plugin

export default function () {
  const output = kafka(
    ["localhost:9092"], // bootstrap servers
    "test-k6-plugin-topic", // Kafka topic
    [
      {
        key: "module-name",
        value: "k6-plugin-kafka",
      },
      {
        key: "module-version",
        value: "0.0.1",
      },
    ]
  );

  check(output, {
    "is sent": (result) => result == "Sent",
  });
}
```

And here's the test result output:

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

    ✓ is sent

    checks...............: 100.00% ✓ 55594 ✗ 0
    data_received........: 0 B     0 B/s
    data_sent............: 0 B     0 B/s
    iteration_duration...: avg=1.07s min=1s med=1.03s max=3.88s p(90)=1.11s p(95)=1.32s
    iterations...........: 55594   463.283038/s
    vus..................: 500     min=500 max=500
    vus_max..............: 500     min=500 max=500
```
