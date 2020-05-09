# k6-plugin-kafka

This project is a k6 plugin that can be used to load test Kafka, using a producer. Currently only a single message is supported, meaning that per each connection to Kafka, one message is sent. So, if you send thousands/millions of message, each message will be a new connection. This is going to be changed.

In order to build the source, you should have the latest version of Go installed. I recommend you to have [gvm: Go version manager](https://github.com/moovweb/gvm) installed.

This project is not feature-complete, nor something to rely on. USE IT AT YOUR OWN RISK.

## Build plugin from source

```bash
$ go get github.com/mostafa/k6-plugin-kafka
$ go build -buildmode=plugin -o kafka.so github.com/mostafa/k6-plugin-kafka
$ cp kafka.so test.js $GOPATH/src/github.com/loadimpact/k6/
```

## Build k6 from source (plugin support PR)

```bash
$ go get github.com/loadimpact/k6
$ cd $GOPATH/src/github.com/loadimpact/k6
$ git checkout -b andremedeiros-feature/plugins master
$ git pull https://github.com/andremedeiros/k6.git feature/plugins
$ make
```

## Run & Test

First, you need to have your Kafka development environment setup. I recommend you to use [Lenses.io fast-data-dev Docker image](https://github.com/lensesio/fast-data-dev), which is a complete Kafka setup for development that includes: Kafka, Zookeeper, Schema Registry, Kafka-Connect, Landoop Tools, 20+ connectors. It is fairly easy to setup, if you have Docker installed. Just make sure to monitor Docker logs to have a working setup, before attempting to test.

### Development Environment

Visit [localhost:3030](http://localhost:3030) to get into the fast-data-dev environment.

```bash
$ docker run --rm --name lensesio --net=host lensesio/fast-data-dev
$ docker logs -f lensesio
```

### k6 Test

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

    checks...............: 100.00% ✓ 55274 ✗ 0
    data_received........: 0 B     0 B/s
    data_sent............: 0 B     0 B/s
    iteration_duration...: avg=1.08s min=1s med=1.03s max=4.38s p(90)=1.12s p(95)=1.34s
    iterations...........: 55274   460.616385/s
    vus..................: 500     min=500 max=500
    vus_max..............: 500     min=500 max=500
```
