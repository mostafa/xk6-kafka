/*

This is a k6 test script that imports the xk6-kafka and
tests Kafka with a 200 JSON messages per iteration. It
also uses SASL authentication.

*/

import { check } from "k6";
import {
    writer,
    produce,
    reader,
    consume,
    createTopic,
    deleteTopic,
    listTopics,
    constants,
    SASL_PLAIN,
    TLS_1_2,
} from "k6/x/kafka"; // import kafka extension

export const options = {
    // This is used for testing purposes. For real-world use, you should use your own options:
    // https://k6.io/docs/using-k6/k6-options/
    scenarios: {
        sasl_auth: {
            executor: "constant-vus",
            vus: 1,
            duration: "10s",
            gracefulStop: "1s",
        },
    },
};

const bootstrapServers = ["localhost:9092"];
const kafkaTopic = "xk6_kafka_json_topic";

// SASL config is optional
const saslConfig = {
    username: "client",
    password: "client-secret",
    // Possible values for the algorithm is:
    // NONE (default)
    // SASL_PLAIN
    // SASL_SCRAM_SHA256
    // SASL_SCRAM_SHA512
    // SASL_SSL (must enable TLS)
    algorithm: SASL_PLAIN,
};

// TLS config is optional
const tlsConfig = {
    // Enable/disable TLS (default: false)
    enableTLS: false,
    // Skip TLS verification if the certificate is invalid or self-signed (default: false)
    insecureSkipTLSVerify: false,
    // Possible values:
    // TLS_1_0
    // TLS_1_1
    // TLS_1_2 (default)
    // TLS_1_3
    minVersion: TLS_1_2,

    // Only needed if you have a custom or self-signed certificate and keys
    // clientCertPem: "/path/to/your/client.pem",
    // clientKeyPem: "/path/to/your/client-key.pem",
    // serverCaPem: "/path/to/your/ca.pem",
};

const offset = 0;
// partition and groupID are mutually exclusive
const partition = 0;
const groupID = "";
const numPartitions = 1;
const replicationFactor = 1;
const compression = "";

const [producer, _writerError] = writer(bootstrapServers, kafkaTopic, saslConfig, tlsConfig);
const [consumer, _readerError] = reader(
    bootstrapServers,
    kafkaTopic,
    partition,
    groupID,
    offset,
    saslConfig,
    tlsConfig
);

if (__VU == 0) {
    createTopic(
        bootstrapServers[0],
        kafkaTopic,
        numPartitions,
        replicationFactor,
        compression,
        saslConfig,
        tlsConfig
    );
    console.log("Existing topics: ", listTopics(bootstrapServers[0], saslConfig, tlsConfig));
}

export default function () {
    for (let index = 0; index < 100; index++) {
        let messages = [
            {
                key: JSON.stringify({
                    correlationId: "test-id-abc-" + index,
                }),
                value: JSON.stringify({
                    name: "xk6-kafka",
                    version: "0.2.1",
                    author: "Mostafa Moradian",
                    description:
                        "k6 extension to load test Apache Kafka with support for Avro messages",
                    index: index,
                }),
            },
            {
                key: JSON.stringify({
                    correlationId: "test-id-def-" + index,
                }),
                value: JSON.stringify({
                    name: "xk6-kafka",
                    version: "0.2.1",
                    author: "Mostafa Moradian",
                    description:
                        "k6 extension to load test Apache Kafka with support for Avro messages",
                    index: index,
                }),
            },
        ];

        let error = produce(producer, messages);
        check(error, {
            "is sent": (err) => err == undefined,
        });
    }

    // Read 10 messages only
    let [messages, _consumeError] = consume(consumer, 10);
    check(messages, {
        "10 messages returned": (msgs) => msgs.length == 10,
    });
}

export function teardown(data) {
    if (__VU == 0) {
        // Delete the topic
        const error = deleteTopic(bootstrapServers[0], kafkaTopic, saslConfig, tlsConfig);
        if (error == null) {
            // If no error returns, it means that the topic
            // is successfully deleted
            console.log("Topic deleted successfully");
        } else {
            console.log("Error while deleting topic: ", error);
        }
    }
    producer.close();
    consumer.close();
}
