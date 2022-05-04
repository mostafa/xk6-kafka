/*

This is a k6 test script that imports the xk6-kafka and
tests Kafka with a 200 byte array messages per iteration.

*/

import { check } from "k6";
import {
    writer,
    produceWithConfiguration,
    reader,
    consumeWithConfiguration,
    createTopic,
} from "k6/x/kafka"; // import kafka extension

const bootstrapServers = ["localhost:9092"];
const kafkaTopic = "xk6_kafka_byte_array_topic";

const producer = writer(bootstrapServers, kafkaTopic);
const consumer = reader(bootstrapServers, kafkaTopic);

if (__VU == 1) {
    createTopic(bootstrapServers[0], kafkaTopic);
}

var configuration = JSON.stringify({
    producer: {
        keySerializer: "org.apache.kafka.common.serialization.StringSerializer",
        valueSerializer: "org.apache.kafka.common.serialization.ByteArraySerializer",
    },
    consumer: {
        keyDeserializer: "org.apache.kafka.common.serialization.StringDeserializer",
        valueDeserializer: "org.apache.kafka.common.serialization.ByteArrayDeserializer",
    },
});

const payload = "byte array payload";

export default function () {
    for (let index = 0; index < 100; index++) {
        let messages = [
            {
                key: "test-id-abc-" + index,
                value: Array.from(payload, (x) => x.charCodeAt(0)),
            },
            {
                key: "test-id-def-" + index,
                value: Array.from(payload, (x) => x.charCodeAt(0)),
            },
        ];

        let error = produceWithConfiguration(producer, messages, configuration);
        check(error, {
            "is sent": (err) => err == undefined,
        });
    }

    // Read 10 messages only
    let messages = consumeWithConfiguration(consumer, 10, configuration);
    check(messages, {
        "10 messages returned": (msgs) => msgs.length == 10,
        "key starts with 'test-id-' string": (msgs) => msgs[0].key.startsWith("test-id-"),
        "payload is correct": (msgs) => String.fromCharCode(...msgs[0].value) === payload,
    });
}

export function teardown(data) {
    if (__VU == 1) {
        // Delete the topic
        const error = deleteTopic(bootstrapServers[0], kafkaTopic);
        if (error === undefined) {
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
