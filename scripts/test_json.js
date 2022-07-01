/*

This is a k6 test script that imports the xk6-kafka and
tests Kafka with a 200 JSON messages per iteration.

*/

import { check } from "k6";
// import * as kafka from "k6/x/kafka";
import { Writer, Reader, createTopic, deleteTopic } from "k6/x/kafka"; // import kafka extension

// Prints module-level constants
// console.log(kafka);

const brokers = ["localhost:9092"];
const topic = "xk6_kafka_json_topic";

// The writer and reader functions will be deprecated soon after
// the new constructor changes is released. So, the new syntax need to be used,
// for example:
// const writer = new kafka.Writer(...);
// const reader = new kafka.Reader(...);
const writer = new Writer({
    brokers: brokers,
    topic: topic,
});
const reader = new Reader({
    brokers: brokers,
    topic: topic,
});

if (__VU == 0) {
    createTopic(brokers[0], topic);
}

export const options = {
    thresholds: {
        // Base thresholds to see if the writer or reader is working
        "kafka.writer.error.count": ["count == 0"],
        "kafka.reader.error.count": ["count == 0"],
    },
};

export default function () {
    for (let index = 0; index < 100; index++) {
        let messages = [
            {
                key: JSON.stringify({
                    correlationId: "test-id-abc-" + index,
                }),
                value: JSON.stringify({
                    name: "xk6-kafka",
                    version: "0.9.0",
                    author: "Mostafa Moradian",
                    description:
                        "k6 extension to load test Apache Kafka with support for Avro messages",
                    index: index,
                }),
                headers: {
                    mykey: "myvalue",
                },
                offset: index,
                partition: 0,
                time: new Date(), // Will be converted to timestamp automatically
            },
            {
                key: JSON.stringify({
                    correlationId: "test-id-def-" + index,
                }),
                value: JSON.stringify({
                    name: "xk6-kafka",
                    version: "0.9.0",
                    author: "Mostafa Moradian",
                    description:
                        "k6 extension to load test Apache Kafka with support for Avro messages",
                    index: index,
                }),
                headers: {
                    mykey: "myvalue",
                },
            },
        ];

        writer.produce({ messages: messages });
    }

    // Read 10 messages only
    let messages = reader.consume({ limit: 10 });

    check(messages, {
        "10 messages are received": (messages) => messages.length == 10,
    });

    check(messages[0], {
        "Topic equals to xk6_kafka_json_topic": (msg) => msg["topic"] == topic,
        "Key is correct": (msg) => msg["key"] == JSON.stringify({ correlationId: "test-id-abc-0" }),
        "Value is correct": (msg) =>
            msg["value"] ==
            JSON.stringify({
                name: "xk6-kafka",
                version: "0.9.0",
                author: "Mostafa Moradian",
                description:
                    "k6 extension to load test Apache Kafka with support for Avro messages",
                index: 0,
            }),
        "Header equals {mykey: 'myvalue'}": (msg) =>
            msg.headers[0]["key"] == "mykey" &&
            String.fromCharCode(...msg.headers[0]["value"]) == "myvalue",
        "Time is past": (msg) => new Date(msg["time"]) < new Date(),
        "Partition is zero": (msg) => msg["partition"] == 0,
        "Offset is gte zero": (msg) => msg["offset"] >= 0,
        "High watermark is gte zero": (msg) => msg["highWaterMark"] >= 0,
    });
}

export function teardown(data) {
    if (__VU == 0) {
        // Delete the topic
        deleteTopic(brokers[0], topic);
    }
    writer.close();
    reader.close();
}
