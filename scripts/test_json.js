/*

This is a k6 test script that imports the xk6-kafka and
tests Kafka with a 200 JSON messages per iteration.

*/

import { check } from "k6";
import { writer, produce, reader, consume, createTopic, deleteTopic } from "k6/x/kafka"; // import kafka extension

const bootstrapServers = ["localhost:9092"];
const kafkaTopic = "xk6_kafka_json_topic";

const producer = writer(bootstrapServers, kafkaTopic);
const consumer = reader(bootstrapServers, kafkaTopic);

if (__VU == 1) {
    createTopic(bootstrapServers[0], kafkaTopic);
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

        let error = produce(producer, messages);
        check(error, {
            "Messages are sent": (err) => err == undefined,
        });
    }

    // Read 10 messages only
    let messages = consume(consumer, 10);
    check(messages, {
        "10 messages are received": (messages) => messages.length == 10,
    });

    check(messages[0], {
        "Topic equals xk6_kafka_json_topic": (msg) => msg["topic"] == kafkaTopic,
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
