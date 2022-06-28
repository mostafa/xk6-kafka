/*

This is a k6 test script that imports the xk6-kafka and
tests Kafka by sending 200 Avro messages per iteration
without any associated key.
*/

import { check } from "k6";
import { Writer, produce, Reader, consume, createTopic, deleteTopic } from "k6/x/kafka"; // import kafka extension

const bootstrapServers = ["localhost:9092"];
const kafkaTopic = "xk6_kafka_avro_topic";

const writer = new Writer(bootstrapServers, kafkaTopic);
const reader = new Reader(bootstrapServers, kafkaTopic);

const valueSchema = JSON.stringify({
    type: "record",
    name: "Value",
    namespace: "dev.mostafa.xk6.kafka",
    fields: [
        {
            name: "name",
            type: "string",
        },
        {
            name: "version",
            type: "string",
        },
        {
            name: "author",
            type: "string",
        },
        {
            name: "description",
            type: "string",
        },
        {
            name: "url",
            type: "string",
        },
        {
            name: "index",
            type: "int",
        },
    ],
});

if (__VU == 0) {
    createTopic(bootstrapServers[0], kafkaTopic);
}

export default function () {
    for (let index = 0; index < 100; index++) {
        let messages = [
            {
                value: JSON.stringify({
                    name: "xk6-kafka",
                    version: "0.2.1",
                    author: "Mostafa Moradian",
                    description:
                        "k6 extension to load test Apache Kafka with support for Avro messages",
                    url: "https://mostafa.dev",
                    index: index,
                }),
            },
            {
                value: JSON.stringify({
                    name: "xk6-kafka",
                    version: "0.2.1",
                    author: "Mostafa Moradian",
                    description:
                        "k6 extension to load test Apache Kafka with support for Avro messages",
                    url: "https://mostafa.dev",
                    index: index,
                }),
            },
        ];
        let error = produce(writer, messages, null, valueSchema);
        check(error, {
            "is sent": (err) => err == undefined,
        });
    }

    // Read 10 messages only
    let [messages, _consumeError] = consume(reader, 10, null, valueSchema);
    check(messages, {
        "10 messages returned": (msgs) => msgs.length == 10,
    });

    for (let index = 0; index < messages.length; index++) {
        console.debug("Received Message: " + JSON.stringify(messages[index]));
    }
}

export function teardown(data) {
    if (__VU == 0) {
        // Delete the topic
        deleteTopic(bootstrapServers[0], kafkaTopic);
    }
    writer.close();
    reader.close();
}
