/*

This is a k6 test script that imports the xk6-kafka and
tests Kafka by sending 200 Avro messages per iteration
without any associated key.
*/

import { check } from "k6";
import { writer, produce, reader, consume, createTopic } from "k6/x/kafka"; // import kafka extension

const bootstrapServers = ["localhost:9092"];
const kafkaTopic = "xk6_kafka_avro_topic";

const producer = writer(bootstrapServers, kafkaTopic);
const consumer = reader(bootstrapServers, kafkaTopic);

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

createTopic(bootstrapServers[0], kafkaTopic);

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
        let error = produce(producer, messages, null, valueSchema);
        check(error, {
            "is sent": (err) => err == undefined,
        });
    }

    // Read 10 messages only
    let messages = consume(consumer, 10, null, valueSchema);
    check(messages, {
        "10 messages returned": (msgs) => msgs.length == 10,
    });
}

export function teardown(data) {
    producer.close();
    consumer.close();
}
