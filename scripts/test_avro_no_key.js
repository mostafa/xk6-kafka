/*

This is a k6 test script that imports the xk6-kafka and
tests Kafka by sending 200 Avro messages per iteration
without any associated key.
*/

import { check } from "k6";
import { writer, produce, reader, consume, createTopic, deleteTopic } from "k6/x/kafka"; // import kafka extension

const bootstrapServers = ["localhost:9092"];
const kafkaTopic = "xk6_kafka_avro_topic";

const [producer, _writerError] = writer(bootstrapServers, kafkaTopic);
const [consumer, _readerError] = reader(bootstrapServers, kafkaTopic);

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
        let error = produce(producer, messages, null, valueSchema);
        check(error, {
            "is sent": (err) => err == undefined,
        });
    }

    // Read 10 messages only
    let [messages, _consumeError] = consume(consumer, 10, null, valueSchema);
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
        const error = deleteTopic(bootstrapServers[0], kafkaTopic);
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
