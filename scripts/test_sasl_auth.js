/*

This is a k6 test script that imports the xk6-kafka and
tests Kafka with a 200 JSON messages per iteration. It
also uses SASL authentication.

*/

import { check } from "k6";
import { writer, produce, reader, consume, createTopic, deleteTopic, listTopics } from "k6/x/kafka"; // import kafka extension

const bootstrapServers = ["localhost:9093"];
const kafkaTopic = "xk6_kafka_json_topic";
const auth = JSON.stringify({
    username: "client",
    password: "client-secret",
    // Possible values for the algorithm is:
    // SASL Plain: "plain" (default if omitted)
    // SASL SCRAM: "sha256", "sha512"
    algorithm: "sha256",
});
const offset = 0;
// partition and groupID are mutually exclusive
const partition = 0;
const groupID = "";
const partitions = 1;
const replicationFactor = 1;
const compression = "";

const [producer, _writerError] = writer(bootstrapServers, kafkaTopic, auth);
const [consumer, _readerError] = reader(
    bootstrapServers,
    kafkaTopic,
    partition,
    groupID,
    offset,
    auth
);

if (__VU == 1) {
    createTopic(bootstrapServers[0], kafkaTopic, partitions, replicationFactor, compression, auth);
    console.log("Existing topics: ", listTopics(bootstrapServers[0], auth));
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
