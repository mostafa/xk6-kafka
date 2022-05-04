/*

This is a k6 test script that imports the xk6-kafka and
list topics on all Kafka partitions and creates a topic.

*/

import { createTopic, listTopics } from "k6/x/kafka"; // import kafka extension

const address = "localhost:9092";
const kafkaTopic = "xk6_kafka_test_topic";

const results = listTopics(address);
const error = createTopic(address, kafkaTopic);

export default function () {
    results.forEach((topic) => console.log(topic));

    if (error === undefined) {
        // If no error returns, it means that the topic
        // is successfully created or already exists
        console.log("Topic created successfully");
    } else {
        console.log("Error while creating topic: ", error);
    }
}

export function teardown() {
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
}
