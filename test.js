/*

This is a k6 test script that imports the k6-kafka-plugin and
tests Kafka with a single message per connection.

*/

import { check } from 'k6';
import { kafka } from 'k6-plugin/kafka';  // import kafka plugin

export default function () {
    const output = kafka(
        ["localhost:9092"],  // bootstrap servers
        "test-k6-plugin-topic",  // Kafka topic
        "module-name",  // key
        "k6-plugin-kafka");  // value

    check(output, {
        "is sent": result => result == "Sent"
    });
}
