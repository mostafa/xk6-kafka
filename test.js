/*

This is a k6 test script that imports the k6-kafka-plugin and
tests Kafka with a single message per connection.

*/

import { check } from 'k6';
import { connect, produce, close } from 'k6-plugin/kafka';  // import kafka plugin

export default function () {
    const server = connect(
        ["localhost:9092"],  // bootstrap servers
        "test-k6-plugin-topic",  // Kafka topic
    )

    let error = produce(server,
        [{
            key: "module-name",
            value: "k6-plugin-kafka"
        }, {
            key: "module-version",
            value: "0.0.1"
        }]);

    check(error, {
        "is sent": err => err == undefined
    });

    error = produce(server,
        [{
            key: "module-author",
            value: "Mostafa Moradian"
        }, {
            key: "module-purpose",
            value: "Kafka load testing"
        }]);

    check(error, {
        "is sent": err => err == undefined
    });

    close(server);
}
