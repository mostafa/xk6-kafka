/*

This is a k6 test script that imports the k6-kafka-plugin and
tests Kafka with a single message per connection.

*/

import { check } from 'k6';
import { writer, produce, reader, consume } from 'k6-plugin/kafka';  // import kafka plugin

const value_schema = JSON.stringify({
    "type": "record",
    "name": "ModuleValue",
    "fields": [
        { "name": "name", "type": "string" },
        { "name": "version", "type": "string" },
        { "name": "author", "type": "string" },
        { "name": "description", "type": "string" }
    ]
});

export default function () {
    const producer = writer(
        ["localhost:9092"],  // bootstrap servers
        "test-k6-plugin-topic",  // Kafka topic
    )

    for (let index = 0; index < 100; index++) {
        let error = produce(producer,
            [{
                key: "DA KEY!",
                value: JSON.stringify({
                    "name": "k6-plugin-kafka",
                    "version": "0.0.1",
                    "author": "Mostafa Moradian",
                    "description": "k6 Plugin to Load Test Apache Kafka"
                })
            }], "", value_schema);

        check(error, {
            "is sent": err => err == undefined
        });
    }

    // If you don't want to use Avro, don't pass key and value schema
    // error = produce(producer,
    //     [{
    //         key: "module-author",
    //         value: "Mostafa Moradian"
    //     }, {
    //         key: "module-purpose",
    //         value: "Kafka load testing"
    //     }]);

    // check(error, {
    //     "is sent": err => err == undefined
    // });
    producer.close();

    const consumer = reader(
        ["localhost:9092"],  // bootstrap servers
        "test-k6-plugin-topic",  // Kafka topic
    )

    // Read 10 messages only
    let messages = consume(consumer, 10, "", value_schema);
    // let messages = consume(consumer, 1);

    // console.log(JSON.stringify(messages));
    check(messages, {
        "10 messages returned": msgs => msgs.length == 10
    })

    consumer.close();
}
