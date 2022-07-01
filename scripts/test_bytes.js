/*

This is a k6 test script that imports the xk6-kafka and
tests Kafka with a 200 byte array messages per iteration.

*/

import { check } from "k6";
import {
    Writer,
    Reader,
    createTopic,
    deleteTopic,
    STRING_SERIALIZER,
    STRING_DESERIALIZER,
    BYTE_ARRAY_SERIALIZER,
    BYTE_ARRAY_DESERIALIZER,
} from "k6/x/kafka"; // import kafka extension

const brokers = ["localhost:9092"];
const topic = "xk6_kafka_byte_array_topic";

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

var config = JSON.stringify({
    producer: {
        keySerializer: STRING_SERIALIZER,
        valueSerializer: BYTE_ARRAY_SERIALIZER,
    },
    consumer: {
        keyDeserializer: STRING_DESERIALIZER,
        valueDeserializer: BYTE_ARRAY_DESERIALIZER,
    },
});

const payload = "byte array payload";

export default function () {
    for (let index = 0; index < 100; index++) {
        let messages = [
            {
                key: "test-id-abc-" + index,
                value: Array.from(payload, (x) => x.charCodeAt(0)),
            },
            {
                key: "test-id-def-" + index,
                value: Array.from(payload, (x) => x.charCodeAt(0)),
            },
        ];

        writer.produce({
            messages: messages,
            config: config,
        });
    }

    // Read 10 messages only
    let messages = reader.consume({
        limit: 10,
        config: config,
    });
    check(messages, {
        "10 messages returned": (msgs) => msgs.length == 10,
        "key starts with 'test-id-' string": (msgs) => msgs[0].key.startsWith("test-id-"),
        "payload is correct": (msgs) => String.fromCharCode(...msgs[0].value) === payload,
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
