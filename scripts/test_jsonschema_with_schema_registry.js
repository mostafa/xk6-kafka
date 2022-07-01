/*
This is a k6 test script that imports the xk6-kafka and
tests Kafka with a 100 Avro messages per iteration.
*/

import { check } from "k6";
import { Writer, Reader, JSON_SCHEMA_SERIALIZER, JSON_SCHEMA_DESERIALIZER } from "k6/x/kafka"; // import kafka extension

const brokers = ["localhost:9092"];
const topic = "xk6_jsonschema_test";

const writer = new Writer({
    brokers: brokers,
    topic: topic,
    autoCreateTopic: true,
});
const reader = new Reader({
    brokers: brokers,
    topic: topic,
});

const keySchema = JSON.stringify({
    title: "Key",
    type: "object",
    properties: {
        key: {
            type: "string",
            description: "A key.",
        },
    },
});

const valueSchema = JSON.stringify({
    title: "Value",
    type: "object",
    properties: {
        firstName: {
            type: "string",
            description: "First name.",
        },
        lastName: {
            type: "string",
            description: "Last name.",
        },
    },
});

var config = JSON.stringify({
    consumer: {
        keyDeserializer: JSON_SCHEMA_DESERIALIZER,
        valueDeserializer: JSON_SCHEMA_DESERIALIZER,
    },
    producer: {
        keySerializer: JSON_SCHEMA_SERIALIZER,
        valueSerializer: JSON_SCHEMA_SERIALIZER,
    },
    schemaRegistry: {
        url: "http://localhost:8081",
    },
});

export default function () {
    for (let index = 0; index < 100; index++) {
        let messages = [
            {
                key: JSON.stringify({
                    key: "key" + index,
                }),
                value: JSON.stringify({
                    firstName: "firstName-" + index,
                    lastName: "lastName-" + index,
                }),
            },
        ];
        writer.produce({
            messages: messages,
            config: config,
            keySchema: keySchema,
            valueSchema: valueSchema,
        });
    }

    let messages = reader.consume({
        limit: 20,
        config: config,
        keySchema: keySchema,
        valueSchema: valueSchema,
    });
    check(messages, {
        "20 message returned": (msgs) => msgs.length == 20,
    });

    check(messages[0], {
        "Topic equals to xk6_jsonschema_test": (msg) => msg.topic == topic,
        "Key is correct": (msg) => msg.key.key == "key0",
        "Value is correct": (msg) =>
            msg.value.firstName == "firstName-0" && msg.value.lastName == "lastName-0",
        "Headers are correct": (msg) => msg.headers.length == 0,
        "Time is past": (msg) => new Date(msg["time"]) < new Date(),
        "Offset is correct": (msg) => msg.offset == 0,
        "Partition is correct": (msg) => msg.partition == 0,
        "High watermark is gte zero": (msg) => msg["highWaterMark"] >= 0,
    });
}

export function teardown(data) {
    if (__VU == 0) {
        // Delete the topic
        connection.deleteTopic(topic);
    }
    writer.close();
    reader.close();
    connection.close();
}
