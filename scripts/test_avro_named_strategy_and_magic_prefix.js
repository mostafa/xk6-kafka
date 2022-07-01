/*
This is a k6 test script that imports the xk6-kafka and
tests Kafka with a 100 Avro messages per iteration.
*/

import { check } from "k6";
import {
    Writer,
    Reader,
    Connection,
    AVRO_SERIALIZER,
    AVRO_DESERIALIZER,
    RECORD_NAME_STRATEGY,
} from "k6/x/kafka";
import { getSubject } from "./helpers/schema_registry.js";

const brokers = ["localhost:9092"];
const topic = "test_schema_registry_consume_magic_prefix";

const writer = new Writer({
    brokers: brokers,
    topic: topic,
    autoCreateTopic: true,
});
const reader = new Reader({
    brokers: brokers,
    topic: topic,
});
const connection = new Connection({
    address: brokers[0],
});

if (__VU == 0) {
    connection.createTopic({ topic: topic });
}

let config = JSON.stringify({
    consumer: {
        valueDeserializer: AVRO_DESERIALIZER,
        userMagicPrefix: true,
    },
    producer: {
        valueSerializer: AVRO_SERIALIZER,
        subjectNameStrategy: RECORD_NAME_STRATEGY,
    },
    schemaRegistry: {
        url: "http://localhost:8081",
    },
});

export default function () {
    let message = {
        value: JSON.stringify({
            firstname: "firstname",
            lastname: "lastname",
        }),
    };
    const valueSchema = JSON.stringify({
        name: "MagicNameValueSchema",
        type: "record",
        namespace: "com.example",
        fields: [
            {
                name: "firstname",
                type: "string",
            },
            {
                name: "lastname",
                type: "string",
            },
        ],
    });
    writer.produce({
        messages: [message],
        config: config,
        valueSchema: valueSchema,
    });

    check(getSubject("com.example.MagicNameValueSchema"), {
        "status is 200": (r) => r.status === 200,
    });

    let messages = reader.consume({ limit: 1, config: config, valueSchema: valueSchema });
    check(messages, {
        "1 message returned": (msgs) => msgs.length === 1,
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
