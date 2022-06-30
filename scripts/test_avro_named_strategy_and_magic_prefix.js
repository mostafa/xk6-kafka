/*
This is a k6 test script that imports the xk6-kafka and
tests Kafka with a 100 Avro messages per iteration.
*/

import { check } from "k6";
import {
    Writer,
    Reader,
    createTopic,
    deleteTopic,
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
});
const reader = new Reader({
    brokers: brokers,
    topic: topic,
});

let configuration = JSON.stringify({
    consumer: {
        keyDeserializer: "",
        valueDeserializer: AVRO_DESERIALIZER,
        userMagicPrefix: true,
    },
    producer: {
        keySerializer: "",
        valueSerializer: AVRO_SERIALIZER,
        subjectNameStrategy: RECORD_NAME_STRATEGY,
    },
    schemaRegistry: {
        url: "http://localhost:8081",
    },
});

if (__VU == 0) {
    createTopic(brokers[0], topic);
}

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
    writer.produceWithConfiguration([message], configuration, null, valueSchema);

    check(getSubject("com.example.MagicNameValueSchema"), {
        "status is 200": (r) => r.status === 200,
    });

    let messages = reader.consumeWithConfiguration(reader, 1, configuration, null, valueSchema);
    check(messages, {
        "1 message returned": (msgs) => msgs.length === 1,
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
