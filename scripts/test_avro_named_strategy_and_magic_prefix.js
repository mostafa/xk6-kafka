/*
This is a k6 test script that imports the xk6-kafka and
tests Kafka with a 100 Avro messages per iteration.
*/

import { check } from "k6";
import {
    Writer,
    Reader,
    consumeWithConfiguration,
    createTopic,
    deleteTopic,
    AVRO_SERIALIZER,
    AVRO_DESERIALIZER,
    RECORD_NAME_STRATEGY,
} from "k6/x/kafka";
import { getSubject } from "./helpers/schema_registry.js";

const bootstrapServers = ["localhost:9092"];
const kafkaTopic = "test_schema_registry_consume_magic_prefix";

const writer = new Writer(bootstrapServers, kafkaTopic, null);
const reader = new Reader(bootstrapServers, kafkaTopic, null, "", null, null);

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
    createTopic(bootstrapServers[0], kafkaTopic);
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

    let [messages, _consumeError] = consumeWithConfiguration(
        reader,
        1,
        configuration,
        null,
        valueSchema
    );
    check(messages, {
        "1 message returned": (msgs) => msgs.length === 1,
    });
}

export function teardown(data) {
    if (__VU == 0) {
        // Delete the kafkaTopic
        deleteTopic(bootstrapServers[0], kafkaTopic);
    }
    writer.close();
    reader.close();
}
