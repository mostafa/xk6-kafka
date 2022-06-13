/*
This is a k6 test script that imports the xk6-kafka and
tests Kafka with a 100 Avro messages per iteration.
*/

import { check } from "k6";
import {
    writer,
    reader,
    consumeWithConfiguration,
    produceWithConfiguration,
    createTopic,
    deleteTopic,
} from "k6/x/kafka"; // import kafka extension

const bootstrapServers = ["localhost:9092"];
const kafkaTopic = "xk6_jsonschema_test";

const [producer, _writerError] = writer(bootstrapServers, kafkaTopic, null);
const [consumer, _readerError] = reader(bootstrapServers, kafkaTopic, null, "", null, null);

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

var configuration = JSON.stringify({
    consumer: {
        keyDeserializer: "io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer",
        valueDeserializer: "io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer",
    },
    producer: {
        keySerializer: "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer",
        valueSerializer: "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer",
    },
    schemaRegistry: {
        url: "http://localhost:8081",
    },
});

if (__VU == 0) {
    createTopic(bootstrapServers[0], kafkaTopic);
}

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
        let error = produceWithConfiguration(
            producer,
            messages,
            configuration,
            keySchema,
            valueSchema
        );
        check(error, {
            "is sent": (err) => err == undefined,
        });
    }

    let [messages, _consumeError] = consumeWithConfiguration(
        consumer,
        20,
        configuration,
        keySchema,
        valueSchema
    );
    check(messages, {
        "20 message returned": (msgs) => msgs.length == 20,
    });

    check(messages[0], {
        "Topic equals to xk6_jsonschema_test": (msg) => msg.topic == kafkaTopic,
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
        // Delete the kafkaTopic
        const error = deleteTopic(bootstrapServers[0], kafkaTopic);
        if (error == null) {
            // If no error returns, it means that the kafkaTopic
            // is successfully deleted
            console.log("Topic deleted successfully");
        } else {
            console.log("Error while deleting kafkaTopic: ", error);
        }
    }
    producer.close();
    consumer.close();
}
