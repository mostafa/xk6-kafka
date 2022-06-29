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
} from "k6/x/kafka"; // import kafka extension

const bootstrapServers = ["localhost:9092"];
const kafkaTopic = "com.example.person";

const writer = new Writer(bootstrapServers, kafkaTopic);
const reader = new Reader(bootstrapServers, kafkaTopic, null, "", null);

const keySchema = `{
  "name": "KeySchema",
  "type": "record",
  "namespace": "com.example.key",
  "fields": [
    {
      "name": "ssn",
      "type": "string"
    }
  ]
}
`;
const valueSchema = `{
  "name": "ValueSchema",
  "type": "record",
  "namespace": "com.example.value",
  "fields": [
    {
      "name": "firstname",
      "type": "string"
    },
    {
      "name": "lastname",
      "type": "string"
    }
  ]
}`;

var configuration = JSON.stringify({
    consumer: {
        keyDeserializer: AVRO_DESERIALIZER,
        valueDeserializer: AVRO_DESERIALIZER,
    },
    producer: {
        keySerializer: AVRO_SERIALIZER,
        valueSerializer: AVRO_SERIALIZER,
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
                    ssn: "ssn-" + index,
                }),
                value: JSON.stringify({
                    firstname: "firstname-" + index,
                    lastname: "lastname-" + index,
                }),
            },
        ];
        writer.produceWithConfiguration(messages, configuration, keySchema, valueSchema);
    }

    let messages = reader.consumeWithConfiguration(20, configuration, keySchema, valueSchema);
    check(messages, {
        "20 message returned": (msgs) => msgs.length == 20,
    });
}

export function teardown(data) {
    if (__VU == 0) {
        // Delete the topic
        deleteTopic(bootstrapServers[0], kafkaTopic);
    }
    writer.close();
    reader.close();
}
