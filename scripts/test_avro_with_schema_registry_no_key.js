/*
This is a k6 test script that imports the xk6-kafka and
tests Kafka with a 100 Avro messages per iteration.
*/

import { check } from "k6";
import {
    Writer,
    Reader,
    consumeWithConfiguration,
    produceWithConfiguration,
    createTopic,
    deleteTopic,
    AVRO_SERIALIZER,
    AVRO_DESERIALIZER,
} from "k6/x/kafka"; // import kafka extension

const bootstrapServers = ["localhost:9092"];
const kafkaTopic = "com.example.person";

const writer = new Writer(bootstrapServers, kafkaTopic, null);
const reader = new Reader(bootstrapServers, kafkaTopic, null, "", null, null);

const valueSchema = `{
  "name": "ValueSchema",
  "type": "record",
  "namespace": "com.example",
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
        keyDeserializer: "",
        valueDeserializer: AVRO_DESERIALIZER,
    },
    producer: {
        keySerializer: "",
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
                value: JSON.stringify({
                    firstname: "firstname-" + index,
                    lastname: "lastname-" + index,
                }),
            },
        ];
        let error = produceWithConfiguration(writer, messages, configuration, null, valueSchema);
        check(error, {
            "is sent": (err) => err == undefined,
        });
    }

    let [messages, _consumeError] = consumeWithConfiguration(
        reader,
        20,
        configuration,
        null,
        valueSchema
    );
    check(messages, {
        "20 message returned": (msgs) => msgs.length == 20,
    });

    for (let index = 0; index < messages.length; index++) {
        console.debug("Received Message: " + JSON.stringify(messages[index]));
    }
}

export function teardown(data) {
    if (__VU == 0) {
        // Delete the kafkaTopic
        deleteTopic(bootstrapServers[0], kafkaTopic);
    }
    writer.close();
    reader.close();
}
