/*
This is a k6 test script that imports the xk6-kafka and
tests Kafka with a 100 Avro messages per iteration.
*/

import { check } from "k6";
import { Writer, Reader, Connection, AVRO_SERIALIZER, AVRO_DESERIALIZER } from "k6/x/kafka"; // import kafka extension

const brokers = ["localhost:9092"];
const topic = "com.example.person";

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

var config = JSON.stringify({
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
        writer.produce({
            messages: messages,
            config: config,
            valueSchema: valueSchema,
        });
    }

    let messages = reader.consume({ limit: 20, config: config, valueSchema: valueSchema });
    check(messages, {
        "20 message returned": (msgs) => msgs.length == 20,
    });

    for (let index = 0; index < messages.length; index++) {
        console.debug("Received Message: " + JSON.stringify(messages[index]));
    }
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
