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
} from "k6/x/kafka"; // import kafka extension

const bootstrapServers = ["localhost:9092"];
const topic = "com.example.person";

const producer = writer(bootstrapServers, topic, null);
const consumer = reader(bootstrapServers, topic, null, "", null, null);

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
        valueDeserializer:
            "io.confluent.kafka.serializers.KafkaAvroDeserializer",
    },
    producer: {
        keySerializer: "",
        valueSerializer: "io.confluent.kafka.serializers.KafkaAvroSerializer",
    },
    schemaRegistry: {
        url: "http://localhost:8081",
    },
});

createTopic(bootstrapServers[0], topic);

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
        let error = produceWithConfiguration(
            producer,
            messages,
            configuration,
            null,
            valueSchema
        );
        check(error, {
            "is sent": (err) => err == undefined,
        });
    }

    let messages = consumeWithConfiguration(
        consumer,
        20,
        configuration,
        null,
        valueSchema
    );
    check(messages, {
        "20 message returned": (msgs) => msgs.length == 20,
    });
}

export function teardown(data) {
    producer.close();
    consumer.close();
}
