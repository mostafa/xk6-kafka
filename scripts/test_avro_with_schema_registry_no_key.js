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
const kafkaTopic = "com.example.person";

const [producer, _writerError] = writer(bootstrapServers, kafkaTopic, null);
const [consumer, _readerError] = reader(bootstrapServers, kafkaTopic, null, "", null, null);

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
        valueDeserializer: "io.confluent.kafka.serializers.KafkaAvroDeserializer",
    },
    producer: {
        keySerializer: "",
        valueSerializer: "io.confluent.kafka.serializers.KafkaAvroSerializer",
    },
    schemaRegistry: {
        url: "http://localhost:8081",
    },
});

if (__VU == 1) {
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
        let error = produceWithConfiguration(producer, messages, configuration, null, valueSchema);
        check(error, {
            "is sent": (err) => err == undefined,
        });
    }

    let [messages, _consumeError] = consumeWithConfiguration(
        consumer,
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
    if (__VU == 1) {
        // Delete the kafkaTopic
        const error = deleteTopic(bootstrapServers[0], kafkaTopic);
        if (error === undefined) {
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
