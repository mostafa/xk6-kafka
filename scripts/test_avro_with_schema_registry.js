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

const bootstrapServers = ["subdomain.us-east-1.aws.confluent.cloud:9092"];
const topic = "com.example.person";

const auth = JSON.stringify({
    username: "username",
    password: "password",
    algorithm: "plain",
});

const producer = writer({
    brokers: bootstrapServers,
    topic,
    auth
});
const consumer = reader(bootstrapServers, topic, null, "", null, auth);

const keySchema = `{
  "name": "KeySchema",
  "type": "record",
  "namespace": "com.example",
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
        keyDeserializer: "io.confluent.kafka.serializers.KafkaAvroDeserializer",
        valueDeserializer:
            "io.confluent.kafka.serializers.KafkaAvroDeserializer",
    },
    producer: {
        keySerializer: "io.confluent.kafka.serializers.KafkaAvroSerializer",
        valueSerializer: "io.confluent.kafka.serializers.KafkaAvroSerializer",
    },
    schemaRegistry: {
        url: "https://subdomain.us-east-2.aws.confluent.cloud",
        basicAuth: {
            credentialsSource: "USER_INFO",
            userInfo: "KEY:SECRET",
        },
    },
});

createTopic(bootstrapServers[0], topic);

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

    let messages = consumeWithConfiguration(
        consumer,
        20,
        configuration,
        keySchema,
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
