/*
This is a k6 test script that imports the xk6-kafka and
tests Kafka with a 100 Avro messages per iteration.
*/

import {
    check
} from 'k6';
import {
    writer,
    produce,
    reader,
    consume,
    consumeWithProps,
    produceWithProps
} from 'k6/x/kafka'; // import kafka extension

const bootstrapServers = ["localhost:9092"];
const topic = "com.example.person";

const producer = writer(bootstrapServers, topic);
const consumer = reader(bootstrapServers, topic);

const keySchema = open('schema/key.avro');
const valueSchema = open('schema/value.avro');

var properties = new Map();

properties["schema.registry.urll"] = "http://localhost:8081";
properties["key.serializer"] = "io.confluent.kafka.serializers.KafkaAvroSerializer";
properties["value.serializer"] = "io.confluent.kafka.serializers.KafkaAvroSerializer";
properties["key.deserializer"] = "io.confluent.kafka.serializers.KafkaAvroDeserializer";
properties["value.deserializer"] = "io.confluent.kafka.serializers.KafkaAvroDeserializer";

export default function () {
    for (let index = 0; index < 1; index++) {
        let messages = [{
            key: JSON.stringify({
                "ssn": "ssn-" + index,
            }),
            value: JSON.stringify({
                    "firstname": "firstname-" + index,
                    "lastname": "lastname-" + index,
            }),
        }]
        let error = produceWithProps(producer, messages, properties, keySchema, valueSchema);
        check(error, {
            "is sent": err => err == undefined
        });
    }

    let messages = consumeWithProps(consumer, 1, properties, keySchema, valueSchema);
    console.log('messages ', JSON.stringify(messages))
    check(messages, {
        "10 messages returned": msgs => msgs.length == 1
    })

}

export function teardown(data) {
    producer.close();
    consumer.close();
}
