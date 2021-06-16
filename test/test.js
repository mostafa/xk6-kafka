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
    consume
} from 'k6/x/kafka'; // import kafka extension

const bootstrapServers = ["localhost:9092"];
const topic = "com.example.person";

const producer = writer(bootstrapServers, topic);
const consumer = reader(bootstrapServers, topic);

const keySchema = open('schema/key.avro');
const valueSchema = open('schema/value.avro');

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
        let error = produce(producer, messages, keySchema, valueSchema);
        check(error, {
            "is sent": err => err == undefined
        });
    }

    //Read 10 messages only
    let messages = consume(consumer, 1, keySchema, valueSchema);
    console.log('messages ', JSON.stringify(messages))
    check(messages, {
        "10 messages returned": msgs => msgs.length == 1
    })

}

export function teardown(data) {
    producer.close();
    consumer.close();
}
