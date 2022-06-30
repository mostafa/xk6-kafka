/*

This is a k6 test script that imports the xk6-kafka and
tests Kafka with a 200 JSON messages per iteration.

*/

import { check } from "k6";
import { Writer, Reader, createTopic, deleteTopic, CODEC_SNAPPY } from "k6/x/kafka"; // import kafka extension

const brokers = ["localhost:9092"];
const topic = "xk6_kafka_json_snappy_topic";
/*
Supported compression codecs:

- CODEC_GZIP
- CODEC_SNAPPY
- CODEC_LZ4
- CODEC_ZSTD
*/
const compression = CODEC_SNAPPY;

const writer = new Writer({
    brokers: brokers,
    topic: topic,
    compression: compression,
});
const reader = new Reader({
    brokers: brokers,
    topic: topic,
});

const replicationFactor = 1;
const partitions = 1;

if (__VU == 0) {
    // Create the topic or do nothing if the topic exists.
    createTopic(brokers[0], topic, partitions, replicationFactor, compression);
}

export default function () {
    for (let index = 0; index < 100; index++) {
        let messages = [
            {
                key: JSON.stringify({
                    correlationId: "test-id-abc-" + index,
                }),
                value: JSON.stringify({
                    name: "xk6-kafka",
                    version: "0.2.1",
                    author: "Mostafa Moradian",
                    description:
                        "k6 extension to load test Apache Kafka with support for Avro messages",
                    index: index,
                }),
            },
            {
                key: JSON.stringify({
                    correlationId: "test-id-def-" + index,
                }),
                value: JSON.stringify({
                    name: "xk6-kafka",
                    version: "0.2.1",
                    author: "Mostafa Moradian",
                    description:
                        "k6 extension to load test Apache Kafka with support for Avro messages",
                    index: index,
                }),
            },
        ];

        writer.produce(messages);
    }

    // Read 10 messages only
    let messages = reader.consume(10);
    check(messages, {
        "10 messages returned": (msgs) => msgs.length == 10,
    });
}

export function teardown(data) {
    if (__VU == 0) {
        // Delete the topic
        deleteTopic(brokers[0], topic);
    }
    writer.close();
    reader.close();
}
