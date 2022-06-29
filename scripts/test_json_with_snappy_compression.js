/*

This is a k6 test script that imports the xk6-kafka and
tests Kafka with a 200 JSON messages per iteration.

*/

import { check } from "k6";
import { Writer, Reader, consume, createTopic, deleteTopic, CODEC_SNAPPY } from "k6/x/kafka"; // import kafka extension

const bootstrapServers = ["localhost:9092"];
const kafkaTopic = "xk6_kafka_json_snappy_topic";
const no_auth = "";
/*
Supported compression codecs:

- CODEC_GZIP
- CODEC_SNAPPY
- CODEC_LZ4
- CODEC_ZSTD
*/
const compression = CODEC_SNAPPY;

const writer = new Writer(bootstrapServers, kafkaTopic, no_auth, compression);
const reader = new Reader(bootstrapServers, kafkaTopic);

const replicationFactor = 1;
const partitions = 1;

if (__VU == 0) {
    // Create the topic or do nothing if the topic exists.
    createTopic(bootstrapServers[0], kafkaTopic, partitions, replicationFactor, compression);
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
    let [messages, _consumeError] = consume(reader, 10);
    check(messages, {
        "10 messages returned": (msgs) => msgs.length == 10,
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
