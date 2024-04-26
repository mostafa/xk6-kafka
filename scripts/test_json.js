/*

This is a k6 test script that imports the xk6-kafka and
tests Kafka with a 200 JSON messages per iteration.

*/

import { check } from "k6";
// import * as kafka from "k6/x/kafka";
import {
  Writer,
  Reader,
  Connection,
  SchemaRegistry,
  CODEC_SNAPPY,
  SCHEMA_TYPE_JSON,
} from "k6/x/kafka"; // import kafka extension

// Prints module-level constants
// console.log(kafka);

const brokers = ["localhost:9092"];
const topic = "xk6_kafka_json_topic";

const writer = new Writer({
  brokers: brokers,
  topic: topic,
  autoCreateTopic: true,
  compression: CODEC_SNAPPY,
});
const reader = new Reader({
  brokers: brokers,
  topic: topic,
});
const connection = new Connection({
  address: brokers[0],
});
const schemaRegistry = new SchemaRegistry();

if (__VU == 0) {
  connection.createTopic({
    topic: topic,
    configEntries: [
      {
        configName: "compression.type",
        configValue: CODEC_SNAPPY,
      },
    ],
  });
}

export const options = {
  thresholds: {
    // Base thresholds to see if the writer or reader is working
    kafka_writer_error_count: ["count == 0"],
    kafka_reader_error_count: ["count == 0"],
  },
};

export default function () {
  for (let index = 0; index < 100; index++) {
    let messages = [
      {
        // The data type of the key is JSON
        key: schemaRegistry.serialize({
          data: {
            correlationId: "test-id-abc-" + index,
          },
          schemaType: SCHEMA_TYPE_JSON,
        }),
        // The data type of the value is JSON
        value: schemaRegistry.serialize({
          data: {
            name: "xk6-kafka",
            version: "0.9.0",
            author: "Mostafa Moradian",
            description:
              "k6 extension to load test Apache Kafka with support for Avro messages",
            index: index,
          },
          schemaType: SCHEMA_TYPE_JSON,
        }),
        headers: {
          mykey: "myvalue",
        },
        offset: index,
        partition: 0,
        time: new Date(), // Will be converted to timestamp automatically
      },
      {
        key: schemaRegistry.serialize({
          data: {
            correlationId: "test-id-def-" + index,
          },
          schemaType: SCHEMA_TYPE_JSON,
        }),
        value: schemaRegistry.serialize({
          data: {
            name: "xk6-kafka",
            version: "0.9.0",
            author: "Mostafa Moradian",
            description:
              "k6 extension to load test Apache Kafka with support for Avro messages",
            index: index,
          },
          schemaType: SCHEMA_TYPE_JSON,
        }),
        headers: {
          mykey: "myvalue",
        },
      },
    ];

    writer.produce({ messages: messages });
  }

  try {
    // Read 10 messages only
    let messages = reader.consume({ limit: 10 });

    check(messages, {
      "10 messages are received": (messages) => messages.length == 10,
    });

    check(messages[0], {
      "Topic equals to xk6_kafka_json_topic": (msg) => msg["topic"] == topic,
      "Key contains key/value and is JSON": (msg) =>
        schemaRegistry
          .deserialize({ data: msg.key, schemaType: SCHEMA_TYPE_JSON })
          .correlationId.startsWith("test-id-"),
      "Value contains key/value and is JSON": (msg) =>
        typeof schemaRegistry.deserialize({
          data: msg.value,
          schemaType: SCHEMA_TYPE_JSON,
        }) == "object" &&
        schemaRegistry.deserialize({
          data: msg.value,
          schemaType: SCHEMA_TYPE_JSON,
        }).name == "xk6-kafka",
      "Header equals {'mykey': 'myvalue'}": (msg) =>
        "mykey" in msg.headers &&
        String.fromCharCode(...msg.headers["mykey"]) == "myvalue",
      "Time is past": (msg) => new Date(msg["time"]) < new Date(),
      "Partition is zero": (msg) => msg["partition"] == 0,
      "Offset is gte zero": (msg) => msg["offset"] >= 0,
      "High watermark is gte zero": (msg) => msg["highWaterMark"] >= 0,
    });
  } catch (error) {
    console.error(error);
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
