/*

This is a k6 test script that imports the xk6-kafka and
tests Kafka with a 200 JSON messages per iteration. It
also uses SASL authentication.

*/

import { check, sleep } from "k6";
import {
  Producer,
  Consumer,
  Connection,
  SchemaRegistry,
  SCHEMA_TYPE_JSON,
  SASL_AZURE_ENTRA,
  TLS_1_2,
} from "k6/x/kafka"; // import kafka extension

if (!__ENV.EVENT_HUB_NAMESPACE) {
  throw new Error(`Environment variable EVENT_HUB_NAMESPACE is missing!`);
}

const brokers = [`${__ENV.EVENT_HUB_NAMESPACE}.servicebus.windows.net:9093`];
const topic = "xk6_kafka";

const saslConfig = {
  algorithm: SASL_AZURE_ENTRA,
};

const tlsConfig = {
  enableTls: true,
  insecureSkipTlsVerify: false,
  minVersion: TLS_1_2,
};

const offset = 0;
// partition and groupId are mutually exclusive
const partition = 0;
const numPartitions = 1;
const replicationFactor = 1;

const producer = new Producer({
  brokers: brokers,
  topic: topic,
  sasl: saslConfig,
  tls: tlsConfig,
});
const consumer = new Consumer({
  brokers: brokers,
  topic: topic,
  partition: partition,
  offset: offset,
  sasl: saslConfig,
  tls: tlsConfig,
});
const schemaRegistry = new SchemaRegistry();

export function setup() {
  /*
  const connection = new Connection({
    address: brokers[0],
    sasl: saslConfig,
    tls: tlsConfig,
  });

  connection.createTopic({
    topic: topic,
    numPartitions: numPartitions,
    replicationFactor: replicationFactor,
  });

  // Verify topic was created
  const topics = connection.listTopics(saslConfig, tlsConfig);
  console.log("Existing topics: ", topics);
  if (!topics.includes(topic)) {
    throw new Error(`Topic ${topic} was not created successfully`);
  }

  connection.close();

  // Wait for Kafka metadata to propagate to all brokers
  sleep(2);
  */
}

export default function () {
  for (let index = 0; index < 10; index++) {
    let messages = [
      {
        key: schemaRegistry.serialize({
          data: {
            correlationId: "test-id-abc-" + index,
          },
          schemaType: SCHEMA_TYPE_JSON,
        }),
        value: schemaRegistry.serialize({
          data: {
            name: "xk6-kafka",
          },
          schemaType: SCHEMA_TYPE_JSON,
        }),
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
          },
          schemaType: SCHEMA_TYPE_JSON,
        }),
      },
    ];

    producer.produce({ messages: messages });
  }

  // Read 10 messages only
  let messages = consumer.consume({ limit: 10 });
  check(messages, {
    "10 messages returned": (msgs) => msgs.length == 10,
    "key is correct": (msgs) =>
      schemaRegistry
        .deserialize({ data: msgs[0].key, schemaType: SCHEMA_TYPE_JSON })
        .correlationId.startsWith("test-id-"),
    "value is correct": (msgs) =>
      schemaRegistry.deserialize({
        data: msgs[0].value,
        schemaType: SCHEMA_TYPE_JSON,
      }).name == "xk6-kafka",
  });
}

export function teardown(data) {
  /*
  const connection = new Connection({
    address: brokers[0],
    sasl: saslConfig,
    tls: tlsConfig,
  });
  connection.deleteTopic(topic);
  connection.close();
  */
  producer.close();
  consumer.close();
}
