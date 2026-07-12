/*
This is a k6 test script that imports the xk6-kafka and
tests Kerberized Kafka with 1 string message per iteration.
*/

import {
    check,
    sleep
} from "k6";
import {
  Producer,
  Consumer,
  AdminClient,
  SchemaRegistry,
  SCHEMA_TYPE_STRING,
  SASL_GSSAPI,
  TLS_1_2,
} from "k6/x/kafka";

if (!__ENV.BOOTSTRAP_SERVER) {
  throw new Error(`Environment variable BOOTSTRAP_SERVER is missing!`);
}

if (!__ENV.KEYTAB_PATH) {
  throw new Error(`Environment variable KEYTAB_PATH is missing!`);
}

if (!__ENV.PRINCIPAL) {
  throw new Error(`Environment variable PRINCIPAL is missing!`);
}


const brokers = [__ENV.BOOTSTRAP_SERVER];
const topic = "k6-sasl-kerberos-test";
const numPartitions = 1;
const groupId = "k6";

const saslConfig = {
  algorithm: SASL_GSSAPI,
  kerberosConfig: {
    keyTab: __ENV.KEYTAB_PATH,
    principal: __ENV.PRINCIPAL,
  }
};

const tlsConfig = {
  enableTls: false,
}

const producer = new Producer({
  brokers: brokers,
  topic: topic,
  sasl: saslConfig,
  tls: tlsConfig,
});

const consumer = new Consumer({
  brokers: brokers,
  topic: topic,
  groupId: groupId,
  // Default rebalance protocol could not complete in 30 seconds
  groupBalancers: ["group_balancer_round_robin"],
  sasl: saslConfig,
  tls: tlsConfig,
  // Need to allow time for rebalance
  maxWait: "30s",
});

const adminClient = new AdminClient({
  brokers: brokers,
  sasl: saslConfig,
  tls: tlsConfig,
});

const schemaRegistry = new SchemaRegistry();

export function setup() {
    const topics = adminClient
        .listTopics(saslConfig, tlsConfig)
        .map(topic => topic.Topic);

    if (!topics.includes(topic)) {
        adminClient.createTopic({ topic: topic });

        // Wait for Kafka metadata to propagate to all brokers
        sleep(5);
    }
}

export default function main() {
  produce();
  consume();
}

function produce() {
  for (let i = 0; i < 10; i++) {
    let messages = [
      {
        key: schemaRegistry.serialize({
          data: String(i),
          schemaType: SCHEMA_TYPE_STRING,
        }),
        value: schemaRegistry.serialize({
          data: `Hello, Event Hub!`,
          schemaType: SCHEMA_TYPE_STRING,
        }),
      },
    ];

    producer.produce({ messages: messages });
  }
}

function consume() {
  let messages = consumer.consume({ limit: 10 });

  check(messages, {
    "10 messages returned": (msgs) => msgs.length == 10,
    "key is correct": (msgs) =>
      schemaRegistry.deserialize({
        data: msgs[0].key,
        schemaType: SCHEMA_TYPE_STRING,
      }) != undefined,
    "value is correct": (msgs) =>
      schemaRegistry.deserialize({
        data: msgs[0].value,
        schemaType: SCHEMA_TYPE_STRING,
      }) == "Hello, Event Hub!",
  });
}

export function teardown(data) {
  adminClient.deleteTopic(topic);
  adminClient.close();
  producer.close();
  consumer.close();
}
