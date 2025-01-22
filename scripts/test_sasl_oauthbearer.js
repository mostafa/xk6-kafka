/*
 * This script shows how to load certificates and keys from
 * a JKS keystore, so that they can be used in a configuring TLS.
 * This is just a showcase, and not a complete script.
 * The keystore MUST be created with JKS storetype.
 *
 * ⚠️ The PKCS#12 format is not supported.
 */

import { check } from "k6";
import {
  Writer,
  Reader,
  Connection,
  SchemaRegistry,
  SCHEMA_TYPE_BYTES,
  TLS_1_2,
  SASL_SSL_OAUTHBEARER
} from "k6/x/kafka"; // import kafka extension

// SASL config is optional
const saslConfig = {
  username: __ENV.AZURE_CLIENT_ID,
  password: __ENV.AZURE_CLIENT_SECRET,
  tenant: __ENV.AZURE_TENANT_ID,
  scope: __ENV.AZURE_SCOPE,
  algorithm: SASL_SSL_OAUTHBEARER,
};


const tlsConfig = {
  enableTls: true,
  insecureSkipTlsVerify: true,
  minVersion: TLS_1_2,

  // The certificates and keys can be loaded from a JKS keystore:
  // clientCertsPem is an array of PEM-encoded certificates, and the filenames
  // will be named "client-cert-0.pem", "client-cert-1.pem", etc.
  // clientKeyPem is the PEM-encoded private key and the filename will be
  // named "client-key.pem".
  // serverCaPem is the PEM-encoded CA certificate and the filename will be
  // named "server-ca.pem".
  // clientCertPem: jks["clientCertsPem"][0], // The first certificate in the chain
  // clientKeyPem: jks["clientKeyPem"],
  //clientCertsPem: jks["clientCertsPem"],
  //serverCaPem: jksTrust["kafka.pem"],
  
  serverCaPem: "fixtures/kafka.pem",
};

//console.log(tlsConfig);

const brokers = ["example.kafka.com:9097"];
const topic = "xk6_kafka_byte_array_topic";

const writer = new Writer({
  brokers: brokers,
  topic: topic,
  autoCreateTopic: true,
  tls: tlsConfig,
  sasl: saslConfig
});
const reader = new Reader({
  brokers: brokers,
  topic: topic,
  tls: tlsConfig,
  sasl: saslConfig
});
const connection = new Connection({
  address: brokers[0],
  tls: tlsConfig,
  sasl: saslConfig
});
const schemaRegistry = new SchemaRegistry();

if (__VU == 0) {
  connection.createTopic({ topic: topic });
}

const payload = "byte array payload";

export default function () {
  for (let index = 0; index < 100; index++) {
    let messages = [
      {
        // The data type of the key is a string
        key: schemaRegistry.serialize({
          data: Array.from("test-id-abc-" + index, (x) => x.charCodeAt(0)),
          schemaType: SCHEMA_TYPE_BYTES,
        }),
        // The data type of the value is a byte array
        value: schemaRegistry.serialize({
          data: Array.from(payload, (x) => x.charCodeAt(0)),
          schemaType: SCHEMA_TYPE_BYTES,
        }),
      },
      {
        key: schemaRegistry.serialize({
          data: Array.from("test-id-def-" + index, (x) => x.charCodeAt(0)),
          schemaType: SCHEMA_TYPE_BYTES,
        }),
        value: schemaRegistry.serialize({
          data: Array.from(payload, (x) => x.charCodeAt(0)),
          schemaType: SCHEMA_TYPE_BYTES,
        }),
      },
    ];

    writer.produce({
      messages: messages,
    });
  }

  // Read 10 messages only
  let messages = reader.consume({ limit: 10 });
  check(messages, {
    "10 messages returned": (msgs) => msgs.length == 10,
    "key starts with 'test-id-' string": (msgs) =>
      String.fromCharCode(
        ...schemaRegistry.deserialize({
          data: msgs[0].key,
          schemaType: SCHEMA_TYPE_BYTES,
        }),
      ).startsWith("test-id-"),
    "value is correct": (msgs) =>
      String.fromCharCode(
        ...schemaRegistry.deserialize({
          data: msgs[0].value,
          schemaType: SCHEMA_TYPE_BYTES,
        }),
      ) == payload,
  });
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