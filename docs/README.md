# xk6-kafka

## Table of contents

### Functions

- [consume](README.md#consume)
- [consumeWithConfiguration](README.md#consumewithconfiguration)
- [createTopic](README.md#createtopic)
- [deleteTopic](README.md#deletetopic)
- [listTopics](README.md#listtopics)
- [produce](README.md#produce)
- [produceWithConfiguration](README.md#producewithconfiguration)
- [reader](README.md#reader)
- [writer](README.md#writer)

## Functions

### consume

▸ **consume**(`reader`, `limit`, `keySchema`, `valueSchema`): [[`Object`], `Object`]

Read a sequence of messages from Kafka.

**`function`**

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `reader` | `Object` | The reader object created with the reader constructor. |
| `limit` | `Number` | How many messages should be read in one go, which blocks. Defaults to 1. |
| `keySchema` | `String` | An optional Avro/JSONSchema schema for the key. |
| `valueSchema` | `String` | An optional Avro/JSONSchema schema for the value. |

#### Returns

[[`Object`], `Object`]

An array of two objects: an array of objects and an error object. Each message object can contain a value and an optional set of key, topic, partition, offset, time, highWaterMark and headers. Headers are objects.

___

### consumeWithConfiguration

▸ **consumeWithConfiguration**(`reader`, `limit`, `configurationJson`, `keySchema`, `valueSchema`): [[`Object`], `Object`]

Read a sequence of messages from Kafka.

**`function`**

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `reader` | `object` | The reader object created with the reader constructor. |
| `limit` | `Number` | How many messages should be read in one go, which blocks. Defaults to 1. |
| `configurationJson` | `String` | Serializer, deserializer and schemaRegistry configuration. |
| `keySchema` | `String` | An optional Avro/JSONSchema schema for the key. |
| `valueSchema` | `String` | An optional Avro/JSONSchema schema for the value. |

#### Returns

[[`Object`], `Object`]

An array of two objects: an array of objects and an error object. Each message object can contain a value and an optional set of key, topic, partition, offset, time, highWaterMark and headers. Headers are objects.

___

### createTopic

▸ **createTopic**(`address`, `topic`, `partitions`, `replicationFactor`, `compression`, `saslConfig`, `tlsConfig`): `Object`

Create a topic in Kafka. It does nothing if the topic exists.

**`function`**

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `address` | `String` | The broker address. |
| `topic` | `String` | The topic name. |
| `partitions` | `Number` | The Number of partitions. |
| `replicationFactor` | `Number` | The replication factor in a clustered setup. |
| `compression` | `String` | The compression algorithm. |
| `saslConfig` | `object` | The SASL configuration. |
| `tlsConfig` | `object` | The TLS configuration. |

#### Returns

`Object`

A error object.

___

### deleteTopic

▸ **deleteTopic**(`address`, `topic`, `saslConfig`, `tlsConfig`): `Object`

Delete a topic from Kafka. It raises an error if the topic doesn't exist.

**`function`**

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `address` | `String` | The broker address. |
| `topic` | `String` | The topic name. |
| `saslConfig` | `Object` | The SASL configuration. |
| `tlsConfig` | `object` | The TLS configuration. |

#### Returns

`Object`

A error object.

___

### listTopics

▸ **listTopics**(`address`, `saslConfig`, `tlsConfig`): [[`String`], `Object`]

List all topics in Kafka.

**`function`**

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `address` | `String` | The broker address. |
| `saslConfig` | `Object` | The SASL configuration. |
| `tlsConfig` | `Object` | The TLS configuration. |

#### Returns

[[`String`], `Object`]

A nested list of strings containing a list of topics and the error object (if any).

___

### produce

▸ **produce**(`writer`, `messages`, `keySchema`, `valueSchema`, `autoCreateTopic`): `Object`

Write a sequence of messages to Kafka.

**`function`**

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `writer` | `Object` | The writer object created with the writer constructor. |
| `messages` | [`Object`] | An array of message objects containing an optional key and a value. Topic, offset and time and headers are also available and optional. Headers are objects. |
| `keySchema` | `String` | An optional Avro/JSONSchema schema for the key. |
| `valueSchema` | `String` | An optional Avro/JSONSchema schema for the value. |
| `autoCreateTopic` | `boolean` | Automatically creates the topic on the first produced message. Defaults to false. |

#### Returns

`Object`

A error object.

___

### produceWithConfiguration

▸ **produceWithConfiguration**(`writer`, `messages`, `configurationJson`, `keySchema`, `valueSchema`, `autoCreateTopic`): `Object`

Write a sequence of messages to Kafka with a specific serializer/deserializer.

**`function`**

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `writer` | `Object` | The writer object created with the writer constructor. |
| `messages` | [`Object`] | An array of message objects containing an optional key and a value. Topic, offset and time and headers are also available and optional. Headers are objects. |
| `configurationJson` | `String` | Serializer, deserializer and schemaRegistry configuration. |
| `keySchema` | `String` | An optional Avro/JSONSchema schema for the key. |
| `valueSchema` | `String` | An optional Avro/JSONSchema schema for the value. |
| `autoCreateTopic` | `boolean` | Automatically creates the topic on the first produced message. Defaults to false. |

#### Returns

`Object`

A error object.

___

### reader

▸ **reader**(`brokers`, `topic`, `partition`, `groupID`, `offset`, `saslConfig`, `tlsConfig`): [`Object`, `Object`]

Create a new Reader object for reading messages from Kafka.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `brokers` | [`String`] | An array of brokers, e.g. ["host:port", ...]. |
| `topic` | `String` | The topic to read from. |
| `partition` | `Number` | The partition. |
| `groupID` | `String` | The group ID. |
| `offset` | `Number` | The offset to begin reading from. |
| `saslConfig` | `object` | The SASL configuration. |
| `tlsConfig` | `object` | The TLS configuration. |

#### Returns

[`Object`, `Object`]

An array of two objects: A Reader object and an error object.

___

### writer

▸ **writer**(`brokers`, `topic`, `saslConfig`, `tlsConfig`, `compression`): [`Object`, `Object`]

Create a new Writer object for writing messages to Kafka.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `brokers` | [`String`] | An array of brokers, e.g. ["host:port", ...]. |
| `topic` | `String` | The topic to write to. |
| `saslConfig` | `object` | The SASL configuration. |
| `tlsConfig` | `object` | The TLS configuration. |
| `compression` | `String` | The Compression algorithm. |

#### Returns

[`Object`, `Object`]

An array of two objects: A Writer object and an error object.
