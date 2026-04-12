# Migration Guide

## v2 Constructor Parity

| Old API | New API | Status |
| --- | --- | --- |
| `new Writer(config)` | `new Producer(config)` | `Writer` is a deprecated alias in `v2.x` |
| `new Reader(config)` | `new Consumer(config)` | `Reader` is a deprecated alias in `v2.x` |
| `new Connection(config)` | `new AdminClient(config)` | `Connection` is a deprecated alias in `v2.x` |

## Method Parity

| Old API | New API | Notes |
| --- | --- | --- |
| `writer.produce({ messages })` | `producer.produce({ messages })` | Same payload shape |
| `reader.consume({ limit })` | `consumer.consume({ maxMessages })` | Both keys are accepted in `v2.x` |
| `connection.listTopics()` | `adminClient.listTopics()` | `Connection` returns `string[]`; `AdminClient` returns topic metadata objects |
| `connection.createTopic()` | `adminClient.createTopic()` | Same topic payload shape |
| `connection.deleteTopic()` | `adminClient.deleteTopic()` | Same topic-name argument |

## Config Notes

- `WriterConfig` and `ReaderConfig` remain the input shapes for `Producer` and `Consumer` in `v2.0.0`.
- `ConnectionConfig` now also accepts `brokers` for `AdminClient`. The legacy `Connection` constructor still accepts `address`.
- `ConsumeConfig.maxMessages` is the preferred v2 name. `ConsumeConfig.limit` is still accepted for compatibility.

## Known v2 Differences

- Custom writer balancer configuration is not supported on the Confluent compatibility path. Scripts that rely on `balancer` or a custom balancer callback should stay on the v1 surface until a replacement is implemented.
- `AdminClient.listTopics()` returns structured topic metadata. The deprecated `Connection.listTopics()` alias keeps the old `string[]` shape.
- `SCHEMA_TYPE_PROTOBUF` remains exported, but Protobuf Schema Registry serialization and deserialization are not implemented in `v2.0.0`. The supported Schema Registry formats in `v2.0.0` are Avro and JSON, and the Protobuf serde path is planned for `v2.1`.

## Deprecation Policy

- `Writer`, `Reader`, and `Connection` remain available throughout `v2.x`.
- New examples, docs, and scripts should prefer `Producer`, `Consumer`, and `AdminClient`.
- Removal of the deprecated aliases is planned for the next major version only after replacement coverage and migration notes are complete.
