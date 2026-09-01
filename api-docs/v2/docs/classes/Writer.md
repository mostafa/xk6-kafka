[**xk6-kafka**](../README.md)

---

# ~~Class: Writer~~

Defined in: [index.d.ts:387](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L387)

## Deprecated

Use `Producer` instead. `Writer` remains as a compatibility alias in v2.x.

## Constructors

### Constructor

> **new Writer**(`writerConfig`): `Writer`

Defined in: [index.d.ts:394](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L394)

#### Parameters

##### writerConfig

[`WriterConfig`](../interfaces/WriterConfig.md)

Writer configuration.

#### Returns

`Writer`

- Writer instance.

## Methods

### ~~close()~~

> **close**(): `void`

Defined in: [index.d.ts:407](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L407)

#### Returns

`void`

- Nothing.

#### Destructor

#### Description

Close the writer.

---

### ~~produce()~~

> **produce**(`produceConfig`): `void`

Defined in: [index.d.ts:401](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L401)

#### Parameters

##### produceConfig

[`ProduceConfig`](../interfaces/ProduceConfig.md)

Produce configuration.

#### Returns

`void`

- Nothing.

#### Method

Write messages to Kafka.
