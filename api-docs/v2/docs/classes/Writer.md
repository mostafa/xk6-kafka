[**xk6-kafka**](../README.md)

---

# ~~Class: Writer~~

Defined in: [index.d.ts:401](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L401)

## Deprecated

Use `Producer` instead. `Writer` remains as a compatibility alias in v2.x.

## Constructors

### Constructor

> **new Writer**(`writerConfig`): `Writer`

Defined in: [index.d.ts:408](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L408)

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

Defined in: [index.d.ts:421](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L421)

#### Returns

`void`

- Nothing.

#### Destructor

#### Description

Close the writer.

---

### ~~produce()~~

> **produce**(`produceConfig`): `void`

Defined in: [index.d.ts:415](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L415)

#### Parameters

##### produceConfig

[`ProduceConfig`](../interfaces/ProduceConfig.md)

Produce configuration.

#### Returns

`void`

- Nothing.

#### Method

Write messages to Kafka.
