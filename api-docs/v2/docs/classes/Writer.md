[**xk6-kafka**](../README.md)

---

# ~~Class: Writer~~

Defined in: [index.d.ts:369](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L369)

## Deprecated

Use `Producer` instead. `Writer` remains as a compatibility alias in v2.x.

## Constructors

### Constructor

> **new Writer**(`writerConfig`): `Writer`

Defined in: [index.d.ts:376](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L376)

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

Defined in: [index.d.ts:389](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L389)

#### Returns

`void`

- Nothing.

#### Destructor

#### Description

Close the writer.

---

### ~~produce()~~

> **produce**(`produceConfig`): `void`

Defined in: [index.d.ts:383](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L383)

#### Parameters

##### produceConfig

[`ProduceConfig`](../interfaces/ProduceConfig.md)

Produce configuration.

#### Returns

`void`

- Nothing.

#### Method

Write messages to Kafka.
