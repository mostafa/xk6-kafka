[**xk6-kafka**](../README.md)

---

# ~~Class: Reader~~

Defined in: [index.d.ts:428](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L428)

## Deprecated

Use `Consumer` instead. `Reader` remains as a compatibility alias in v2.x.

## Constructors

### Constructor

> **new Reader**(`readerConfig`): `Reader`

Defined in: [index.d.ts:435](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L435)

#### Parameters

##### readerConfig

[`ReaderConfig`](../interfaces/ReaderConfig.md)

Reader configuration.

#### Returns

`Reader`

- Reader instance.

## Methods

### ~~close()~~

> **close**(): `void`

Defined in: [index.d.ts:448](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L448)

#### Returns

`void`

- Nothing.

#### Destructor

#### Description

Close the reader.

---

### ~~consume()~~

> **consume**(`consumeConfig`): [`Message`](../interfaces/Message.md)[]

Defined in: [index.d.ts:442](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L442)

#### Parameters

##### consumeConfig

[`ConsumeConfig`](../interfaces/ConsumeConfig.md)

Consume configuration.

#### Returns

[`Message`](../interfaces/Message.md)[]

- Messages.

#### Method

Read messages from Kafka.
