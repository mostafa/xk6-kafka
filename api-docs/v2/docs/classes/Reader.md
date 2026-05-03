[**xk6-kafka**](../README.md)

---

# ~~Class: Reader~~

Defined in: [index.d.ts:429](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L429)

## Deprecated

Use `Consumer` instead. `Reader` remains as a compatibility alias in v2.x.

## Constructors

### Constructor

> **new Reader**(`readerConfig`): `Reader`

Defined in: [index.d.ts:436](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L436)

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

Defined in: [index.d.ts:449](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L449)

#### Returns

`void`

- Nothing.

#### Destructor

#### Description

Close the reader.

---

### ~~consume()~~

> **consume**(`consumeConfig`): [`Message`](../interfaces/Message.md)[]

Defined in: [index.d.ts:443](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L443)

#### Parameters

##### consumeConfig

[`ConsumeConfig`](../interfaces/ConsumeConfig.md)

Consume configuration.

#### Returns

[`Message`](../interfaces/Message.md)[]

- Messages.

#### Method

Read messages from Kafka.
