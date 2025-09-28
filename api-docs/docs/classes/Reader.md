[**xk6-kafka**](../README.md)

---

# Class: Reader

Defined in: [index.d.ts:365](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L365)

## Classdesc

Reader reads messages from Kafka.

## Example

```javascript
// In init context
const reader = new Reader({
  brokers: ["localhost:9092"],
  topic: "my-topic",
});

// In VU code (default function)
const messages = reader.consume({ limit: 10, nanoPrecision: false });

// In teardown function
reader.close();
```

## Constructors

### Constructor

> **new Reader**(`readerConfig`): `Reader`

Defined in: [index.d.ts:372](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L372)

#### Parameters

##### readerConfig

[`ReaderConfig`](../interfaces/ReaderConfig.md)

Reader configuration.

#### Returns

`Reader`

- Reader instance.

## Methods

### close()

> **close**(): `void`

Defined in: [index.d.ts:385](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L385)

#### Returns

`void`

- Nothing.

#### Destructor

#### Description

Close the reader.

---

### consume()

> **consume**(`consumeConfig`): [`Message`](../interfaces/Message.md)[]

Defined in: [index.d.ts:379](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L379)

#### Parameters

##### consumeConfig

[`ConsumeConfig`](../interfaces/ConsumeConfig.md)

Consume configuration.

#### Returns

[`Message`](../interfaces/Message.md)[]

- Messages.

#### Method

Read messages from Kafka.
