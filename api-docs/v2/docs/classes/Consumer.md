[**xk6-kafka**](../README.md)

---

# Class: Consumer

Defined in: [index.d.ts:443](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L443)

## Classdesc

Consumer reads messages from Kafka.

## Example

```javascript
// In init context
const consumer = new Consumer({
  brokers: ["localhost:9092"],
  topic: "my-topic",
});

// In VU code (default function)
const messages = consumer.consume({ maxMessages: 10, nanoPrecision: false });

// In teardown function
consumer.close();
```

## Constructors

### Constructor

> **new Consumer**(`readerConfig`): `Consumer`

Defined in: [index.d.ts:444](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L444)

#### Parameters

##### readerConfig

[`ReaderConfig`](../interfaces/ReaderConfig.md)

#### Returns

`Consumer`

## Methods

### close()

> **close**(): `void`

Defined in: [index.d.ts:450](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L450)

#### Returns

`void`

---

### commitOffsets()

> **commitOffsets**(): `void`

Defined in: [index.d.ts:448](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L448)

#### Returns

`void`

---

### consume()

> **consume**(`consumeConfig`): [`Message`](../interfaces/Message.md)[]

Defined in: [index.d.ts:445](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L445)

#### Parameters

##### consumeConfig

[`ConsumeConfig`](../interfaces/ConsumeConfig.md)

#### Returns

[`Message`](../interfaces/Message.md)[]

---

### position()

> **position**(`partition`): `number`

Defined in: [index.d.ts:447](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L447)

#### Parameters

##### partition

`number`

#### Returns

`number`

---

### seek()

> **seek**(`partition`, `offset`): `void`

Defined in: [index.d.ts:446](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L446)

#### Parameters

##### partition

`number`

##### offset

`number`

#### Returns

`void`

---

### stats()

> **stats**(): [`ConsumerStats`](../interfaces/ConsumerStats.md)

Defined in: [index.d.ts:449](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L449)

#### Returns

[`ConsumerStats`](../interfaces/ConsumerStats.md)
