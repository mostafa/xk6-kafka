[**xk6-kafka**](../README.md)

---

# Class: Consumer

Defined in: [index.d.ts:413](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L413)

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

Defined in: [index.d.ts:414](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L414)

#### Parameters

##### readerConfig

[`ReaderConfig`](../interfaces/ReaderConfig.md)

#### Returns

`Consumer`

## Methods

### close()

> **close**(): `void`

Defined in: [index.d.ts:420](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L420)

#### Returns

`void`

---

### commitOffsets()

> **commitOffsets**(): `void`

Defined in: [index.d.ts:418](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L418)

#### Returns

`void`

---

### consume()

> **consume**(`consumeConfig`): [`Message`](../interfaces/Message.md)[]

Defined in: [index.d.ts:415](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L415)

#### Parameters

##### consumeConfig

[`ConsumeConfig`](../interfaces/ConsumeConfig.md)

#### Returns

[`Message`](../interfaces/Message.md)[]

---

### position()

> **position**(`partition`): `number`

Defined in: [index.d.ts:417](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L417)

#### Parameters

##### partition

`number`

#### Returns

`number`

---

### seek()

> **seek**(`partition`, `offset`): `void`

Defined in: [index.d.ts:416](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L416)

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

Defined in: [index.d.ts:419](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L419)

#### Returns

[`ConsumerStats`](../interfaces/ConsumerStats.md)
