[**xk6-kafka**](../README.md)

---

# Class: Consumer

Defined in: index.d.ts:411

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

Defined in: index.d.ts:412

#### Parameters

##### readerConfig

[`ReaderConfig`](../interfaces/ReaderConfig.md)

#### Returns

`Consumer`

## Methods

### close()

> **close**(): `void`

Defined in: index.d.ts:418

#### Returns

`void`

---

### commitOffsets()

> **commitOffsets**(): `void`

Defined in: index.d.ts:416

#### Returns

`void`

---

### consume()

> **consume**(`consumeConfig`): [`Message`](../interfaces/Message.md)[]

Defined in: index.d.ts:413

#### Parameters

##### consumeConfig

[`ConsumeConfig`](../interfaces/ConsumeConfig.md)

#### Returns

[`Message`](../interfaces/Message.md)[]

---

### position()

> **position**(`partition`): `number`

Defined in: index.d.ts:415

#### Parameters

##### partition

`number`

#### Returns

`number`

---

### seek()

> **seek**(`partition`, `offset`): `void`

Defined in: index.d.ts:414

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

Defined in: index.d.ts:417

#### Returns

[`ConsumerStats`](../interfaces/ConsumerStats.md)
