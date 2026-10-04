[**xk6-kafka**](../README.md)

---

# Class: Consumer

Defined in: [index.d.ts:459](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L459)

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

Defined in: [index.d.ts:460](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L460)

#### Parameters

##### readerConfig

[`ReaderConfig`](../interfaces/ReaderConfig.md)

#### Returns

`Consumer`

## Methods

### close()

> **close**(): `void`

Defined in: [index.d.ts:466](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L466)

#### Returns

`void`

---

### commitOffsets()

> **commitOffsets**(): `void`

Defined in: [index.d.ts:464](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L464)

#### Returns

`void`

---

### consume()

> **consume**(`consumeConfig`): [`Message`](../interfaces/Message.md)[]

Defined in: [index.d.ts:461](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L461)

#### Parameters

##### consumeConfig

[`ConsumeConfig`](../interfaces/ConsumeConfig.md)

#### Returns

[`Message`](../interfaces/Message.md)[]

---

### position()

> **position**(`partition`): `number`

Defined in: [index.d.ts:463](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L463)

#### Parameters

##### partition

`number`

#### Returns

`number`

---

### seek()

> **seek**(`partition`, `offset`): `void`

Defined in: [index.d.ts:462](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L462)

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

Defined in: [index.d.ts:465](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L465)

#### Returns

[`ConsumerStats`](../interfaces/ConsumerStats.md)
