[**xk6-kafka**](../README.md)

---

# Class: Producer

Defined in: [index.d.ts:406](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L406)

## Classdesc

Producer writes messages to Kafka.

## Example

```javascript
// In init context
const producer = new Producer({
  brokers: ["localhost:9092"],
  topic: "my-topic",
  autoCreateTopic: true,
});

// In VU code (default function)
producer.produce({
  messages: [
    {
      key: "key",
      value: "value",
    },
  ],
});

// In teardown function
producer.close();
```

## Constructors

### Constructor

> **new Producer**(`writerConfig`): `Producer`

Defined in: [index.d.ts:407](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L407)

#### Parameters

##### writerConfig

[`WriterConfig`](../interfaces/WriterConfig.md)

#### Returns

`Producer`

## Methods

### close()

> **close**(): `void`

Defined in: [index.d.ts:411](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L411)

#### Returns

`void`

---

### flush()

> **flush**(): `void`

Defined in: [index.d.ts:409](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L409)

#### Returns

`void`

---

### produce()

> **produce**(`produceConfig`): `void`

Defined in: [index.d.ts:408](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L408)

#### Parameters

##### produceConfig

[`ProduceConfig`](../interfaces/ProduceConfig.md)

#### Returns

`void`

---

### stats()

> **stats**(): [`ProducerStats`](../interfaces/ProducerStats.md)

Defined in: [index.d.ts:410](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L410)

#### Returns

[`ProducerStats`](../interfaces/ProducerStats.md)
