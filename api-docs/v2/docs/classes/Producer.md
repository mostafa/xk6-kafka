[**xk6-kafka**](../README.md)

---

# Class: Producer

Defined in: index.d.ts:358

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

Defined in: index.d.ts:359

#### Parameters

##### writerConfig

[`WriterConfig`](../interfaces/WriterConfig.md)

#### Returns

`Producer`

## Methods

### close()

> **close**(): `void`

Defined in: index.d.ts:363

#### Returns

`void`

---

### flush()

> **flush**(): `void`

Defined in: index.d.ts:361

#### Returns

`void`

---

### produce()

> **produce**(`produceConfig`): `void`

Defined in: index.d.ts:360

#### Parameters

##### produceConfig

[`ProduceConfig`](../interfaces/ProduceConfig.md)

#### Returns

`void`

---

### stats()

> **stats**(): [`ProducerStats`](../interfaces/ProducerStats.md)

Defined in: index.d.ts:362

#### Returns

[`ProducerStats`](../interfaces/ProducerStats.md)
