[**xk6-kafka**](../README.md)

---

# Class: Writer

Defined in: [index.d.ts:323](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L323)

## Classdesc

Writer writes messages to Kafka.

## Example

```javascript
// In init context
const writer = new Writer({
  brokers: ["localhost:9092"],
  topic: "my-topic",
  autoCreateTopic: true,
});

// In VU code (default function)
writer.produce({
  messages: [
    {
      key: "key",
      value: "value",
    },
  ],
});

// In teardown function
writer.close();
```

## Constructors

### Constructor

> **new Writer**(`writerConfig`): `Writer`

Defined in: [index.d.ts:330](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L330)

#### Parameters

##### writerConfig

[`WriterConfig`](../interfaces/WriterConfig.md)

Writer configuration.

#### Returns

`Writer`

- Writer instance.

## Methods

### close()

> **close**(): `void`

Defined in: [index.d.ts:343](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L343)

#### Returns

`void`

- Nothing.

#### Destructor

#### Description

Close the writer.

---

### produce()

> **produce**(`produceConfig`): `void`

Defined in: [index.d.ts:337](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L337)

#### Parameters

##### produceConfig

[`ProduceConfig`](../interfaces/ProduceConfig.md)

Produce configuration.

#### Returns

`void`

- Nothing.

#### Method

Write messages to Kafka.
