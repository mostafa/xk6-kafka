[**xk6-kafka**](../README.md)

---

# Class: AdminClient

Defined in: [index.d.ts:497](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L497)

## Classdesc

AdminClient connects to Kafka for topic administration.

## Example

```javascript
// In init context
const adminClient = new AdminClient({
  brokers: ["localhost:9092"],
});

// In VU code (default function)
const topics = adminClient.listTopics();

// In teardown function
adminClient.close();
```

## Constructors

### Constructor

> **new AdminClient**(`connectionConfig`): `AdminClient`

Defined in: [index.d.ts:498](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L498)

#### Parameters

##### connectionConfig

[`ConnectionConfig`](../interfaces/ConnectionConfig.md)

#### Returns

`AdminClient`

## Methods

### close()

> **close**(): `void`

Defined in: [index.d.ts:510](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L510)

#### Returns

`void`

---

### createTopic()

> **createTopic**(`topicConfig`): `void`

Defined in: [index.d.ts:499](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L499)

#### Parameters

##### topicConfig

[`TopicConfig`](../interfaces/TopicConfig.md)

#### Returns

`void`

---

### deleteTopic()

> **deleteTopic**(`topic`): `void`

Defined in: [index.d.ts:500](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L500)

#### Parameters

##### topic

`string`

#### Returns

`void`

---

### getMetadata()

> **getMetadata**(`topic`): [`TopicMetadata`](../interfaces/TopicMetadata.md)

Defined in: [index.d.ts:502](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L502)

#### Parameters

##### topic

`string`

#### Returns

[`TopicMetadata`](../interfaces/TopicMetadata.md)

---

### initializeConsumerGroupOffsets()

> **initializeConsumerGroupOffsets**(`config`): [`ConsumerGroupOffset`](../interfaces/ConsumerGroupOffset.md)[]

Defined in: [index.d.ts:507](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L507)

Reset an inactive consumer group to a snapshot of the current end offset
of every partition in the supplied topics.

#### Parameters

##### config

[`ConsumerGroupOffsetsConfig`](../interfaces/ConsumerGroupOffsetsConfig.md)

#### Returns

[`ConsumerGroupOffset`](../interfaces/ConsumerGroupOffset.md)[]

---

### listTopics()

> **listTopics**(): [`TopicInfo`](../interfaces/TopicInfo.md)[]

Defined in: [index.d.ts:501](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L501)

#### Returns

[`TopicInfo`](../interfaces/TopicInfo.md)[]
