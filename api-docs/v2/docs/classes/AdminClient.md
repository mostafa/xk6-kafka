[**xk6-kafka**](../README.md)

---

# Class: AdminClient

Defined in: [index.d.ts:469](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L469)

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

Defined in: [index.d.ts:470](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L470)

#### Parameters

##### connectionConfig

[`ConnectionConfig`](../interfaces/ConnectionConfig.md)

#### Returns

`AdminClient`

## Methods

### close()

> **close**(): `void`

Defined in: [index.d.ts:475](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L475)

#### Returns

`void`

---

### createTopic()

> **createTopic**(`topicConfig`): `void`

Defined in: [index.d.ts:471](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L471)

#### Parameters

##### topicConfig

[`TopicConfig`](../interfaces/TopicConfig.md)

#### Returns

`void`

---

### deleteTopic()

> **deleteTopic**(`topic`): `void`

Defined in: [index.d.ts:472](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L472)

#### Parameters

##### topic

`string`

#### Returns

`void`

---

### getMetadata()

> **getMetadata**(`topic`): [`TopicMetadata`](../interfaces/TopicMetadata.md)

Defined in: [index.d.ts:474](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L474)

#### Parameters

##### topic

`string`

#### Returns

[`TopicMetadata`](../interfaces/TopicMetadata.md)

---

### listTopics()

> **listTopics**(): [`TopicInfo`](../interfaces/TopicInfo.md)[]

Defined in: [index.d.ts:473](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L473)

#### Returns

[`TopicInfo`](../interfaces/TopicInfo.md)[]
