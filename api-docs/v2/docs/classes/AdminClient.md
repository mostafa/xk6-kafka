[**xk6-kafka**](../README.md)

---

# Class: AdminClient

Defined in: [index.d.ts:465](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L465)

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

Defined in: [index.d.ts:466](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L466)

#### Parameters

##### connectionConfig

[`ConnectionConfig`](../interfaces/ConnectionConfig.md)

#### Returns

`AdminClient`

## Methods

### close()

> **close**(): `void`

Defined in: [index.d.ts:471](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L471)

#### Returns

`void`

---

### createTopic()

> **createTopic**(`topicConfig`): `void`

Defined in: [index.d.ts:467](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L467)

#### Parameters

##### topicConfig

[`TopicConfig`](../interfaces/TopicConfig.md)

#### Returns

`void`

---

### deleteTopic()

> **deleteTopic**(`topic`): `void`

Defined in: [index.d.ts:468](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L468)

#### Parameters

##### topic

`string`

#### Returns

`void`

---

### getMetadata()

> **getMetadata**(`topic`): [`TopicMetadata`](../interfaces/TopicMetadata.md)

Defined in: [index.d.ts:470](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L470)

#### Parameters

##### topic

`string`

#### Returns

[`TopicMetadata`](../interfaces/TopicMetadata.md)

---

### listTopics()

> **listTopics**(): [`TopicInfo`](../interfaces/TopicInfo.md)[]

Defined in: [index.d.ts:469](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L469)

#### Returns

[`TopicInfo`](../interfaces/TopicInfo.md)[]
