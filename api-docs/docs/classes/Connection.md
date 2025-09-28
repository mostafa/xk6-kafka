[**xk6-kafka**](../README.md)

---

# Class: Connection

Defined in: [index.d.ts:406](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L406)

## Classdesc

Connection connects to Kafka for working with topics.

## Example

```javascript
// In init context
const connection = new Connection({
  address: "localhost:9092",
});

// In VU code (default function)
const topics = connection.listTopics();

// In teardown function
connection.close();
```

## Constructors

### Constructor

> **new Connection**(`connectionConfig`): `Connection`

Defined in: [index.d.ts:413](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L413)

#### Parameters

##### connectionConfig

[`ConnectionConfig`](../interfaces/ConnectionConfig.md)

Connection configuration.

#### Returns

`Connection`

- Connection instance.

## Methods

### close()

> **close**(): `void`

Defined in: [index.d.ts:439](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L439)

#### Returns

`void`

- Nothing.

#### Destructor

#### Description

Close the connection.

---

### createTopic()

> **createTopic**(`topicConfig`): `void`

Defined in: [index.d.ts:420](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L420)

#### Parameters

##### topicConfig

[`TopicConfig`](../interfaces/TopicConfig.md)

Topic configuration.

#### Returns

`void`

- Nothing.

#### Method

Create a new topic.

---

### deleteTopic()

> **deleteTopic**(`topic`): `void`

Defined in: [index.d.ts:427](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L427)

#### Parameters

##### topic

`string`

Topic name.

#### Returns

`void`

- Nothing.

#### Method

Delete a topic.

---

### listTopics()

> **listTopics**(): `string`[]

Defined in: [index.d.ts:433](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L433)

#### Returns

`string`[]

- Topics.

#### Method

List topics.
