[**xk6-kafka**](../README.md)

---

# ~~Class: Connection~~

Defined in: [index.d.ts:495](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L495)

## Deprecated

Use `AdminClient` instead. `Connection` remains as a compatibility alias in v2.x.

## Constructors

### Constructor

> **new Connection**(`connectionConfig`): `Connection`

Defined in: [index.d.ts:502](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L502)

#### Parameters

##### connectionConfig

[`ConnectionConfig`](../interfaces/ConnectionConfig.md)

Connection configuration.

#### Returns

`Connection`

- Connection instance.

## Methods

### ~~close()~~

> **close**(): `void`

Defined in: [index.d.ts:528](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L528)

#### Returns

`void`

- Nothing.

#### Destructor

#### Description

Close the connection.

---

### ~~createTopic()~~

> **createTopic**(`topicConfig`): `void`

Defined in: [index.d.ts:509](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L509)

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

### ~~deleteTopic()~~

> **deleteTopic**(`topic`): `void`

Defined in: [index.d.ts:516](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L516)

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

### ~~listTopics()~~

> **listTopics**(): `string`[]

Defined in: [index.d.ts:522](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L522)

#### Returns

`string`[]

- Topics.

#### Method

List topics.
