[**xk6-kafka**](../README.md)

---

# ~~Class: Connection~~

Defined in: [index.d.ts:532](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L532)

## Deprecated

Use `AdminClient` instead. `Connection` remains as a compatibility alias in v2.x.

## Constructors

### Constructor

> **new Connection**(`connectionConfig`): `Connection`

Defined in: [index.d.ts:539](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L539)

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

Defined in: [index.d.ts:565](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L565)

#### Returns

`void`

- Nothing.

#### Destructor

#### Description

Close the connection.

---

### ~~createTopic()~~

> **createTopic**(`topicConfig`): `void`

Defined in: [index.d.ts:546](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L546)

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

Defined in: [index.d.ts:553](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L553)

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

Defined in: [index.d.ts:559](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L559)

#### Returns

`string`[]

- Topics.

#### Method

List topics.
