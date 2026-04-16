[**xk6-kafka**](../README.md)

---

# ~~Class: Connection~~

Defined in: [index.d.ts:479](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L479)

## Deprecated

Use `AdminClient` instead. `Connection` remains as a compatibility alias in v2.x.

## Constructors

### Constructor

> **new Connection**(`connectionConfig`): `Connection`

Defined in: [index.d.ts:486](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L486)

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

Defined in: [index.d.ts:512](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L512)

#### Returns

`void`

- Nothing.

#### Destructor

#### Description

Close the connection.

---

### ~~createTopic()~~

> **createTopic**(`topicConfig`): `void`

Defined in: [index.d.ts:493](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L493)

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

Defined in: [index.d.ts:500](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L500)

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

Defined in: [index.d.ts:506](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L506)

#### Returns

`string`[]

- Topics.

#### Method

List topics.
