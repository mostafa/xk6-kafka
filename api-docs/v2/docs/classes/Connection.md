[**xk6-kafka**](../README.md)

---

# ~~Class: Connection~~

Defined in: [index.d.ts:477](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L477)

## Deprecated

Use `AdminClient` instead. `Connection` remains as a compatibility alias in v2.x.

## Constructors

### Constructor

> **new Connection**(`connectionConfig`): `Connection`

Defined in: [index.d.ts:484](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L484)

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

Defined in: [index.d.ts:510](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L510)

#### Returns

`void`

- Nothing.

#### Destructor

#### Description

Close the connection.

---

### ~~createTopic()~~

> **createTopic**(`topicConfig`): `void`

Defined in: [index.d.ts:491](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L491)

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

Defined in: [index.d.ts:498](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L498)

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

Defined in: [index.d.ts:504](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L504)

#### Returns

`string`[]

- Topics.

#### Method

List topics.
