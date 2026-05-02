[**xk6-kafka**](../README.md)

---

# ~~Class: Connection~~

Defined in: [index.d.ts:482](https://github.com/tnewman/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L482)

## Deprecated

Use `AdminClient` instead. `Connection` remains as a compatibility alias in v2.x.

## Constructors

### Constructor

> **new Connection**(`connectionConfig`): `Connection`

Defined in: [index.d.ts:489](https://github.com/tnewman/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L489)

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

Defined in: [index.d.ts:515](https://github.com/tnewman/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L515)

#### Returns

`void`

- Nothing.

#### Destructor

#### Description

Close the connection.

---

### ~~createTopic()~~

> **createTopic**(`topicConfig`): `void`

Defined in: [index.d.ts:496](https://github.com/tnewman/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L496)

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

Defined in: [index.d.ts:503](https://github.com/tnewman/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L503)

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

Defined in: [index.d.ts:509](https://github.com/tnewman/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L509)

#### Returns

`string`[]

- Topics.

#### Method

List topics.
