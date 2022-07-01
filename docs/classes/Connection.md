# Class: Connection

**`classdesc`** Connection can connect to Kafka for working with topics.

**`example`**

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

## Table of contents

### Constructors

- [constructor](Connection.md#constructor)

### Methods

- [close](Connection.md#close)
- [createTopic](Connection.md#createtopic)
- [deleteTopic](Connection.md#deletetopic)
- [listTopics](Connection.md#listtopics)

## Constructors

### constructor

• **new Connection**(`connectionConfig`)

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `connectionConfig` | [`ConnectionConfig`](../interfaces/ConnectionConfig.md) | Connection configuration. |

## Methods

### close

▸ **close**(): `void`

**`destructor`**

**`description`** Close the connection.

#### Returns

`void`

- Nothing.

___

### createTopic

▸ **createTopic**(`topicConfig`): `void`

**`method`**
Create a new topic.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `topicConfig` | [`TopicConfig`](../interfaces/TopicConfig.md) | Topic configuration. |

#### Returns

`void`

- Nothing.

___

### deleteTopic

▸ **deleteTopic**(`topic`): `void`

**`method`**
Delete a topic.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `topic` | `string` | Topic name. |

#### Returns

`void`

- Nothing.

___

### listTopics

▸ **listTopics**(): `string`[]

**`method`**
List topics.

#### Returns

`string`[]

- Topics.
