# Class: Writer

**`classdesc`** Writer can write messages to Kafka.

**`example`**

```javascript
// In init context
const writer = new Writer({
  brokers: ["localhost:9092"],
  topic: "my-topic",
  autoCreateTopic: true,
});

// In VU code (default function)
writer.produce({
  messages: [
    {
      key: "key",
      value: "value",
    }
  ]
});

// In teardown function
writer.close();
```

## Table of contents

### Constructors

- [constructor](Writer.md#constructor)

### Methods

- [close](Writer.md#close)
- [produce](Writer.md#produce)

## Constructors

### constructor

• **new Writer**(`writerConfig`)

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `writerConfig` | [`WriterConfig`](../interfaces/WriterConfig.md) | Writer configuration. |

## Methods

### close

▸ **close**(): `void`

**`destructor`**

**`description`** Close the writer.

#### Returns

`void`

- Nothing.

___

### produce

▸ **produce**(`produceConfig`): `void`

**`method`**
Write messages to Kafka.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `produceConfig` | [`ProduceConfig`](../interfaces/ProduceConfig.md) | Produce configuration. |

#### Returns

`void`

- Nothing.
