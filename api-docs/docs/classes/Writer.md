# Class: Writer

**`classdesc`** Writer writes messages to Kafka.

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
    },
  ],
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

| Name           | Type                                            | Description           |
| :------------- | :---------------------------------------------- | :-------------------- |
| `writerConfig` | [`WriterConfig`](../interfaces/WriterConfig.md) | Writer configuration. |

#### Defined in

[index.d.ts:317](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L317)

## Methods

### close

▸ **close**(): `void`

**`destructor`**

**`description`** Close the writer.

#### Returns

`void`

- Nothing.

#### Defined in

[index.d.ts:330](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L330)

---

### produce

▸ **produce**(`produceConfig`): `void`

**`method`**
Write messages to Kafka.

#### Parameters

| Name            | Type                                              | Description            |
| :-------------- | :------------------------------------------------ | :--------------------- |
| `produceConfig` | [`ProduceConfig`](../interfaces/ProduceConfig.md) | Produce configuration. |

#### Returns

`void`

- Nothing.

#### Defined in

[index.d.ts:324](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L324)
