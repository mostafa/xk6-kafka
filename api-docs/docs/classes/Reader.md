# Class: Reader

**`classdesc`** Reader reads messages from Kafka.

**`example`**

```javascript
// In init context
const reader = new Reader({
  brokers: ["localhost:9092"],
  topic: "my-topic",
});

// In VU code (default function)
const messages = reader.consume({ limit: 10, nanoPrecision: false });

// In teardown function
reader.close();
```

## Table of contents

### Constructors

- [constructor](Reader.md#constructor)

### Methods

- [close](Reader.md#close)
- [consume](Reader.md#consume)

## Constructors

### constructor

• **new Reader**(`readerConfig`)

#### Parameters

| Name           | Type                                            | Description           |
| :------------- | :---------------------------------------------- | :-------------------- |
| `readerConfig` | [`ReaderConfig`](../interfaces/ReaderConfig.md) | Reader configuration. |

#### Defined in

[index.d.ts:367](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L367)

## Methods

### close

▸ **close**(): `void`

**`destructor`**

**`description`** Close the reader.

#### Returns

`void`

- Nothing.

#### Defined in

[index.d.ts:380](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L380)

---

### consume

▸ **consume**(`consumeConfig`): [`Message`](../interfaces/Message.md)[]

**`method`**
Read messages from Kafka.

#### Parameters

| Name            | Type                                              | Description            |
| :-------------- | :------------------------------------------------ | :--------------------- |
| `consumeConfig` | [`ConsumeConfig`](../interfaces/ConsumeConfig.md) | Consume configuration. |

#### Returns

[`Message`](../interfaces/Message.md)[]

- Messages.

#### Defined in

[index.d.ts:374](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L374)
