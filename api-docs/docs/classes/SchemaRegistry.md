# Class: SchemaRegistry

**`classdesc`** Schema Registry is a client for Schema Registry and handles serdes.

**`example`**

```javascript
// In init context
const writer = new Writer({
  brokers: ["localhost:9092"],
  topic: "my-topic",
  autoCreateTopic: true,
});

const schemaRegistry = new SchemaRegistry({
  url: "localhost:8081",
});

const keySchema = schemaRegistry.createSchema({
  version: 0,
  element: KEY,
  subject: "...",
  schema: "...",
  schemaType: "AVRO",
});

const valueSchema = schemaRegistry.createSchema({
  version: 0,
  element: VALUE,
  subject: "...",
  schema: "...",
  schemaType: "AVRO",
});

// In VU code (default function)
writer.produce({
  messages: [
    {
      key: schemaRegistry.serialize({
        data: "key",
        schema: keySchema,
        schemaType: SCHEMA_TYPE_AVRO,
      }),
      value: schemaRegistry.serialize({
        data: "value",
        schema: valueSchema,
        schemaType: SCHEMA_TYPE_AVRO,
      }),
    },
  ],
});
```

## Table of contents

### Constructors

- [constructor](SchemaRegistry.md#constructor)

### Methods

- [createSchema](SchemaRegistry.md#createschema)
- [deserialize](SchemaRegistry.md#deserialize)
- [getSchema](SchemaRegistry.md#getschema)
- [getSubjectName](SchemaRegistry.md#getsubjectname)
- [serialize](SchemaRegistry.md#serialize)

## Constructors

### constructor

• **new SchemaRegistry**(`schemaRegistryConfig`)

#### Parameters

| Name                   | Type                                                            | Description                    |
| :--------------------- | :-------------------------------------------------------------- | :----------------------------- |
| `schemaRegistryConfig` | [`SchemaRegistryConfig`](../interfaces/SchemaRegistryConfig.md) | Schema Registry configuration. |

#### Defined in

[index.d.ts:496](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L496)

## Methods

### createSchema

▸ **createSchema**(`schema`): [`Schema`](../interfaces/Schema.md)

**`method`**
Create or update a schema on Schema Registry.

#### Parameters

| Name     | Type                                | Description           |
| :------- | :---------------------------------- | :-------------------- |
| `schema` | [`Schema`](../interfaces/Schema.md) | Schema configuration. |

#### Returns

[`Schema`](../interfaces/Schema.md)

- Schema.

#### Defined in

[index.d.ts:510](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L510)

---

### deserialize

▸ **deserialize**(`container`): `any`

**`method`**
Deserializes the given data and schema into its original form.

#### Parameters

| Name        | Type                                      | Description                                      |
| :---------- | :---------------------------------------- | :----------------------------------------------- |
| `container` | [`Container`](../interfaces/Container.md) | Container including data, schema and schemaType. |

#### Returns

`any`

- Deserialized data as string, byte array or JSON object.

#### Defined in

[index.d.ts:531](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L531)

---

### getSchema

▸ **getSchema**(`schema`): [`Schema`](../interfaces/Schema.md)

**`method`**
Get a schema from Schema Registry by version and subject.

#### Parameters

| Name     | Type                                | Description           |
| :------- | :---------------------------------- | :-------------------- |
| `schema` | [`Schema`](../interfaces/Schema.md) | Schema configuration. |

#### Returns

[`Schema`](../interfaces/Schema.md)

- Schema.

#### Defined in

[index.d.ts:503](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L503)

---

### getSubjectName

▸ **getSubjectName**(`subjectNameConfig`): `string`

**`method`**
Returns the subject name for the given SubjectNameConfig.

#### Parameters

| Name                | Type                                                      | Description                 |
| :------------------ | :-------------------------------------------------------- | :-------------------------- |
| `subjectNameConfig` | [`SubjectNameConfig`](../interfaces/SubjectNameConfig.md) | Subject name configuration. |

#### Returns

`string`

- Subject name.

#### Defined in

[index.d.ts:517](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L517)

---

### serialize

▸ **serialize**(`container`): `Uint8Array`

**`method`**
Serializes the given data and schema into a byte array.

#### Parameters

| Name        | Type                                      | Description                                      |
| :---------- | :---------------------------------------- | :----------------------------------------------- |
| `container` | [`Container`](../interfaces/Container.md) | Container including data, schema and schemaType. |

#### Returns

`Uint8Array`

- Serialized data as byte array.

#### Defined in

[index.d.ts:524](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/index.d.ts#L524)
