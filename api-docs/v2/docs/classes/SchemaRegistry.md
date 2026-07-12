[**xk6-kafka**](../README.md)

---

# Class: SchemaRegistry

Defined in: [index.d.ts:571](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L571)

## Classdesc

Schema Registry is a client for Schema Registry and handles serdes.

## Example

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

## Constructors

### Constructor

> **new SchemaRegistry**(`schemaRegistryConfig`): `SchemaRegistry`

Defined in: [index.d.ts:578](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L578)

#### Parameters

##### schemaRegistryConfig

[`SchemaRegistryConfig`](../interfaces/SchemaRegistryConfig.md)

Schema Registry configuration.

#### Returns

`SchemaRegistry`

- SchemaRegistry instance.

## Methods

### createSchema()

> **createSchema**(`schema`): [`Schema`](../interfaces/Schema.md)

Defined in: [index.d.ts:592](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L592)

#### Parameters

##### schema

[`Schema`](../interfaces/Schema.md)

Schema configuration.

#### Returns

[`Schema`](../interfaces/Schema.md)

- Schema.

#### Method

Create or update a schema on Schema Registry.

---

### deserialize()

> **deserialize**(`container`): `any`

Defined in: [index.d.ts:613](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L613)

#### Parameters

##### container

[`Container`](../interfaces/Container.md)

Container including data, schema and schemaType.

#### Returns

`any`

- Deserialized data as string, byte array or JSON object.

#### Method

Deserializes the given data and schema into its original form.

---

### getSchema()

> **getSchema**(`schema`): [`Schema`](../interfaces/Schema.md)

Defined in: [index.d.ts:585](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L585)

#### Parameters

##### schema

[`Schema`](../interfaces/Schema.md)

Schema configuration.

#### Returns

[`Schema`](../interfaces/Schema.md)

- Schema.

#### Method

Get a schema from Schema Registry by version and subject.

---

### getSubjectName()

> **getSubjectName**(`subjectNameConfig`): `string`

Defined in: [index.d.ts:599](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L599)

#### Parameters

##### subjectNameConfig

[`SubjectNameConfig`](../interfaces/SubjectNameConfig.md)

Subject name configuration.

#### Returns

`string`

- Subject name.

#### Method

Returns the subject name for the given SubjectNameConfig.

---

### serialize()

> **serialize**(`container`): `Uint8Array`

Defined in: [index.d.ts:606](https://github.com/mostafa/xk6-kafka/blob/main/api-docs/v2/index.d.ts#L606)

#### Parameters

##### container

[`Container`](../interfaces/Container.md)

Container including data, schema and schemaType.

#### Returns

`Uint8Array`

- Serialized data as byte array.

#### Method

Serializes the given data and schema into a byte array.
