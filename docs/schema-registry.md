# Schema Registry usage with xk6-kafka

## What is Schema Registry?

Schema Registry is a service that provides a centralized repository for managing and validating schemas used in data serialization formats like Avro, JSON, and Protobuf.
It ensures that the data produced and consumed by Kafka producers and consumers adheres to a defined schema, enabling compatibility and evolution of data structures over time.
It is highly recommended to use Schema Registry when working with Kafka, in order to ensure that your messages conform to the expected structure and types.

## How to Use Schema Registry with xk6-kafka

### Create a Schema Registry Client

To use Schema Registry with xk6-kafka,
you need to create a Schema Registry client that will handle the communication with the Schema Registry service.

#### No authentication

```javascript
const schemaRegistry = new SchemaRegistry({
  url: "http://localhost:8081",
});
```

#### Basic authentication

```javascript
const schemaRegistry = new SchemaRegistry({
  url: "http://localhost:8081",
  basicAuth: {
    username: "my-username",
    password: "my-password",
  },
});
```

### Create a new Schema into your Schema Registry

If you have not already created a schema in your Schema Registry, you can do so using the following steps
to let your load test create it for you.

```javascript
const keySchema = `{
  "name": "KeySchema",
  "type": "record",
  "namespace": "com.example.key",
  "fields": [
    {
      "name": "ssn",
      "type": "string"
    }
  ]
}
`;
const valueSchema = `{
  "name": "ValueSchema",
  "type": "record",
  "namespace": "com.example.value",
  "fields": [
    {
      "name": "firstName",
      "type": "string"
    },
    {
      "name": "lastName",
      "type": "string"
    }
  ]
}`;

const keySchemaObject = schemaRegistry.createSchema({
  subject: "my-key-schema-name",
  schema: keySchema,
  schemaType: SCHEMA_TYPE_AVRO,
});

const valueSchemaObject = schemaRegistry.createSchema({
  subject: "my-value-schema-name",
  schema: valueSchema,
  schemaType: SCHEMA_TYPE_AVRO,
});
```

Optional : Retrieve the subject name directly from the schema:

```javascript
const keySubjectName = schemaRegistry.getSubjectName({
  topic: topic,
  element: KEY,
  subjectNameStrategy: TOPIC_NAME_STRATEGY, // or RECORD_NAME_STRATEGY depending on your needs
  schema: keySchema,
});

const valueSubjectName = schemaRegistry.getSubjectName({
  topic: topic,
  element: VALUE,
  subjectNameStrategy: RECORD_NAME_STRATEGY, // or TOPIC_NAME_STRATEGY depending on your needs
  schema: valueSchema,
});
```

You can then use these schemas in your load test to produce and consume messages.

```javascript
const messages = [
  {
    key: schemaRegistry.serialize({
      data: {
        ssn: "123-45-6789",
      },
      schema: keySchemaObject,
      schemaType: SCHEMA_TYPE_AVRO,
    }),
    value: schemaRegistry.serialize({
      data: {
        firstName: "John",
        lastName: "Doe",
      },
      schema: valueSchemaObject,
      schemaType: SCHEMA_TYPE_AVRO,
    }),
    headers: {
      mykey: "myvalue",
    },
    offset: 0,
    partition: 0,
    time: new Date(),
  },
];
```

### Load an existing Schema from your Schema Registry

If you already have a schema registered in your Schema Registry, you can load it using the following method:

```javascript
const keySchemaObject = schemaRegistry.getSchema({
  subject: "my-key-schema-name",
});

const valueSchemaObject = schemaRegistry.getSchema({
  subject: "my-value-schema-name",
});

const messages = [
  {
    key: schemaRegistry.serialize({
      data: {
        ssn: "123-45-6789",
      },
      schema: keySchemaObject,
      schemaType: SCHEMA_TYPE_AVRO,
    }),
    value: schemaRegistry.serialize({
      data: {
        firstName: "John",
        lastName: "Doe",
      },
      schema: valueSchemaObject,
      schemaType: SCHEMA_TYPE_AVRO,
    }),
    headers: {
      mykey: "myvalue",
    },
    offset: 0,
    partition: 0,
    time: new Date(),
  },
];
```

### Complex schemas : Manage union types

When dealing with complex schemas, especially those involving union types, you'll have to ensure that the data you serialize matches the expected schema structure.
Union types in Avro allow a field to hold values of different types, which can complicate serialization and deserialization.
For example, if you have a union type schema like this:

```json
{
  "name": "MyUnionSchema",
  "type": "record",
  "fields": [
    {
      "name": "myField",
      "type": ["null", "string"]
    }
  ]
}
```

Here `myField` can either be a string or null. When serializing data for this schema, you need to ensure that the value you provide matches one of the types in the union.
Your object must respect the goavro serialization rules, which means you need to provide the correct type for `myField`:

```javascript
export const productionOrder = {
  myField: { string: "My awesome value" },
};
```

Here you have to define the type explicitly as `string` to match the schema.
**Be careful**, in your schema, you'll have to define the _null_ value first in the union type, otherwise the serialization will fail.

In order to help you with complex schemas, you can use the [nested-avro-schema](https://github.com/mostafa/nested-avro-schema) project, which provides a way to define complex Avro schemas in a more structured way.
