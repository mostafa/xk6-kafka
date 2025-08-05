import {check} from "k6";
import {
    Writer,
    Reader,
    Connection,
    SchemaRegistry,
    KEY,
    VALUE,
    TOPIC_NAME_STRATEGY,
    RECORD_NAME_STRATEGY,
    SCHEMA_TYPE_AVRO,
} from "k6/x/kafka"; // import kafka extension

const brokers = ["localhost:9092"];
const topic = "com.example.person";

// Kafka writer and reader
const writer = new Writer({
    brokers,
    topic,
    autoCreateTopic: true,
});

const reader = new Reader({
    brokers,
    topic,
});

const connection = new Connection({
    address: brokers[0],
});

// Schema registry with authentication (basic auth)
const schemaRegistry = new SchemaRegistry({
    url: "http://localhost:8081",
    username: "your-username", // 🔐 Replace with your actual username
    password: "your-password", // 🔐 Replace with your actual password
});

// Only VU 0 creates the topic
if (__VU == 0) {
    connection.createTopic({topic});
}

// Define schemas
const keySchema = `{
  "name": "KeySchema",
  "type": "record",
  "namespace": "com.example.key",
  "fields": [
    { "name": "ssn", "type": "string" }
  ]
}`;

const valueSchema = `{
  "name": "ValueSchema",
  "type": "record",
  "namespace": "com.example.value",
  "fields": [
    { "name": "firstName", "type": "string" },
    { "name": "lastName", "type": "string" }
  ]
}`;

// Get subject names using strategies
const keySubjectName = schemaRegistry.getSubjectName({
    topic,
    element: KEY,
    subjectNameStrategy: TOPIC_NAME_STRATEGY,
    schema: keySchema,
});

const valueSubjectName = schemaRegistry.getSubjectName({
    topic,
    element: VALUE,
    subjectNameStrategy: RECORD_NAME_STRATEGY,
    schema: valueSchema,
});

// Fetch schema objects from registry (must be pre-registered!)
const keySchemaObject = schemaRegistry.getSchema({subject: keySubjectName});
const valueSchemaObject = schemaRegistry.getSchema({subject: valueSubjectName});

export default function () {
    for (let index = 0; index < 100; index++) {
        const messages = [
            {
                key: schemaRegistry.serialize({
                    data: {ssn: "ssn-" + index},
                    schema: keySchemaObject,
                    schemaType: SCHEMA_TYPE_AVRO,
                }),
                value: schemaRegistry.serialize({
                    data: {
                        firstName: "firstName-" + index,
                        lastName: "lastName-" + index,
                    },
                    schema: valueSchemaObject,
                    schemaType: SCHEMA_TYPE_AVRO,
                }),
            },
        ];
        writer.produce({messages});
    }

    const messages = reader.consume({limit: 20});

    check(messages, {
        "20 messages returned": (msgs) => msgs.length === 20,
        "key starts with 'ssn-'": (msgs) =>
            schemaRegistry.deserialize({
                data: msgs[0].key,
                schema: keySchemaObject,
                schemaType: SCHEMA_TYPE_AVRO,
            }).ssn.startsWith("ssn-"),
        "value contains correct fields": (msgs) => {
            const value = schemaRegistry.deserialize({
                data: msgs[0].value,
                schema: valueSchemaObject,
                schemaType: SCHEMA_TYPE_AVRO,
            });
            return value.firstName.startsWith("firstName-") && value.lastName.startsWith("lastName-");
        },
    });
}

export function teardown() {
    if (__VU == 0) {
        connection.deleteTopic(topic);
    }
    writer.close();
    reader.close();
    connection.close();
}
