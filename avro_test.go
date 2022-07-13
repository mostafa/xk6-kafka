package kafka

import (
	"testing"

	"github.com/riferrei/srclient"
	"github.com/stretchr/testify/assert"
)

var (
	avroConfig Configuration = Configuration{
		Producer: ProducerConfiguration{
			ValueSerializer: AvroSerializer,
			KeySerializer:   AvroSerializer,
		},
		Consumer: ConsumerConfiguration{
			ValueDeserializer: AvroDeserializer,
			KeyDeserializer:   AvroDeserializer,
		},
	}
	avroSchemaForAvroTests string = `{
		"type":"record",
		"name":"Schema",
		"namespace":"io.confluent.kafka.avro",
		"fields":[{"name":"field","type":"string"}]}`
	data string = `{"field":"value"}`
)

// TestSerializeDeserializeAvro tests serialization and deserialization of Avro messages.
func TestSerializeDeserializeAvro(t *testing.T) {
	// Test with a schema registry, which fails and manually (de)serializes the data.
	for _, element := range []Element{Key, Value} {
		// Serialize the key or value
		serialized, err := SerializeAvro(avroConfig, "topic", `{"field":"value"}`, element, avroSchemaForAvroTests, 0)
		assert.Nil(t, err)
		assert.NotNil(t, serialized)
		// 4 bytes for magic byte, 1 byte for schema ID, and the rest is the data.
		assert.GreaterOrEqual(t, len(serialized), 10)

		// Deserialize the key or value (removes the magic bytes).
		deserialized, err := DeserializeAvro(avroConfig, "", serialized, element, avroSchemaForAvroTests, 0)
		assert.Nil(t, err)
		assert.Equal(t, map[string]interface{}{"field": "value"}, deserialized)
	}
}

// TestSerializeDeserializeAvroFailsOnSchemaError tests serialization and
// deserialization of Avro messages and fails on schema error.
func TestSerializeDeserializeAvroFailsOnSchemaError(t *testing.T) {
	jsonSchema = `{}`

	for _, element := range []Element{Key, Value} {
		// Serialize the key or value.
		serialized, err := SerializeAvro(avroConfig, "topic", `{"field":"value"}`, element, jsonSchema, 0)
		assert.Nil(t, serialized)
		assert.Error(t, err.Unwrap())
		assert.Equal(t, "Failed to create codec for encoding Avro", err.Message)
		assert.Equal(t, failedCreateAvroCodec, err.Code)

		// Deserialize the key or value.
		deserialized, err := DeserializeAvro(avroConfig, "topic", []byte{0, 1, 2, 3, 4, 5}, element, jsonSchema, 0)
		assert.Nil(t, deserialized)
		assert.Error(t, err.Unwrap())
		assert.Equal(t, "Failed to create codec for decoding Avro", err.Message)
		assert.Equal(t, failedCreateAvroCodec, err.Code)
	}
}

// TestSerializeDeserializeAvroFailsOnWireFormatError tests serialization and
// deserialization of Avro messages and fails on wire format error.
func TestSerializeDeserializeAvroFailsOnWireFormatError(t *testing.T) {
	schema := `{}`

	for _, element := range []Element{Key, Value} {
		// Deserialize an empty key or value.
		deserialized, err := DeserializeAvro(avroConfig, "topic", []byte{}, element, schema, 0)
		assert.Nil(t, deserialized)
		assert.Error(t, err.Unwrap())
		assert.Equal(t, "Failed to remove wire format from the binary data", err.Message)
		assert.Equal(t, failedDecodeFromWireFormat, err.Code)

		// Deserialize a broken key or value.
		// Proper wire-formatted message has 5 bytes (the wire format) plus data.
		deserialized, err = DeserializeAvro(avroConfig, "topic", []byte{0, 1, 2, 3}, element, schema, 0)
		assert.Nil(t, deserialized)
		assert.Error(t, err.Unwrap())
		assert.Equal(t, "Failed to remove wire format from the binary data", err.Message)
		assert.Equal(t, failedDecodeFromWireFormat, err.Code)
	}
}

// TestSerializeDeserializeAvroFailsOnEncodeDecodeError tests serialization and
// deserialization of Avro messages and fails on encode/decode error.
func TestSerializeDeserializeAvroFailsOnEncodeDecodeError(t *testing.T) {
	data := `{"nonExistingField":"value"}`

	for _, element := range []Element{Key, Value} {
		serialized, err := SerializeAvro(avroConfig, "topic", data, element, avroSchemaForAvroTests, 0)
		assert.Nil(t, serialized)
		assert.Error(t, err.Unwrap())
		assert.Equal(t, "Failed to encode data into Avro", err.Message)
		assert.Equal(t, failedEncodeToAvro, err.Code)

		deserialized, err := DeserializeAvro(avroConfig, "topic", []byte{0, 1, 2, 3, 5}, element, avroSchemaForAvroTests, 0)
		assert.Nil(t, deserialized)
		assert.Error(t, err.Unwrap())
		assert.Equal(t, "Failed to decode data from Avro", err.Message)
		assert.Equal(t, failedDecodeAvroFromBinary, err.Code)
	}
}

// TestAvroSerializeTopicNameStrategy tests serialization of Avro messages with the given topic name strategy.
func TestAvroSerializeTopicNameStrategy(t *testing.T) {
	topic := "TestAvroSerializeTopicNameStrategy-topic"
	config := Configuration{
		Producer: ProducerConfiguration{
			ValueSerializer:     AvroSerializer,
			SubjectNameStrategy: TopicNameStrategy,
		},
		SchemaRegistry: SchemaRegistryConfiguration{
			URL: "http://localhost:8081",
		},
	}

	schema := `{
		"type":"record",
		"name":"TestAvroSerializeTopicNameStrategyIsDefaultStrategy",
		"namespace":"io.confluent.kafka.avro",
		"fields":[{"name":"field","type":"string"}]}`

	serialized, err := SerializeAvro(config, topic, data, Value, schema, 0)
	assert.Nil(t, err)
	assert.NotNil(t, serialized)

	expectedSubject := topic + "-value"
	srClient := SchemaRegistryClientWithConfiguration(config.SchemaRegistry)
	schemaResult, err := GetSchema(srClient, expectedSubject, schema, srclient.Avro, 0)
	assert.Nil(t, err)
	assert.NotNil(t, schemaResult)
}

// TestAvroSerializeTopicNameStrategyIsDefaultStrategy tests serialization of
// Avro messages with the default topic name strategy.
func TestAvroSerializeTopicNameStrategyIsDefaultStrategy(t *testing.T) {
	topic := "TestAvroSerializeTopicNameStrategyIsDefaultStrategy-topic"
	config := Configuration{
		Producer: ProducerConfiguration{
			ValueSerializer: AvroSerializer,
		},
		SchemaRegistry: SchemaRegistryConfiguration{
			URL: "http://localhost:8081",
		},
	}

	schema := `{
		"type":"record",
		"name":"TestAvroSerializeTopicNameStrategyIsDefaultStrategy",
		"namespace":"io.confluent.kafka.avro",
		"fields":[{"name":"field","type":"string"}]}`

	serialized, err := SerializeAvro(config, topic, data, Value, schema, 0)
	assert.Nil(t, err)
	assert.NotNil(t, serialized)

	expectedSubject := topic + "-value"
	srClient := SchemaRegistryClientWithConfiguration(config.SchemaRegistry)
	schemaResult, err := GetSchema(srClient, expectedSubject, schema, srclient.Avro, 0)
	assert.Nil(t, err)
	assert.NotNil(t, schemaResult)
}

// TestAvroSerializeTopicRecordNameStrategy tests serialization of Avro messages
// with the given topic record name strategy.
func TestAvroSerializeTopicRecordNameStrategy(t *testing.T) {
	topic := "TestAvroSerializeTopicRecordNameStrategy-topic"
	config := Configuration{
		Producer: ProducerConfiguration{
			ValueSerializer:     AvroSerializer,
			SubjectNameStrategy: TopicRecordNameStrategy,
		},
		SchemaRegistry: SchemaRegistryConfiguration{
			URL: "http://localhost:8081",
		},
	}
	schema := `{
		"type":"record",
		"name":"TestAvroSerializeTopicRecordNameStrategy",
		"namespace":"io.confluent.kafka.avro",
		"fields":[{"name":"field","type":"string"}]}`

	serialized, err := SerializeAvro(config, topic, data, Value, schema, 0)
	assert.Nil(t, err)
	assert.NotNil(t, serialized)

	expectedSubject := topic + "-io.confluent.kafka.avro.TestAvroSerializeTopicRecordNameStrategy"
	srClient := SchemaRegistryClientWithConfiguration(config.SchemaRegistry)
	schemaResult, err := GetSchema(srClient, expectedSubject, schema, srclient.Avro, 0)
	assert.Nil(t, err)
	assert.NotNil(t, schemaResult)
}

// TestAvroSerializeRecordNameStrategy tests serialization of Avro messages
// with the given record name strategy.
func TestAvroSerializeRecordNameStrategy(t *testing.T) {
	topic := "TestAvroSerializeRecordNameStrategy-topic"
	config := Configuration{
		Producer: ProducerConfiguration{
			ValueSerializer:     AvroSerializer,
			SubjectNameStrategy: RecordNameStrategy,
		},
		SchemaRegistry: SchemaRegistryConfiguration{
			URL: "http://localhost:8081",
		},
	}
	schema := `{
		"type":"record",
		"name":"TestAvroSerializeRecordNameStrategy",
		"namespace":"io.confluent.kafka.avro",
		"fields":[{"name":"field","type":"string"}]}`

	serialized, err := SerializeAvro(config, topic, data, Value, schema, 0)
	assert.Nil(t, err)
	assert.NotNil(t, serialized)

	expectedSubject := "io.confluent.kafka.avro.TestAvroSerializeRecordNameStrategy"
	srClient := SchemaRegistryClientWithConfiguration(config.SchemaRegistry)
	resultSchema, err := GetSchema(srClient, expectedSubject, avroSchemaForAvroTests, srclient.Avro, 0)
	assert.Nil(t, err)
	assert.NotNil(t, resultSchema)
}

// TestAvroDeserializeUsingMagicPrefix tests deserialization of Avro messages
// with the given magic prefix.
func TestAvroDeserializeUsingMagicPrefix(t *testing.T) {
	topic := "TestAvroDeserializeUsingMagicPrefix-topic"
	config := Configuration{
		Consumer: ConsumerConfiguration{
			UseMagicPrefix: true,
		},
		Producer: ProducerConfiguration{
			ValueSerializer:     AvroSerializer,
			SubjectNameStrategy: RecordNameStrategy,
		},
		SchemaRegistry: SchemaRegistryConfiguration{
			URL: "http://localhost:8081",
		},
	}
	schema := `{
		"type":"record",
		"name":"TestAvroDeserializeUsingMagicPrefix",
		"namespace":"io.confluent.kafka.avro",
		"fields":[{"name":"field","type":"string"}]}`

	serialized, err := SerializeAvro(config, topic, data, Value, schema, 0)
	assert.Nil(t, err)

	dData, dErr := DeserializeAvro(config, topic, serialized, Value, "", 0)
	if dData, ok := dData.(map[string]interface{}); ok {
		assert.Equal(t, "value", dData["field"])
	} else {
		assert.Fail(t, "Deserialized data is not a map")
	}
	assert.Nil(t, dErr)
}

// TestAvroDeserializeUsingDefaultSubjectNameStrategy tests deserialization of
// Avro messages with the default topic name strategy.
func TestAvroDeserializeUsingDefaultSubjectNameStrategy(t *testing.T) {
	topic := "TestAvroDeserializeUsingDefaultSubjectNameStrategy-topic"
	config := Configuration{
		Producer: ProducerConfiguration{
			ValueSerializer: AvroSerializer,
		},
		Consumer: ConsumerConfiguration{
			ValueDeserializer: AvroSerializer,
		},
		SchemaRegistry: SchemaRegistryConfiguration{
			URL: "http://localhost:8081",
		},
	}
	schema := `{
		"type":"record",
		"name":"TestAvroDeserializeUsingDefaultSubjectNameStrategy",
		"namespace":"io.confluent.kafka.avro",
		"fields":[{"name":"field","type":"string"}]}`

	serialized, err := SerializeAvro(config, topic, data, Value, schema, 0)
	assert.Nil(t, err)

	dData, dErr := DeserializeAvro(config, topic, serialized, Value, "", 0)
	if dData, ok := dData.(map[string]interface{}); ok {
		assert.Equal(t, "value", dData["field"])
	} else {
		assert.Fail(t, "Deserialized data is not a map")
	}
	assert.Nil(t, dErr)
}

// TestAvroDeserializeUsingSubjectNameStrategyRecordName tests deserialization of
// Avro messages with the given topic record name strategy.
func TestAvroDeserializeUsingSubjectNameStrategyRecordName(t *testing.T) {
	topic := "TestAvroDeserializeUsingSubjectNameStrategyRecordName-topic"
	config := Configuration{
		Producer: ProducerConfiguration{
			ValueSerializer:     AvroSerializer,
			SubjectNameStrategy: RecordNameStrategy,
		},
		Consumer: ConsumerConfiguration{
			ValueDeserializer:   AvroSerializer,
			SubjectNameStrategy: RecordNameStrategy,
		},
		SchemaRegistry: SchemaRegistryConfiguration{
			URL: "http://localhost:8081",
		},
	}
	schema := `{
		"type":"record",
		"name":"TestAvroDeserializeUsingSubjectNameStrategyRecordName",
		"namespace":"io.confluent.kafka.avro",
		"fields":[{"name":"field","type":"string"}]}`

	serialized, err := SerializeAvro(config, topic, data, Value, schema, 0)
	assert.Nil(t, err)

	dData, dErr := DeserializeAvro(config, topic, serialized, Value, schema, 0)
	if dData, ok := dData.(map[string]interface{}); ok {
		assert.Equal(t, "value", dData["field"])
	} else {
		assert.Fail(t, "Deserialized data is not a map")
	}
	assert.Nil(t, dErr)
}

// TestAvroDeserializeUsingSubjectNameStrategyTopicRecordName tests deserialization of
// Avro messages with the given topic record name strategy.
func TestAvroDeserializeUsingSubjectNameStrategyTopicRecordName(t *testing.T) {
	topic := "TestAvroDeserializeUsingSubjectNameStrategyTopicRecordName-topic"
	config := Configuration{
		Producer: ProducerConfiguration{
			ValueSerializer:     AvroSerializer,
			SubjectNameStrategy: TopicRecordNameStrategy,
		},
		Consumer: ConsumerConfiguration{
			ValueDeserializer:   AvroSerializer,
			SubjectNameStrategy: TopicRecordNameStrategy,
		},
		SchemaRegistry: SchemaRegistryConfiguration{
			URL: "http://localhost:8081",
		},
	}
	schema := `{
		"type":"record",
		"name":"TestAvroDeserializeUsingSubjectNameStrategyTopicRecordName",
		"namespace":"io.confluent.kafka.avro",
		"fields":[{"name":"field","type":"string"}]}`

	serialized, err := SerializeAvro(config, topic, data, Value, schema, 0)
	assert.Nil(t, err)

	dData, dErr := DeserializeAvro(config, topic, serialized, Value, schema, 0)
	if dData, ok := dData.(map[string]interface{}); ok {
		assert.Equal(t, "value", dData["field"])
	} else {
		assert.Fail(t, "Deserialized data is not a map")
	}
	assert.Nil(t, dErr)
}

// TestAvroDeserializeUsingSubjectNameStrategyTopicName tests deserialization of Avro
// messages with the given topic name strategy.
func TestAvroDeserializeUsingSubjectNameStrategyTopicName(t *testing.T) {
	topic := "TestAvroDeserializeUsingSubjectNameStrategyTopicName-topic"
	config := Configuration{
		Producer: ProducerConfiguration{
			ValueSerializer:     AvroSerializer,
			SubjectNameStrategy: TopicNameStrategy,
		},
		Consumer: ConsumerConfiguration{
			ValueDeserializer:   AvroSerializer,
			SubjectNameStrategy: TopicNameStrategy,
		},
		SchemaRegistry: SchemaRegistryConfiguration{
			URL: "http://localhost:8081",
		},
	}
	schema := `{
		"type":"record",
		"name":"TestAvroDeserializeUsingSubjectNameStrategyTopicName",
		"namespace":"io.confluent.kafka.avro",
		"fields":[{"name":"field","type":"string"}]}`

	serialized, err := SerializeAvro(config, topic, data, Value, schema, 0)
	assert.Nil(t, err)

	dData, dErr := DeserializeAvro(config, topic, serialized, Value, schema, 0)
	if dData, ok := dData.(map[string]interface{}); ok {
		assert.Equal(t, "value", dData["field"])
	} else {
		assert.Fail(t, "Deserialized data is not a map")
	}
	assert.Nil(t, dErr)
}
