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
	avroSchema string = `{"type":"record","name":"Schema","namespace":"io.confluent.kafka.avro","fields":[{"name":"field","type":"string"}]}`
	data       string = `{"field":"value"}`
)

// TestSerializeDeserializeAvro tests serialization and deserialization of Avro messages
func TestSerializeDeserializeAvro(t *testing.T) {
	// Test with a schema registry, which fails and manually (de)serializes the data
	for _, element := range []Element{Key, Value} {
		// Serialize the key or value
		serialized, err := SerializeAvro(avroConfig, "topic", `{"field":"value"}`, element, avroSchema, 0)
		assert.Nil(t, err)
		assert.NotNil(t, serialized)
		// 4 bytes for magic byte, 1 byte for schema ID, and the rest is the data
		assert.GreaterOrEqual(t, len(serialized), 10)

		// Deserialize the key or value (removes the magic bytes)
		deserialized, err := DeserializeAvro(avroConfig, "", serialized, element, avroSchema, 0)
		assert.Nil(t, err)
		assert.Equal(t, map[string]interface{}{"field": "value"}, deserialized)
	}
}

// TestSerializeDeserializeAvroFailsOnSchemaError tests serialization and deserialization of Avro messages and fails on schema error
func TestSerializeDeserializeAvroFailsOnSchemaError(t *testing.T) {
	jsonSchema = `{}`

	for _, element := range []Element{Key, Value} {
		// Serialize the key or value
		serialized, err := SerializeAvro(avroConfig, "topic", `{"field":"value"}`, element, jsonSchema, 0)
		assert.Nil(t, serialized)
		assert.Error(t, err.Unwrap())
		assert.Equal(t, "Failed to create codec for encoding Avro", err.Message)
		assert.Equal(t, failedCreateAvroCodec, err.Code)

		// Deserialize the key or value
		deserialized, err := DeserializeAvro(avroConfig, "topic", []byte{0, 1, 2, 3, 4, 5}, element, jsonSchema, 0)
		assert.Nil(t, deserialized)
		assert.Error(t, err.Unwrap())
		assert.Equal(t, "Failed to create codec for decoding Avro", err.Message)
		assert.Equal(t, failedCreateAvroCodec, err.Code)
	}
}

// TestSerializeDeserializeAvroFailsOnWireFormatError tests serialization and deserialization of Avro messages and fails on wire format error
func TestSerializeDeserializeAvroFailsOnWireFormatError(t *testing.T) {
	schema := `{}`

	for _, element := range []Element{Key, Value} {
		// Deserialize an empty key or value
		deserialized, err := DeserializeAvro(avroConfig, "topic", []byte{}, element, schema, 0)
		assert.Nil(t, deserialized)
		assert.Error(t, err.Unwrap())
		assert.Equal(t, "Failed to remove wire format from the binary data", err.Message)
		assert.Equal(t, failedDecodeFromWireFormat, err.Code)

		// Deserialize a broken key or value
		// Proper wire-formatted message has 5 bytes (the wire format) plus data
		deserialized, err = DeserializeAvro(avroConfig, "topic", []byte{0, 1, 2, 3}, element, schema, 0)
		assert.Nil(t, deserialized)
		assert.Error(t, err.Unwrap())
		assert.Equal(t, "Failed to remove wire format from the binary data", err.Message)
		assert.Equal(t, failedDecodeFromWireFormat, err.Code)
	}
}

// TestSerializeDeserializeAvroFailsOnEncodeDecodeError tests serialization and deserialization of Avro messages and fails on encode/decode error
func TestSerializeDeserializeAvroFailsOnEncodeDecodeError(t *testing.T) {
	data := `{"nonExistingField":"value"}`

	for _, element := range []Element{Key, Value} {
		serialized, err := SerializeAvro(avroConfig, "topic", data, element, avroSchema, 0)
		assert.Nil(t, serialized)
		assert.Error(t, err.Unwrap())
		assert.Equal(t, "Failed to encode data into Avro", err.Message)
		assert.Equal(t, failedEncodeToAvro, err.Code)

		deserialized, err := DeserializeAvro(avroConfig, "topic", []byte{0, 1, 2, 3, 5}, element, avroSchema, 0)
		assert.Nil(t, deserialized)
		assert.Error(t, err.Unwrap())
		assert.Equal(t, "Failed to decode data from Avro", err.Message)
		assert.Equal(t, failedDecodeAvroFromBinary, err.Code)
	}
}

func TestAvroSerializeTopicNameStrategy(t *testing.T) {
	topic := "TestAvroSerializeTopicNameStrategy-topic"
	config := Configuration{
		Producer: ProducerConfiguration{
			ValueSerializer:     AvroSerializer,
			SubjectNameStrategy: TopicNameStrategy,
		},
		SchemaRegistry: SchemaRegistryConfiguration{
			Url: "http://localhost:8081",
		},
	}

	schema := `{"type":"record","name":"TestAvroSerializeTopicNameStrategyIsDefaultStrategy","namespace":"io.confluent.kafka.avro","fields":[{"name":"field","type":"string"}]}`

	serialized, err := SerializeAvro(config, topic, data, Value, schema, 0)
	assert.Nil(t, err)
	assert.NotNil(t, serialized)

	expectedSubject := topic + "-value"
	srClient := SchemaRegistryClientWithConfiguration(config.SchemaRegistry)
	schemaResult, err := GetSchema(srClient, expectedSubject, schema, srclient.Avro, 0)
	assert.Nil(t, err)
	assert.NotNil(t, schemaResult)
}

func TestAvroSerializeTopicNameStrategyIsDefaultStrategy(t *testing.T) {
	topic := "TestAvroSerializeTopicNameStrategyIsDefaultStrategy-topic"
	config := Configuration{
		Producer: ProducerConfiguration{
			ValueSerializer: AvroSerializer,
		},
		SchemaRegistry: SchemaRegistryConfiguration{
			Url: "http://localhost:8081",
		},
	}

	schema := `{"type":"record","name":"TestAvroSerializeTopicNameStrategyIsDefaultStrategy","namespace":"io.confluent.kafka.avro","fields":[{"name":"field","type":"string"}]}`

	serialized, err := SerializeAvro(config, topic, data, Value, schema, 0)
	assert.Nil(t, err)
	assert.NotNil(t, serialized)

	expectedSubject := topic + "-value"
	srClient := SchemaRegistryClientWithConfiguration(config.SchemaRegistry)
	schemaResult, err := GetSchema(srClient, expectedSubject, schema, srclient.Avro, 0)
	assert.Nil(t, err)
	assert.NotNil(t, schemaResult)
}

func TestAvroSerializeTopicRecordNameStrategy(t *testing.T) {
	topic := "TestAvroSerializeTopicRecordNameStrategy-topic"
	config := Configuration{
		Producer: ProducerConfiguration{
			ValueSerializer:     AvroSerializer,
			SubjectNameStrategy: TopicRecordNameStrategy,
		},
		SchemaRegistry: SchemaRegistryConfiguration{
			Url: "http://localhost:8081",
		},
	}
	schema := `{"type":"record","name":"TestAvroSerializeTopicRecordNameStrategy","namespace":"io.confluent.kafka.avro","fields":[{"name":"field","type":"string"}]}`

	serialized, err := SerializeAvro(config, topic, data, Value, schema, 0)
	assert.Nil(t, err)
	assert.NotNil(t, serialized)

	expectedSubject := topic + "-io.confluent.kafka.avro.TestAvroSerializeTopicRecordNameStrategy"
	srClient := SchemaRegistryClientWithConfiguration(config.SchemaRegistry)
	schemaResult, err := GetSchema(srClient, expectedSubject, schema, srclient.Avro, 0)
	assert.Nil(t, err)
	assert.NotNil(t, schemaResult)
}

func TestAvroSerializeRecordNameStrategy(t *testing.T) {
	topic := "TestAvroSerializeRecordNameStrategy-topic"
	config := Configuration{
		Producer: ProducerConfiguration{
			ValueSerializer:     AvroSerializer,
			SubjectNameStrategy: RecordNameStrategy,
		},
		SchemaRegistry: SchemaRegistryConfiguration{
			Url: "http://localhost:8081",
		},
	}
	schema := `{"type":"record","name":"TestAvroSerializeRecordNameStrategy","namespace":"io.confluent.kafka.avro","fields":[{"name":"field","type":"string"}]}`

	serialized, err := SerializeAvro(config, topic, data, Value, schema, 0)
	assert.Nil(t, err)
	assert.NotNil(t, serialized)

	expectedSubject := "io.confluent.kafka.avro.TestAvroSerializeRecordNameStrategy"
	srClient := SchemaRegistryClientWithConfiguration(config.SchemaRegistry)
	resultSchema, err := GetSchema(srClient, expectedSubject, avroSchema, srclient.Avro, 0)
	assert.Nil(t, err)
	assert.NotNil(t, resultSchema)
}

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
			Url: "http://localhost:8081",
		},
	}
	schema := `{"type":"record","name":"TestAvroDeserializeUsingMagicPrefix","namespace":"io.confluent.kafka.avro","fields":[{"name":"field","type":"string"}]}`

	serialized, err := SerializeAvro(config, topic, data, Value, schema, 0)
	assert.Nil(t, err)

	dData, dErr := DeserializeAvro(config, topic, serialized, Value, "", 0)
	assert.Equal(t, "value", dData.(map[string]interface{})["field"])
	assert.Nil(t, dErr)
}

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
			Url: "http://localhost:8081",
		},
	}
	schema := `{"type":"record","name":"TestAvroDeserializeUsingDefaultSubjectNameStrategy","namespace":"io.confluent.kafka.avro","fields":[{"name":"field","type":"string"}]}`

	serialized, err := SerializeAvro(config, topic, data, Value, schema, 0)
	assert.Nil(t, err)

	dData, dErr := DeserializeAvro(config, topic, serialized, Value, "", 0)
	assert.Equal(t, "value", dData.(map[string]interface{})["field"])
	assert.Nil(t, dErr)
}

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
			Url: "http://localhost:8081",
		},
	}
	schema := `{"type":"record","name":"TestAvroDeserializeUsingSubjectNameStrategyRecordName","namespace":"io.confluent.kafka.avro","fields":[{"name":"field","type":"string"}]}`

	serialized, err := SerializeAvro(config, topic, data, Value, schema, 0)
	assert.Nil(t, err)

	dData, dErr := DeserializeAvro(config, topic, serialized, Value, schema, 0)
	assert.Equal(t, "value", dData.(map[string]interface{})["field"])
	assert.Nil(t, dErr)
}

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
			Url: "http://localhost:8081",
		},
	}
	schema := `{"type":"record","name":"TestAvroDeserializeUsingSubjectNameStrategyTopicRecordName","namespace":"io.confluent.kafka.avro","fields":[{"name":"field","type":"string"}]}`

	serialized, err := SerializeAvro(config, topic, data, Value, schema, 0)
	assert.Nil(t, err)

	dData, dErr := DeserializeAvro(config, topic, serialized, Value, schema, 0)
	assert.Equal(t, "value", dData.(map[string]interface{})["field"])
	assert.Nil(t, dErr)
}

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
			Url: "http://localhost:8081",
		},
	}
	schema := `{"type":"record","name":"TestAvroDeserializeUsingSubjectNameStrategyTopicName","namespace":"io.confluent.kafka.avro","fields":[{"name":"field","type":"string"}]}`

	serialized, err := SerializeAvro(config, topic, data, Value, schema, 0)
	assert.Nil(t, err)

	dData, dErr := DeserializeAvro(config, topic, serialized, Value, schema, 0)
	assert.Equal(t, "value", dData.(map[string]interface{})["field"])
	assert.Nil(t, dErr)
}
