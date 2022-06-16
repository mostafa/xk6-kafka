package kafka

import (
	"testing"

	"github.com/riferrei/srclient"
	"github.com/stretchr/testify/assert"
)

// TestDecodeWireFormat tests the decoding of a wire-formatted message.
func TestDecodeWireFormat(t *testing.T) {
	encoded := []byte{0, 1, 2, 3, 4, 5}
	decoded := []byte{5}
	prefix := 16909060

	magic, result, err := DecodeWireFormat(encoded)
	assert.Nil(t, err)
	assert.Equal(t, decoded, result)
	assert.Equal(t, magic, prefix)
}

// TestDecodeWireFormatFails tests the decoding of a wire-formatted message and
// fails because the message is too short.
func TestDecodeWireFormatFails(t *testing.T) {
	encoded := []byte{0, 1, 2, 3} // too short

	_, result, err := DecodeWireFormat(encoded)
	assert.Nil(t, result)
	assert.NotNil(t, err)
	assert.Equal(t, "Invalid message: message too short to contain schema id.", err.Message)
	assert.Equal(t, messageTooShort, err.Code)
	assert.Nil(t, err.Unwrap())
}

// TestEncodeWireFormat tests the encoding of a message and adding wire-format to it
func TestEncodeWireFormat(t *testing.T) {
	data := []byte{6}
	schemaID := 5
	encoded := []byte{0, 0, 0, 0, 5, 6}

	result := EncodeWireFormat(data, schemaID)
	assert.Equal(t, encoded, result)
}

// TestSchemaRegistryClient tests the creation of a SchemaRegistryClient instance
// with the given configuration.
func TestSchemaRegistryClient(t *testing.T) {
	srConfig := SchemaRegistryConfiguration{
		Url: "http://localhost:8081",
		BasicAuth: BasicAuth{
			Username: "username",
			Password: "password",
		},
	}
	srClient := SchemaRegistryClientWithConfiguration(srConfig)
	assert.NotNil(t, srClient)
}

// TestSchemaRegistryClientWithTLSConfig tests the creation of a SchemaRegistryClient instance
// with the given configuration along with TLS configuration.
func TestSchemaRegistryClientWithTLSConfig(t *testing.T) {
	srConfig := SchemaRegistryConfiguration{
		Url: "http://localhost:8081",
		BasicAuth: BasicAuth{
			Username: "username",
			Password: "password",
		},
		TLSConfig: &TLSConfig{
			ClientCertPem: "fixtures/client.cer",
			ClientKeyPem:  "fixtures/client.pem",
			ServerCaPem:   "fixtures/caroot.cer",
		},
	}
	srClient := SchemaRegistryClientWithConfiguration(srConfig)
	assert.NotNil(t, srClient)
}

// TestGetLatestSchemaFails tests getting the latest schema and fails because
// the configuration is invalid.
func TestGetLatestSchemaFails(t *testing.T) {
	srConfig := SchemaRegistryConfiguration{
		Url: "http://localhost:8081",
		BasicAuth: BasicAuth{
			Username: "username",
			Password: "password",
		},
	}
	srClient := SchemaRegistryClientWithConfiguration(srConfig)
	schema, err := GetSchema(srClient, "test-subject", "test-schema", srclient.Avro, 0)
	assert.Nil(t, schema)
	assert.NotNil(t, err)
	assert.Equal(t, "Failed to get schema from schema registry", err.Message)
}

// TestGetSchemaFails tests getting the first version of the schema and fails because
// the configuration is invalid.
func TestGetSchemaFails(t *testing.T) {
	srConfig := SchemaRegistryConfiguration{
		Url: "http://localhost:8081",
		BasicAuth: BasicAuth{
			Username: "username",
			Password: "password",
		},
	}
	srClient := SchemaRegistryClientWithConfiguration(srConfig)
	schema, err := GetSchema(srClient, "test-subject", "test-schema", srclient.Avro, 1)
	assert.Nil(t, schema)
	assert.NotNil(t, err)
	assert.Equal(t, "Failed to get schema from schema registry", err.Message)
}

// TestCreateSchemaFails tests creating the schema and fails because the
// configuration is invalid.
func TestCreateSchemaFails(t *testing.T) {
	srConfig := SchemaRegistryConfiguration{
		Url: "http://localhost:8081",
		BasicAuth: BasicAuth{
			Username: "username",
			Password: "password",
		},
	}
	srClient := SchemaRegistryClientWithConfiguration(srConfig)
	schema, err := CreateSchema(srClient, "test-subject", "test-schema", srclient.Avro)
	assert.Nil(t, schema)
	assert.NotNil(t, err)
	assert.Equal(t, "Failed to create schema.", err.Message)
}

func TestGetSubjectNameFailsIfInvalidSchema(t *testing.T) {
	_, err := GetSubjectName(`Bad Schema`, "test-topic", Value, RecordNameStrategy)
	assert.NotNil(t, err)
	assert.Contains(t, err.Message, "Failed to unmarshal schema")
}

func TestGetSubjectNameFailsIfSubjectNameStrategyUnknown(t *testing.T) {
	avroSchema := `{"type":"record","name":"Schema","fields":[{"name":"field","type":"string"}]}`
	_, err := GetSubjectName(avroSchema, "test-topic", Value, "Unknown")
	assert.NotNil(t, err)
	assert.Contains(t, err.Message, "Unknown subject name strategy")
}

func TestGetSubjectNameCanUseDefaultSubjectNameStrategy(t *testing.T) {
	avroSchema := `{"type":"record","name":"Schema","fields":[{"name":"field","type":"string"}]}`
	for _, element := range []Element{Key, Value} {
		subject, err := GetSubjectName(avroSchema, "test-topic", element, "")
		assert.Nil(t, err)
		assert.Equal(t, "test-topic-"+string(element), subject)
	}
}

func TestGetSubjectNameCanUseTopicNameStrategy(t *testing.T) {
	avroSchema := `{"type":"record","name":"Schema","fields":[{"name":"field","type":"string"}]}`

	for _, element := range []Element{Key, Value} {
		subject, err := GetSubjectName(avroSchema, "test-topic", element, TopicNameStrategy)
		assert.Nil(t, err)
		assert.Equal(t, "test-topic-"+string(element), subject)
	}
}

func TestGetSubjectNameCanUseTopicRecordNameStrategyWithNamespace(t *testing.T) {
	avroSchema := `{"type":"record","namespace":"com.example.person","name":"Schema","fields":[{"name":"field","type":"string"}]}`
	subject, err := GetSubjectName(avroSchema, "test-topic", Value, TopicRecordNameStrategy)
	assert.Nil(t, err)
	assert.Equal(t, "test-topic-com.example.person.Schema", subject)
}

func TestGetSubjectNameCanUseTopicRecordNameStrategyWithoutNamespace(t *testing.T) {
	avroSchema := `{"type":"record","name":"Schema","fields":[{"name":"field","type":"string"}]}`
	subject, err := GetSubjectName(avroSchema, "test-topic", Value, TopicRecordNameStrategy)
	assert.Nil(t, err)
	assert.Equal(t, "test-topic-Schema", subject)
}

func TestGetSubjectNameCanUseRecordNameStrategyWithoutNamespace(t *testing.T) {
	avroSchema := `{"type":"record","name":"Schema","fields":[{"name":"field","type":"string"}]}`
	subject, err := GetSubjectName(avroSchema, "test-topic", Value, RecordNameStrategy)
	assert.Nil(t, err)
	assert.Equal(t, "Schema", subject)
}

func TestGetSubjectNameCanUseRecordNameStrategyWithNamespace(t *testing.T) {
	avroSchema := `{"type":"record","namespace":"com.example.person","name":"Schema","fields":[{"name":"field","type":"string"}]}`
	subject, err := GetSubjectName(avroSchema, "test-topic", Value, RecordNameStrategy)
	assert.Nil(t, err)
	assert.Equal(t, "com.example.person.Schema", subject)
}

func TestDecodeWireRequiresMagicPrefixByte(t *testing.T) {
	encoded := []byte{1, 1, 2, 3, 4} // too short

	_, result, err := DecodeWireFormat(encoded)
	assert.Nil(t, result)
	assert.NotNil(t, err)
	assert.Equal(t, "Invalid message: invalid start byte.", err.Message)
	assert.Equal(t, messageTooShort, err.Code)
	assert.Nil(t, err.Unwrap())
}
