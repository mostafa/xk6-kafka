package kafka

import (
	"testing"

	"github.com/dop251/goja"
	"github.com/stretchr/testify/assert"
)

var avroSchemaForSRTests = `{"type":"record","name":"Schema","fields":[{"name":"field","type":"string"}]}`

// TestDecodeWireFormat tests the decoding of a wire-formatted message.
func TestDecodeWireFormat(t *testing.T) {
	test := getTestModuleInstance(t)

	encoded := []byte{0, 1, 2, 3, 4, 5}
	decoded := []byte{5}

	result := test.module.Kafka.decodeWireFormat(encoded)
	assert.Equal(t, decoded, result)
}

// TestDecodeWireFormatFails tests the decoding of a wire-formatted message and
// fails because the message is too short.
func TestDecodeWireFormatFails(t *testing.T) {
	test := getTestModuleInstance(t)

	encoded := []byte{0, 1, 2, 3} // too short

	defer func() {
		err := recover()
		assert.Equal(t,
			err.(*goja.Object).ToString().String(),
			"Invalid message: message too short to contain schema id.")
	}()

	test.module.Kafka.decodeWireFormat(encoded)
}

// TestEncodeWireFormat tests the encoding of a message and adding wire-format to it.
func TestEncodeWireFormat(t *testing.T) {
	test := getTestModuleInstance(t)

	data := []byte{6}
	schemaID := 5
	encoded := []byte{0, 0, 0, 0, 5, 6}

	result := test.module.Kafka.encodeWireFormat(data, schemaID)
	assert.Equal(t, encoded, result)
}

// TestSchemaRegistryClient tests the creation of a SchemaRegistryClient instance
// with the given configuration.
func TestSchemaRegistryClient(t *testing.T) {
	test := getTestModuleInstance(t)

	srConfig := SchemaRegistryConfiguration{
		URL: "http://localhost:8081",
		BasicAuth: BasicAuth{
			Username: "username",
			Password: "password",
		},
	}
	srClient := test.module.Kafka.schemaRegistryClient(&srConfig)
	assert.NotNil(t, srClient)
}

// TestSchemaRegistryClientWithTLSConfig tests the creation of a SchemaRegistryClient instance
// with the given configuration along with TLS configuration.
func TestSchemaRegistryClientWithTLSConfig(t *testing.T) {
	test := getTestModuleInstance(t)

	srConfig := SchemaRegistryConfiguration{
		URL: "http://localhost:8081",
		BasicAuth: BasicAuth{
			Username: "username",
			Password: "password",
		},
		TLS: TLSConfig{
			ClientCertPem: "fixtures/client.cer",
			ClientKeyPem:  "fixtures/client.pem",
			ServerCaPem:   "fixtures/caroot.cer",
		},
	}
	srClient := test.module.Kafka.schemaRegistryClient(&srConfig)
	assert.NotNil(t, srClient)
}

// TestGetLatestSchemaFails tests getting the latest schema and fails because
// the configuration is invalid.
func TestGetLatestSchemaFails(t *testing.T) {
	test := getTestModuleInstance(t)

	srConfig := SchemaRegistryConfiguration{
		URL: "http://localhost:8081",
		BasicAuth: BasicAuth{
			Username: "username",
			Password: "password",
		},
	}
	srClient := test.module.Kafka.schemaRegistryClient(&srConfig)
	assert.Panics(t, func() {
		schema := test.module.Kafka.getSchema(srClient, &Schema{
			Subject: "test-subject",
			Version: 0,
		})
		assert.Equal(t, schema, nil)
	})
}

// TestGetSchemaFails tests getting the first version of the schema and fails because
// the configuration is invalid.
func TestGetSchemaFails(t *testing.T) {
	test := getTestModuleInstance(t)

	srConfig := SchemaRegistryConfiguration{
		URL: "http://localhost:8081",
		BasicAuth: BasicAuth{
			Username: "username",
			Password: "password",
		},
	}
	srClient := test.module.Kafka.schemaRegistryClient(&srConfig)
	assert.Panics(t, func() {
		schema := test.module.Kafka.getSchema(srClient, &Schema{
			Subject: "test-subject",
			Version: 0,
		})
		assert.Equal(t, schema, nil)
	})
}

// TestCreateSchemaFails tests creating the schema and fails because the
// configuration is invalid.
func TestCreateSchemaFails(t *testing.T) {
	test := getTestModuleInstance(t)

	srConfig := SchemaRegistryConfiguration{
		URL: "http://localhost:8081",
		BasicAuth: BasicAuth{
			Username: "username",
			Password: "password",
		},
	}
	srClient := test.module.Kafka.schemaRegistryClient(&srConfig)
	assert.Panics(t, func() {
		schema := test.module.Kafka.getSchema(srClient, &Schema{
			Subject: "test-subject",
			Version: 0,
		})
		assert.Equal(t, schema, nil)
	})
}

func TestGetSubjectNameFailsIfInvalidSchema(t *testing.T) {
	test := getTestModuleInstance(t)

	assert.Panics(t, func() {
		subjectName := test.module.Kafka.getSubjectName(&SubjectNameConfig{
			Schema:              `Bad Schema`,
			Topic:               "test-topic",
			SubjectNameStrategy: RecordNameStrategy,
			Element:             Value,
		})
		assert.Equal(t, subjectName, "")
	})
}

func TestGetSubjectNameFailsIfSubjectNameStrategyUnknown(t *testing.T) {
	test := getTestModuleInstance(t)

	assert.Panics(t, func() {
		subjectName := test.module.Kafka.getSubjectName(&SubjectNameConfig{
			Schema:              avroSchemaForSRTests,
			Topic:               "test-topic",
			SubjectNameStrategy: "Unknown",
			Element:             Value,
		})
		assert.Equal(t, subjectName, "")
	})
}

func TestGetSubjectNameCanUseDefaultSubjectNameStrategy(t *testing.T) {
	test := getTestModuleInstance(t)

	for _, element := range []Element{Key, Value} {
		subjectName := test.module.Kafka.getSubjectName(&SubjectNameConfig{
			Schema:              avroSchemaForSRTests,
			Topic:               "test-topic",
			SubjectNameStrategy: "",
			Element:             element,
		})
		assert.Equal(t, "test-topic-"+string(element), subjectName)
	}
}

func TestGetSubjectNameCanUseTopicNameStrategy(t *testing.T) {
	test := getTestModuleInstance(t)

	for _, element := range []Element{Key, Value} {
		subjectName := test.module.Kafka.getSubjectName(&SubjectNameConfig{
			Schema:              avroSchemaForSRTests,
			Topic:               "test-topic",
			SubjectNameStrategy: TopicNameStrategy,
			Element:             element,
		})
		assert.Equal(t, "test-topic-"+string(element), subjectName)
	}
}

func TestGetSubjectNameCanUseTopicRecordNameStrategyWithNamespace(t *testing.T) {
	test := getTestModuleInstance(t)

	avroSchema := `{
		"type":"record",
		"namespace":"com.example.person",
		"name":"Schema",
		"fields":[{"name":"field","type":"string"}]}`
	subjectName := test.module.Kafka.getSubjectName(&SubjectNameConfig{
		Schema:              avroSchema,
		Topic:               "test-topic",
		SubjectNameStrategy: TopicRecordNameStrategy,
		Element:             Value,
	})
	assert.Equal(t, "test-topic-com.example.person.Schema", subjectName)
}

func TestGetSubjectNameCanUseTopicRecordNameStrategyWithoutNamespace(t *testing.T) {
	test := getTestModuleInstance(t)

	subjectName := test.module.Kafka.getSubjectName(&SubjectNameConfig{
		Schema:              avroSchemaForSRTests,
		Topic:               "test-topic",
		SubjectNameStrategy: TopicRecordNameStrategy,
		Element:             Value,
	})
	assert.Equal(t, "test-topic-Schema", subjectName)
}

func TestGetSubjectNameCanUseRecordNameStrategyWithoutNamespace(t *testing.T) {
	test := getTestModuleInstance(t)

	subject := test.module.Kafka.getSubjectName(&SubjectNameConfig{
		Schema:              avroSchemaForSRTests,
		Topic:               "test-topic",
		SubjectNameStrategy: RecordNameStrategy,
		Element:             Value,
	})
	assert.Equal(t, "Schema", subject)
}

func TestGetSubjectNameCanUseRecordNameStrategyWithNamespace(t *testing.T) {
	test := getTestModuleInstance(t)

	avroSchema := `{
		"type":"record",
		"namespace":"com.example.person",
		"name":"Schema",
		"fields":[{"name":"field","type":"string"}]}`
	subjectName := test.module.Kafka.getSubjectName(&SubjectNameConfig{
		Schema:              avroSchema,
		Topic:               "test-topic",
		SubjectNameStrategy: RecordNameStrategy,
		Element:             Value,
	})
	assert.Equal(t, "com.example.person.Schema", subjectName)
}
