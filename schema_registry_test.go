package kafka

import (
	"testing"

	"github.com/grafana/sobek"
	"github.com/riferrei/srclient"
	"github.com/stretchr/testify/assert"
)

var avroSchemaForSRTests = `{"type":"record","name":"Schema","fields":[{"name":"field","type":"string"}]}`

// TestDecodeWireFormat tests the decoding of a wire-formatted message.
func TestDecodeWireFormat(t *testing.T) {
	test := getTestModuleInstance(t)

	encoded := []byte{0, 1, 2, 3, 4, 5}
	decoded := []byte{5}

	result := test.module.decodeWireFormat(encoded)
	assert.Equal(t, decoded, result)
}

// TestDecodeWireFormatFails tests the decoding of a wire-formatted message and
// fails because the message is too short.
func TestDecodeWireFormatFails(t *testing.T) {
	test := getTestModuleInstance(t)

	encoded := []byte{0, 1, 2, 3} // too short

	defer func() {
		err := recover()
		errObj, ok := err.(*sobek.Object)
		assert.True(t, ok)
		assert.Equal(t,
			errObj.ToString().String(),
			GoErrorPrefix+"Invalid message: message too short to contain schema id.")
	}()

	test.module.decodeWireFormat(encoded)
}

// TestEncodeWireFormat tests the encoding of a message and adding wire-format to it.
func TestEncodeWireFormat(t *testing.T) {
	test := getTestModuleInstance(t)

	data := []byte{6}
	schemaID := 5
	encoded := []byte{0, 0, 0, 0, 5, 6}

	result := test.module.encodeWireFormat(data, schemaID)
	assert.Equal(t, encoded, result)
}

// TestSchemaRegistryClient tests the creation of a SchemaRegistryClient instance
// with the given configuration.
func TestSchemaRegistryClient(t *testing.T) {
	test := getTestModuleInstance(t)

	srConfig := SchemaRegistryConfig{
		URL: "http://localhost:8081",
		BasicAuth: BasicAuth{
			Username: "username",
			Password: "password",
		},
	}
	srClient := test.module.schemaRegistryClient(&srConfig)
	assert.NotNil(t, srClient)
}

// TestSchemaRegistryClientWithTLSConfig tests the creation of a SchemaRegistryClient instance
// with the given configuration along with TLS configuration.
func TestSchemaRegistryClientWithTLSConfig(t *testing.T) {
	test := getTestModuleInstance(t)

	srConfig := SchemaRegistryConfig{
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
	srClient := test.module.schemaRegistryClient(&srConfig)
	assert.NotNil(t, srClient)
}

// TestGetLatestSchemaFails tests getting the latest schema and fails because
// the configuration is invalid.
func TestGetLatestSchemaFails(t *testing.T) {
	test := getTestModuleInstance(t)

	srConfig := SchemaRegistryConfig{
		URL: "http://localhost:8081",
		BasicAuth: BasicAuth{
			Username: "username",
			Password: "password",
		},
	}
	srClient := test.module.schemaRegistryClient(&srConfig)
	assert.Panics(t, func() {
		schema := test.module.getSchema(srClient, &Schema{
			Subject: "no-such-subject",
			Version: 0,
		})
		assert.Equal(t, schema, nil)
	})
}

// TestGetSchemaFails tests getting the first version of the schema and fails because
// the configuration is invalid.
func TestGetSchemaFails(t *testing.T) {
	test := getTestModuleInstance(t)

	srConfig := SchemaRegistryConfig{
		URL: "http://localhost:8081",
		BasicAuth: BasicAuth{
			Username: "username",
			Password: "password",
		},
	}
	srClient := test.module.schemaRegistryClient(&srConfig)
	assert.Panics(t, func() {
		schema := test.module.getSchema(srClient, &Schema{
			Subject: "no-such-subject",
			Version: 0,
		})
		assert.Equal(t, schema, nil)
	})
}

// TestCreateSchemaFails tests creating the schema and fails because the
// configuration is invalid.
func TestCreateSchemaFails(t *testing.T) {
	test := getTestModuleInstance(t)

	srConfig := SchemaRegistryConfig{
		URL: "http://localhost:8081",
		BasicAuth: BasicAuth{
			Username: "username",
			Password: "password",
		},
	}
	srClient := test.module.schemaRegistryClient(&srConfig)
	assert.Panics(t, func() {
		schema := test.module.getSchema(srClient, &Schema{
			Subject: "no-such-subject",
			Version: 0,
		})
		assert.Equal(t, schema, nil)
	})
}

func TestGetSubjectNameFailsIfInvalidSchema(t *testing.T) {
	test := getTestModuleInstance(t)

	assert.Panics(t, func() {
		subjectName := test.module.getSubjectName(&SubjectNameConfig{
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
		subjectName := test.module.getSubjectName(&SubjectNameConfig{
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
		subjectName := test.module.getSubjectName(&SubjectNameConfig{
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
		subjectName := test.module.getSubjectName(&SubjectNameConfig{
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
	subjectName := test.module.getSubjectName(&SubjectNameConfig{
		Schema:              avroSchema,
		Topic:               "test-topic",
		SubjectNameStrategy: TopicRecordNameStrategy,
		Element:             Value,
	})
	assert.Equal(t, "test-topic-com.example.person.Schema", subjectName)
}

func TestGetSubjectNameCanUseTopicRecordNameStrategyWithoutNamespace(t *testing.T) {
	test := getTestModuleInstance(t)

	subjectName := test.module.getSubjectName(&SubjectNameConfig{
		Schema:              avroSchemaForSRTests,
		Topic:               "test-topic",
		SubjectNameStrategy: TopicRecordNameStrategy,
		Element:             Value,
	})
	assert.Equal(t, "test-topic-Schema", subjectName)
}

func TestGetSubjectNameCanUseRecordNameStrategyWithoutNamespace(t *testing.T) {
	test := getTestModuleInstance(t)

	subject := test.module.getSubjectName(&SubjectNameConfig{
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
	subjectName := test.module.getSubjectName(&SubjectNameConfig{
		Schema:              avroSchema,
		Topic:               "test-topic",
		SubjectNameStrategy: RecordNameStrategy,
		Element:             Value,
	})
	assert.Equal(t, "com.example.person.Schema", subjectName)
}

// TestSchemaRegistryClientClass tests the schema registry client class.
func TestSchemaRegistryClientClass(t *testing.T) {
	test := getTestModuleInstance(t)
	avroSchema := `{"type":"record","name":"Schema","namespace":"com.example.person",` +
		`"fields":[{"name":"field","type":"string"}]}`

	test.moveToVUCode()
	assert.NotPanics(t, func() {
		// Create a schema registry client.
		client := test.module.schemaRegistryClientClass(sobek.ConstructorCall{
			Arguments: []sobek.Value{
				test.module.vu.Runtime().ToValue(
					map[string]any{
						"url": "http://localhost:8081",
					},
				),
			},
		})
		assert.NotNil(t, client)

		// Create a schema and send it to the registry.
		createSchemaVal := client.Get("createSchema").Export()
		createSchema, ok := createSchemaVal.(func(sobek.FunctionCall) sobek.Value)
		assert.True(t, ok)
		newSchema := createSchema(sobek.FunctionCall{
			Arguments: []sobek.Value{
				test.module.vu.Runtime().ToValue(
					map[string]any{
						"subject":    "test-subject",
						"schema":     avroSchema,
						"schemaType": srclient.Avro,
					},
				),
			},
		}).Export().(*Schema)
		assert.Equal(t, "test-subject", newSchema.Subject)
		assert.Equal(t, 0, newSchema.Version)

		// Get the latest version of the schema from the registry.
		getSchemaVal := client.Get("getSchema").Export()
		getSchema, ok := getSchemaVal.(func(sobek.FunctionCall) sobek.Value)
		assert.True(t, ok)
		currentSchema := getSchema(sobek.FunctionCall{
			Arguments: []sobek.Value{
				test.module.vu.Runtime().ToValue(
					map[string]any{
						"subject": "test-subject",
						"version": 0,
					},
				),
			},
		}).Export().(*Schema)
		assert.Equal(t, "test-subject", currentSchema.Subject)
		assert.Equal(t, 1, currentSchema.Version)
		assert.Equal(t, avroSchema, currentSchema.Schema)

		// Get the subject name based on the given subject name config.
		getSubjectName := client.Get("getSubjectName").Export().(func(sobek.FunctionCall) sobek.Value)
		subjectName := getSubjectName(sobek.FunctionCall{
			Arguments: []sobek.Value{
				test.module.vu.Runtime().ToValue(
					map[string]any{
						"schema":              avroSchema,
						"topic":               "test-topic",
						"subjectNameStrategy": TopicRecordNameStrategy,
						"element":             Value,
					},
				),
			},
		}).Export().(string)
		assert.Equal(t, "test-topic-com.example.person.Schema", subjectName)

		// Serialize the given value to byte array.
		serialize := client.Get("serialize").Export().(func(sobek.FunctionCall) sobek.Value)
		serialized := serialize(sobek.FunctionCall{
			Arguments: []sobek.Value{
				test.module.vu.Runtime().ToValue(
					map[string]any{
						"data":       map[string]any{"field": "value"},
						"schema":     currentSchema,
						"schemaType": srclient.Avro,
					},
				),
			},
		}).Export().([]byte)
		assert.NotNil(t, serialized)

		// Deserialize the given byte array to the actual value.
		deserialize := client.Get("deserialize").Export().(func(sobek.FunctionCall) sobek.Value)
		deserialized := deserialize(sobek.FunctionCall{
			Arguments: []sobek.Value{
				test.module.vu.Runtime().ToValue(
					map[string]any{
						"data":       serialized,
						"schema":     currentSchema,
						"schemaType": srclient.Avro,
					},
				),
			},
		}).Export().(map[string]any)
		assert.Equal(t, "value", deserialized["field"])
	})
}
