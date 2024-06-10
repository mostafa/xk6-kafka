package kafka

import (
	"errors"
	"testing"
	"unsafe"

	"github.com/grafana/sobek"
	"github.com/riferrei/srclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSerdes tests serialization and deserialization of messages with different schemas.
func TestSerdes(t *testing.T) {
	test := getTestModuleInstance(t)

	test.createTopic("test-serdes-topic")
	writer := test.newWriter("test-serdes-topic")
	defer writer.Close()
	reader := test.newReader("test-serdes-topic")
	defer reader.Close()

	// Switch to VU code.
	require.NoError(t, test.moveToVUCode())

	containers := []*Container{
		{
			Data:       "string",
			SchemaType: String,
		},
		{
			Data:       []byte("byte array"),
			SchemaType: Bytes,
		},
		{
			Data:       []byte{62, 79, 74, 65, 20, 61, 72, 72, 61, 79}, // byte array
			SchemaType: Bytes,
		},
		{
			Data: map[string]interface{}{
				"string": "some-string",
				"number": 1.1,
				"bool":   true,
				"null":   nil,
				"array":  []interface{}{1.0, 2.0, 3.0},
				"object": map[string]interface{}{
					"string": "string-value",
					"number": 1.1,
					"bool":   true,
					"null":   nil,
					"array":  []interface{}{1.0, 2.0, 3.0},
				},
			},
			SchemaType: srclient.Json,
		},
		{
			Data: map[string]interface{}{"key": "value"},
			Schema: &Schema{
				ID: 1,
				Schema: `{
					"$schema": "http://json-schema.org/draft-04/schema#",
					"type": "object",
					"properties": {
						"key": {"type": "string"}
					},
					"required": ["key"]
				}`,
				Version: 1,
				Subject: "json-schema",
			},
			SchemaType: srclient.Json,
		},
		{
			Data: map[string]interface{}{"key": "value"},
			Schema: &Schema{
				ID: 2,
				Schema: `{
					"type":"record",
					"name":"Schema",
					"namespace":"io.confluent.kafka.avro",
					"fields":[{"name":"key","type":"string"}]}`,
				Version: 1,
				Subject: "avro-schema",
			},
			SchemaType: srclient.Avro,
		},
	}

	for _, container := range containers {
		// Test with a schema registry, which fails and manually (de)serializes the data.
		serialized := test.module.Kafka.serialize(container)
		assert.NotNil(t, serialized)
		// 4 bytes for magic byte, 1 byte for schema ID, and the rest is the data.
		assert.GreaterOrEqual(t, len(serialized), 5)

		// Send data to Kafka.
		test.module.Kafka.produce(writer, &ProduceConfig{
			Messages: []Message{
				{
					Value: serialized,
				},
			},
		})

		// Read data from Kafka.
		messages := test.module.Kafka.consume(reader, &ConsumeConfig{
			Limit: 1,
		})
		assert.Equal(t, 1, len(messages))

		if value, ok := messages[0]["value"].(map[string]interface{}); ok {
			// Deserialize the key or value (removes the magic bytes).
			deserialized := test.module.Kafka.deserialize(&Container{
				Data:       value,
				Schema:     container.Schema,
				SchemaType: container.SchemaType,
			})
			assert.Equal(t, container.Data, deserialized)
		}
	}
}

type TestDataContainer struct {
	container *Container
	err       *Xk6KafkaError
}

func TestSerializeFails(t *testing.T) {
	test := getTestModuleInstance(t)

	testDataContainer := []TestDataContainer{
		{
			container: &Container{
				Data:       "string",
				SchemaType: "invalid-schema-type",
			},
			err: ErrUnknownSerdesType,
		},
		{
			container: &Container{
				Data:       1.1,
				SchemaType: String,
			},
			err: ErrInvalidDataType,
		},
		{
			container: &Container{
				Data:       []interface{}{"test"},
				SchemaType: Bytes,
			},
			err: ErrFailedTypeCast,
		},
		{
			container: &Container{
				Data:       "test",
				SchemaType: Bytes,
			},
			err: ErrInvalidDataType,
		},
		{
			container: &Container{
				Data:       map[string]interface{}{"key": unsafe.Pointer(nil)},
				SchemaType: srclient.Json,
			},
			err: ErrInvalidDataType,
		},
		{
			container: &Container{
				Data:       "test",
				SchemaType: srclient.Json,
			},
			err: ErrInvalidDataType,
		},
		{
			container: &Container{
				Data: map[string]interface{}{"key": "value"},
				Schema: &Schema{
					ID:      1,
					Schema:  `{`, // Invalid JSONSchema.
					Version: 1,
					Subject: "json-schema",
				},
				SchemaType: srclient.Json,
			},
			err: ErrInvalidSchema,
		},
		{
			container: &Container{
				Data:       `{"key": "value"}`,
				SchemaType: srclient.Avro,
			},
			err: ErrInvalidDataType,
		},
		{
			container: &Container{
				Data: map[string]interface{}{"unknown": "value"},
				Schema: &Schema{
					ID: 2,
					Schema: `{
						"type":"record",
						"name":"Schema",
						"namespace":"io.confluent.kafka.avro",
						"fields":[{"name":"key","type":"string"}]}`,
					Version: 1,
					Subject: "avro-schema",
				},
				SchemaType: srclient.Avro,
			},
			err: NewXk6KafkaError(failedToEncode, "Failed to encode data", errors.New("cannot decode textual record \"io.confluent.kafka.avro.Schema\": cannot decode textual map: cannot determine codec: \"unknown\"")),
		},
		{
			container: &Container{
				Data: map[string]interface{}{"key": unsafe.Pointer(nil)},
				Schema: &Schema{
					ID: 2,
					Schema: `{
						"type":"record",
						"name":"Schema",
						"namespace":"io.confluent.kafka.avro",
						"fields":[{"name":"key","type":"string"}]}`,
					Version: 1,
					Subject: "avro-schema",
				},
				SchemaType: srclient.Avro,
			},
			err: ErrInvalidDataType,
		},
	}

	for _, testData := range testDataContainer {
		t.Run("serialize fails", func(t *testing.T) {
			defer func(t *testing.T) {
				err := recover()
				assert.Equal(t,
					err.(*sobek.Object).ToString().String(),
					GoErrorPrefix+testData.err.Error())
			}(t)

			err := test.module.Kafka.serialize(testData.container)
			assert.Equal(t, err, testData.err)
		})
	}
}
