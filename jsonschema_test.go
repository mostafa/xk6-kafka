package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	jsonConfig Configuration = Configuration{
		Producer: ProducerConfiguration{
			ValueSerializer: JsonSchemaSerializer,
			KeySerializer:   JsonSchemaSerializer,
		},
		Consumer: ConsumerConfiguration{
			ValueDeserializer: JsonSchemaDeserializer,
			KeyDeserializer:   JsonSchemaDeserializer,
		},
	}
	jsonSchema string = `{"type":"object","title":"Key","properties":{"field": {"type":"string"}},"required":["field"]}`
)

func TestSerializeDeserializeJson(t *testing.T) {
	// Test with a schema registry, which fails and manually (de)serializes the data
	for _, element := range []Element{Key, Value} {
		// Serialize the key or value
		serialized, err := SerializeJson(jsonConfig, "topic", `{"field":"value"}`, element, jsonSchema, 0)
		assert.Nil(t, err)
		assert.NotNil(t, serialized)
		// 4 bytes for magic byte, 1 byte for schema ID, and the rest is the data
		assert.GreaterOrEqual(t, len(serialized), 10)

		// Deserialize the key or value (removes the magic bytes)
		deserialized, err := DeserializeJson(jsonConfig, "topic", serialized, element, jsonSchema, 0)
		assert.Nil(t, err)
		assert.Equal(t, map[string]interface{}{"field": "value"}, deserialized)
	}
}

func TestSerializeDeserializeJsonFailsOnSchemaError(t *testing.T) {
	schema := `{`

	for _, element := range []Element{Key, Value} {
		// Serialize the key or value
		serialized, err := SerializeJson(jsonConfig, "topic", `{"field":"value"}`, element, schema, 0)
		assert.Nil(t, serialized)
		assert.Error(t, err.Unwrap())
		assert.Equal(t, "Failed to create codec for encoding JSON", err.Message)
		assert.Equal(t, failedCreateJsonSchemaCodec, err.Code)

		// Deserialize the key or value
		deserialized, err := DeserializeJson(jsonConfig, "topic", []byte{1, 2, 3, 4, 5, 6}, element, schema, 0)
		assert.Nil(t, deserialized)
		assert.Error(t, err.Unwrap())
		assert.Equal(t, "Failed to create codec for decoding JSON data", err.Message)
		assert.Equal(t, failedCreateJsonSchemaCodec, err.Code)
	}
}

func TestSerializeDeserializeJsonFailsOnWireFormatError(t *testing.T) {
	schema := `{}`

	for _, element := range []Element{Key, Value} {
		// Deserialize an empty key or value
		deserialized, err := DeserializeJson(jsonConfig, "topic", []byte{}, element, schema, 0)
		assert.Nil(t, deserialized)
		assert.Error(t, err.Unwrap())
		assert.Equal(t, "Failed to remove wire format from the binary data", err.Message)
		assert.Equal(t, failedDecodeFromWireFormat, err.Code)

		// Deserialize a broken key or value
		// Proper wire-formatted message has 5 bytes (the wire format) plus data
		deserialized, err = DeserializeJson(jsonConfig, "topic", []byte{1, 2, 3, 4}, element, schema, 0)
		assert.Nil(t, deserialized)
		assert.Error(t, err.Unwrap())
		assert.Equal(t, "Failed to remove wire format from the binary data", err.Message)
		assert.Equal(t, failedDecodeFromWireFormat, err.Code)
	}
}

func TestSerializeDeserializeJsonFailsOnMarshalError(t *testing.T) {
	data := `{"nonExistingField":"`

	for _, element := range []Element{Key, Value} {
		serialized, err := SerializeJson(jsonConfig, "topic", data, element, jsonSchema, 0)
		assert.Nil(t, serialized)
		assert.Error(t, err.Unwrap())
		assert.Equal(t, "Failed to unmarshal JSON data", err.Message)
		assert.Equal(t, failedUnmarshalJson, err.Code)

		deserialized, err := DeserializeJson(jsonConfig, "topic", []byte{1, 2, 3, 4, 5, 6}, element, jsonSchema, 0)
		assert.Nil(t, deserialized)
		assert.Error(t, err.Unwrap())
		assert.Equal(t, "Failed to unmarshal JSON data", err.Message)
		assert.Equal(t, failedUnmarshalJson, err.Code)
	}
}

func TestSerializeDeserializeJsonFailsOnValidationError(t *testing.T) {
	// JSON schema validation fails, but the data is still returned
	data := `{"nonExistingField":"value"}`

	for _, element := range []Element{Key, Value} {
		serialized, err := SerializeJson(jsonConfig, "topic", data, element, jsonSchema, 0)
		assert.Nil(t, err)
		assert.NotNil(t, serialized)
		// 4 bytes for magic byte, 1 byte for schema ID, and the rest is the data
		assert.GreaterOrEqual(t, len(serialized), 28)
	}
}
