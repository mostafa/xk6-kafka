package kafka

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

// TestSerializeJSON tests the serialization of a JSON object.
func TestSerializeJSON(t *testing.T) {
	jsonSerde := &JSONSerde{}
	expected := []byte(`{"key":"value"}`)
	actual, err := jsonSerde.Serialize(map[string]interface{}{"key": "value"}, nil)
	assert.Nil(t, err)
	assert.Equal(t, expected, actual)
}

// TestSerializeJSONWithSchema tests the serialization of a JSON object with a schema.
func TestSerializeJSONWithSchema(t *testing.T) {
	jsonSerde := &JSONSerde{}
	expected := []byte(`{"key":"value"}`)
	schema := &Schema{
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
	}
	actual, err := jsonSerde.Serialize(map[string]interface{}{"key": "value"}, schema)
	assert.Nil(t, err)
	assert.Equal(t, expected, actual)
}

// TestSerializeJSONFailsOnInvalidSchema tests the serialization of a JSON object
// and fails on an invalid schema.
func TestSerializeJSONFailsOnInvalidSchema(t *testing.T) {
	jsonSerde := &JSONSerde{}
	schema := &Schema{
		ID:      1,
		Schema:  `{`,
		Version: 1,
		Subject: "json-schema",
	}
	actual, err := jsonSerde.Serialize(map[string]interface{}{"value": "key"}, schema)
	assert.Nil(t, actual)
	assert.NotNil(t, err)
	assert.Equal(t, ErrInvalidSchema, err)
}

// TestSerializeJSONFailsOnValidation tests the serialization of a JSON object
// and fails on schema validation errors.
func TestSerializeJSONFailsOnValidation(t *testing.T) {
	jsonSerde := &JSONSerde{}
	schema := &Schema{
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
	}
	actual, err := jsonSerde.Serialize(map[string]interface{}{"value": "key"}, schema)
	assert.Nil(t, actual)
	assert.NotNil(t, err)
	assert.Equal(t, "Failed to validate JSON against schema", err.Message)
	assert.Equal(t, failedValidateJSON, err.Code)
}

// TestSerializeJSONFailsOnBadType tests the serialization of a JSON object
// and fails on nested invalid data type.
func TestSerializeJSONFailsOnBadType(t *testing.T) {
	jsonSerde := &JSONSerde{}
	actual, err := jsonSerde.Serialize(map[string]interface{}{"key": unsafe.Pointer(nil)}, nil)
	assert.Nil(t, actual)
	assert.NotNil(t, err)
	assert.Equal(t, ErrInvalidDataType, err)
}

// TestSerializeJSONFailsOnInvalidDataType tests the serialization of a JSON object
// and fails on invalid data type.
func TestSerializeJSONFailsOnInvalidDataType(t *testing.T) {
	jsonSerde := &JSONSerde{}
	actual, err := jsonSerde.Serialize(1, nil)
	assert.Nil(t, actual)
	assert.NotNil(t, err)
	assert.Equal(t, ErrInvalidDataType, err)
}
