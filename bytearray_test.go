package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	originalData string = "test"
)

// TestSerializeByteArray tests the serialization of a byte array into binary.
func TestSerializeByteArray(t *testing.T) {
	var data float64 = 98
	originalData := []interface{}{data}
	result, err := SerializeByteArray(Configuration{}, "", originalData, "", "", 0)
	assert.Nil(t, err)
	assert.Equal(t, []byte{0x62}, result)
}

// TestSerializeByteArrayFails tests the serialization of a byte array into binary and fails
// on invalid data type.
func TestSerializeByteArrayFails(t *testing.T) {
	_, err := SerializeByteArray(Configuration{}, "", originalData, "", "", 0)
	assert.NotNil(t, err)
	assert.Equal(t, err.Message, "Invalid data type provided for byte array serializer (requires []byte)")
	assert.Equal(t, err.Code, invalidDataType)
}

// TestDeserializeByteArray tests the deserialization of a byte array into binary.
func TestDeserializeByteArray(t *testing.T) {
	originalData := []byte{1, 2, 3}
	result, err := DeserializeByteArray(Configuration{}, "", originalData, "", "", 0)
	assert.Equal(t, []byte{1, 2, 3}, result)
	assert.Nil(t, err)
}
