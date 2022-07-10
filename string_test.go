package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestSerializeString tests the serialization of a string
func TestSerializeString(t *testing.T) {
	result, err := SerializeString(Configuration{}, "", originalData, "", "", 0)
	assert.Nil(t, err)
	assert.Equal(t, []byte(originalData), result)
}

// TestSerializeStringFails tests the serialization of a string and
// fails if the given type is not string.
func TestSerializeStringFails(t *testing.T) {
	originalData := 123
	_, err := SerializeString(Configuration{}, "", originalData, "", "", 0)
	assert.EqualErrorf(
		t, err, "Invalid data type provided for string serializer (requires string), OriginalError: %!w(*errors.errorString=&{Expected: string, got: int})",
		"Expected error message is correct")
}

// TestDeserializeString tests the deserialization of a string
func TestDeserializeString(t *testing.T) {
	result, err := DeserializeString(Configuration{}, "", []byte(originalData), "", "", 0)
	assert.Equal(t, originalData, result)
	assert.Nil(t, err)
}
