package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSerializeString(t *testing.T) {
	originalData := "test"
	result, err := SerializeString(Configuration{}, "", originalData, "", "", 0)
	assert.Nil(t, err)
	assert.Equal(t, []byte(originalData), result)
}

func TestSerializeStringFails(t *testing.T) {
	originalData := 123
	_, err := SerializeString(Configuration{}, "", originalData, "", "", 0)
	assert.EqualErrorf(
		t, err, "Invalid data type provided for string serializer (requires string)",
		"Expected error message is correct")
}

func TestDeserializeString(t *testing.T) {
	originalData := "test"
	result, err := DeserializeString(Configuration{}, []byte(originalData), "", "", 0)
	assert.Equal(t, originalData, result)
	assert.Nil(t, err)
}
