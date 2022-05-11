package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSerializeByteArray(t *testing.T) {
	var data int64 = 98
	originalData := []interface{}{data}
	result, err := SerializeByteArray(Configuration{}, "", originalData, "", "", 0)
	assert.Nil(t, err)
	assert.Equal(t, []byte{0x62}, result)
}

func TestSerializeByteArrayFails(t *testing.T) {
	originalData := "test"
	_, err := SerializeByteArray(Configuration{}, "", originalData, "", "", 0)
	assert.EqualErrorf(
		t, err, "Invalid data type provided for byte array serializer (requires []byte)",
		"Expected error message is correct")
}

func TestDeserializeByteArray(t *testing.T) {
	originalData := []byte{1, 2, 3}
	result := DeserializeByteArray(Configuration{}, originalData, "", "", 0)
	assert.Equal(t, []byte{1, 2, 3}, result)
}
