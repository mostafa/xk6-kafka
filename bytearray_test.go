package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSerializeByteArray(t *testing.T) {
	byteArraySerde := &ByteArraySerde{}
	expected := []byte{1, 2, 3}
	actual, err := byteArraySerde.Serialize([]byte{1, 2, 3}, nil)
	assert.Nil(t, err)
	assert.Equal(t, expected, actual)
}

func TestSerializeInterfaceArray(t *testing.T) {
	byteArraySerde := &ByteArraySerde{}
	expected := []byte{0x41, 0x42, 0x43}
	actual, err := byteArraySerde.Serialize([]interface{}{65.0, 66.0, 67.0}, nil)
	assert.Nil(t, err)
	assert.Equal(t, expected, actual)
}

func TestSerializeInterfaceArrayFails(t *testing.T) {
	byteArraySerde := &ByteArraySerde{}
	actual, err := byteArraySerde.Serialize([]interface{}{65.0, 66.0, "a"}, nil)
	assert.Nil(t, actual)
	assert.NotNil(t, err)
	assert.Equal(t, ErrFailedTypeCast, err)
}

func TestSerializeByteArrayFails(t *testing.T) {
	byteArraySerde := &ByteArraySerde{}
	actual, err := byteArraySerde.Serialize(1, nil)
	assert.Nil(t, actual)
	assert.NotNil(t, err)
	assert.Equal(t, ErrInvalidDataType, err)
}

func TestDeserializeByteArray(t *testing.T) {
	byteArraySerde := &ByteArraySerde{}
	expected := []byte{1, 2, 3}
	actual, err := byteArraySerde.Deserialize([]byte{1, 2, 3}, nil)
	assert.Nil(t, err)
	assert.Equal(t, expected, actual)
}
