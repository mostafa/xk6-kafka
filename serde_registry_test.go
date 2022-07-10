package kafka

import (
	"reflect"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
)

// getFuncName returns the name of the function as string.
func getFuncName(function interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(function).Pointer()).Name()
}

// TestSerializersRegistry tests the serdes registry.
func TestSerdesRegistry(t *testing.T) {
	serializersRegistry := NewSerializersRegistry()
	deserializersRegistry := NewDeserializersRegistry()

	assert.Equal(t, 5, len(serializersRegistry.Registry))
	assert.Equal(t, String, serializersRegistry.Registry[StringSerializer].GetSchemaType())
	assert.Equal(t, "github.com/mostafa/xk6-kafka.SerializeString",
		getFuncName(serializersRegistry.Registry[StringSerializer].GetSerializer()))
	assert.False(t, serializersRegistry.Registry[StringSerializer].IsWireFormatted())

	assert.Equal(t, 5, len(deserializersRegistry.Registry))
	assert.Equal(t, "github.com/mostafa/xk6-kafka.DeserializeString",
		getFuncName(deserializersRegistry.Registry[StringDeserializer].GetDeserializer()))
	assert.False(t, deserializersRegistry.Registry[StringDeserializer].IsWireFormatted())
}
