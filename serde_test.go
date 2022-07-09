package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestUseSerializer tests whether a serializer should be used based on the configuration
func TestUseSerializer(t *testing.T) {
	config := Configuration{
		Producer: ProducerConfiguration{
			ValueSerializer: AvroSerializer,
			KeySerializer:   AvroSerializer,
		},
	}

	assert.True(t, useSerializer(config, Key))
	assert.True(t, useSerializer(config, Value))
}

type UseSerializerDeserializerTest struct {
	config  Configuration
	element Element
	result  bool
}

// TestUseSerializerFails tests whether a serializer should be used based on the configuration
// and fails if the configuration is invalid.
func TestUseSerializerFails(t *testing.T) {
	params := []UseSerializerDeserializerTest{
		{config: Configuration{}, element: Key, result: false},
		{config: Configuration{}, element: Value, result: false},
		{config: Configuration{Producer: ProducerConfiguration{}}, element: Key, result: false},
		{config: Configuration{Producer: ProducerConfiguration{}}, element: Value, result: false},
		{config: Configuration{Consumer: ConsumerConfiguration{}}, element: Key, result: false},
		{config: Configuration{Consumer: ConsumerConfiguration{}}, element: Value, result: false},
		{config: Configuration{SchemaRegistry: SchemaRegistryConfiguration{}}, element: Key, result: false},
		{config: Configuration{SchemaRegistry: SchemaRegistryConfiguration{}}, element: Value, result: false},
		{config: Configuration{Producer: ProducerConfiguration{ValueSerializer: "unknown codec"}}, element: Key, result: false},
		{config: Configuration{Producer: ProducerConfiguration{KeySerializer: "unknown codec"}}, element: Value, result: false},
	}

	for _, param := range params {
		assert.Equal(t, param.result, useSerializer(param.config, param.element))
		assert.Equal(t, param.result, useDeserializer(param.config, param.element))
	}
}

// TestUseDeserializer tests whether a deserializer should be used based on the configuration
func TestUseDeserializer(t *testing.T) {
	config := Configuration{
		Consumer: ConsumerConfiguration{
			ValueDeserializer: AvroDeserializer,
			KeyDeserializer:   AvroDeserializer,
		},
	}

	assert.True(t, useDeserializer(config, Key))
	assert.True(t, useDeserializer(config, Value))
}
