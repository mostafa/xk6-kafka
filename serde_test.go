package kafka

import (
	"reflect"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUseSerializer(t *testing.T) {
	config := Configuration{
		Producer: ProducerConfiguration{
			ValueSerializer: "io.confluent.kafka.serializers.KafkaAvroSerializer",
			KeySerializer:   "io.confluent.kafka.serializers.KafkaAvroSerializer",
		},
	}

	assert.True(t, useSerializer(config, Key))
	assert.True(t, useSerializer(config, Value))
}

type UseSerializerTest struct {
	config  Configuration
	element Element
	result  bool
}

func TestUseSerializerFails(t *testing.T) {
	params := []UseSerializerTest{
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
	}
}

func TestUseDeserializer(t *testing.T) {
	config := Configuration{
		Consumer: ConsumerConfiguration{
			ValueDeserializer: "io.confluent.kafka.serializers.KafkaAvroDeserializer",
			KeyDeserializer:   "io.confluent.kafka.serializers.KafkaAvroDeserializer",
		},
	}

	assert.True(t, useDeserializer(config, Key))
	assert.True(t, useDeserializer(config, Value))
}

type UseDeserializerTest struct {
	config  Configuration
	element Element
	result  bool
}

func TestUseDeserializerFails(t *testing.T) {
	params := []UseDeserializerTest{
		{config: Configuration{}, element: Key, result: false},
		{config: Configuration{}, element: Value, result: false},
		{config: Configuration{Producer: ProducerConfiguration{}}, element: Key, result: false},
		{config: Configuration{Producer: ProducerConfiguration{}}, element: Value, result: false},
		{config: Configuration{Consumer: ConsumerConfiguration{}}, element: Key, result: false},
		{config: Configuration{Consumer: ConsumerConfiguration{}}, element: Value, result: false},
		{config: Configuration{SchemaRegistry: SchemaRegistryConfiguration{}}, element: Key, result: false},
		{config: Configuration{SchemaRegistry: SchemaRegistryConfiguration{}}, element: Value, result: false},
		{config: Configuration{Consumer: ConsumerConfiguration{ValueDeserializer: "unknown codec"}}, element: Key, result: false},
		{config: Configuration{Consumer: ConsumerConfiguration{KeyDeserializer: "unknown codec"}}, element: Value, result: false},
	}

	for _, param := range params {
		assert.Equal(t, param.result, useDeserializer(param.config, param.element))
	}
}

func TestIsWireFormatted(t *testing.T) {
	wireFormattedCodecs := []string{
		"io.confluent.kafka.serializers.KafkaAvroSerializer",
		"io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer",
		"io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer",
		"io.confluent.kafka.serializers.KafkaAvroDeserializer",
		"io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer",
		"io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer",
	}

	for _, codec := range wireFormattedCodecs {
		assert.True(t, isWireFormatted(codec))
	}
}

func TestIsNotWireFormatted(t *testing.T) {
	notWireFormattedCodecs := []string{
		"unknown codec",
		"",
		"org.apache.kafka.common.serialization.StringSerializer",
		"org.apache.kafka.common.serialization.StringDeserializer",
		"org.apache.kafka.common.serialization.ByteArraySerializer",
		"org.apache.kafka.common.serialization.ByteArrayDeserializer",
	}

	for _, codec := range notWireFormattedCodecs {
		assert.False(t, isWireFormatted(codec))
	}
}

type SerializerFunctionsTest struct {
	serializer         string
	serializerFuncName string
}

func getFuncName(f interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
}

func TestGetSerializer(t *testing.T) {
	serializers := []SerializerFunctionsTest{
		{serializer: "org.apache.kafka.common.serialization.StringSerializer",
			serializerFuncName: "github.com/mostafa/xk6-kafka.SerializeString"},
		{serializer: "org.apache.kafka.common.serialization.ByteArraySerializer",
			serializerFuncName: "github.com/mostafa/xk6-kafka.SerializeByteArray"},
		{serializer: "io.confluent.kafka.serializers.KafkaAvroSerializer",
			serializerFuncName: "github.com/mostafa/xk6-kafka.SerializeAvro"},
		// Protobuf serializer is not supported yet, so the default serializer is used
		{serializer: "io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer",
			serializerFuncName: "github.com/mostafa/xk6-kafka.SerializeString"},
		{serializer: "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer",
			serializerFuncName: "github.com/mostafa/xk6-kafka.SerializeJsonSchema"},
		// Missing or invalid serializer results in the default serializer
		{serializer: "", serializerFuncName: "github.com/mostafa/xk6-kafka.SerializeString"},
		{serializer: "unknown codec", serializerFuncName: "github.com/mostafa/xk6-kafka.SerializeString"},
	}

	for _, serializer := range serializers {
		assert.Equal(t,
			serializer.serializerFuncName,
			getFuncName(GetSerializer(serializer.serializer)))
	}
}

func TestGetDeserializer(t *testing.T) {
	deserializers := []SerializerFunctionsTest{
		{serializer: "org.apache.kafka.common.serialization.StringDeserializer",
			serializerFuncName: "github.com/mostafa/xk6-kafka.DeserializeString"},
		{serializer: "org.apache.kafka.common.serialization.ByteArrayDeserializer",
			serializerFuncName: "github.com/mostafa/xk6-kafka.DeserializeByteArray"},
		{serializer: "io.confluent.kafka.serializers.KafkaAvroDeserializer",
			serializerFuncName: "github.com/mostafa/xk6-kafka.DeserializeAvro"},
		// Protobuf deserializer is not supported yet, so the default deserializer is used
		{serializer: "io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer",
			serializerFuncName: "github.com/mostafa/xk6-kafka.DeserializeString"},
		{serializer: "io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer",
			serializerFuncName: "github.com/mostafa/xk6-kafka.DeserializeJsonSchema"},
		// Missing or invalid deserializer results in the default deserializer
		{serializer: "", serializerFuncName: "github.com/mostafa/xk6-kafka.DeserializeString"},
		{serializer: "unknown codec", serializerFuncName: "github.com/mostafa/xk6-kafka.DeserializeString"},
	}

	for _, deserializer := range deserializers {
		assert.Equal(t,
			deserializer.serializerFuncName,
			getFuncName(GetDeserializer(deserializer.serializer)))
	}
}
