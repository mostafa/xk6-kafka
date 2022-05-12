package kafka

import (
	"github.com/riferrei/srclient"
)

type Serializer func(configuration Configuration, topic string, data interface{}, element Element, schema string, version int) ([]byte, error)
type Deserializer func(configuration Configuration, data []byte, element Element, schema string, version int) interface{}

var (
	// TODO: Find a better way to do this, like serde registry or something
	Serializers = map[string]Serializer{
		"org.apache.kafka.common.serialization.StringSerializer":          SerializeString,
		"org.apache.kafka.common.serialization.ByteArraySerializer":       SerializeByteArray,
		"io.confluent.kafka.serializers.KafkaAvroSerializer":              SerializeAvro,
		"io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer": nil,
		"io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer":   SerializeJsonSchema,
	}

	Deserializers = map[string]Deserializer{
		"org.apache.kafka.common.serialization.StringDeserializer":          DeserializeString,
		"org.apache.kafka.common.serialization.ByteArrayDeserializer":       DeserializeByteArray,
		"io.confluent.kafka.serializers.KafkaAvroDeserializer":              DeserializeAvro,
		"io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer": nil,
		"io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer":   DeserializeJsonSchema,
	}

	WireFormattedCodecs = map[string]bool{
		// Serializers
		"org.apache.kafka.common.serialization.StringSerializer":          false,
		"org.apache.kafka.common.serialization.ByteArraySerializer":       false,
		"io.confluent.kafka.serializers.KafkaAvroSerializer":              true,
		"io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer": true,
		"io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer":   true,

		// Deserializers
		"org.apache.kafka.common.serialization.StringDeserializer":          false,
		"org.apache.kafka.common.serialization.ByteArrayDeserializer":       false,
		"io.confluent.kafka.serializers.KafkaAvroDeserializer":              true,
		"io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer": true,
		"io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer":   true,
	}

	SchemaTypes = map[string]srclient.SchemaType{
		"io.confluent.kafka.serializers.KafkaAvroSerializer":                srclient.Avro,
		"io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer":   srclient.Protobuf,
		"io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer":     srclient.Json,
		"io.confluent.kafka.serializers.KafkaAvroDeserializer":              srclient.Avro,
		"io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer": srclient.Protobuf,
		"io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer":   srclient.Json,
	}
)

func useSerializer(configuration Configuration, element Element) bool {
	// TODO: Refactor this
	if (Configuration{}) != configuration || (ProducerConfiguration{}) != configuration.Producer &&
		(element == Key && configuration.Producer.KeySerializer != "") || (element == Value && configuration.Producer.ValueSerializer != "") {
		return true
	}
	return false
}

func useDeserializer(configuration Configuration, element Element) bool {
	// TODO: Refactor this
	if (Configuration{}) != configuration || (ConsumerConfiguration{}) != configuration.Consumer &&
		(element == Key && configuration.Consumer.KeyDeserializer != "") || (element == Value && configuration.Consumer.ValueDeserializer != "") {
		return true
	}
	return false
}

func isWireFormatted(serde string) bool {
	return WireFormattedCodecs[serde]
}

func GetSerializer(serializer string, schema string) Serializer {
	serializerFunction := Serializers[serializer]
	if serializerFunction == nil {
		return SerializeString
	}
	return serializerFunction
}

func GetDeserializer(deserializer string, schema string) Deserializer {
	deserializerFunction := Deserializers[deserializer]
	if deserializerFunction == nil {
		return DeserializeString
	}
	return deserializerFunction
}

func GetSchemaType(serializer string) srclient.SchemaType {
	return SchemaTypes[serializer]
}
