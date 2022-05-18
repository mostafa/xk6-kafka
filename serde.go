package kafka

import (
	"reflect"

	"github.com/riferrei/srclient"
)

type Serializer func(configuration Configuration, topic string, data interface{}, element Element, schema string, version int) ([]byte, *Xk6KafkaError)
type Deserializer func(configuration Configuration, topic string, data []byte, element Element, schema string, version int) (interface{}, *Xk6KafkaError)

const (
	// TODO: move these to their own package
	ProtobufSerializer   string = "io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer"
	ProtobufDeserializer string = "io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer"
)

func useSerializer(configuration Configuration, element Element) bool {
	if reflect.ValueOf(configuration).IsZero() || reflect.ValueOf(configuration.Producer).IsZero() {
		return false
	}

	if (element == Key && configuration.Producer.KeySerializer != "") || (element == Value && configuration.Producer.ValueSerializer != "") {
		return true
	}

	return false
}

func useDeserializer(configuration Configuration, element Element) bool {
	if reflect.ValueOf(configuration).IsZero() || reflect.ValueOf(configuration.Consumer).IsZero() {
		return false
	}

	if (element == Key && configuration.Consumer.KeyDeserializer != "") || (element == Value && configuration.Consumer.ValueDeserializer != "") {
		return true
	}

	return false
}

type SerdeType[T Serializer | Deserializer] struct {
	Function      T
	Class         string
	SchemaType    srclient.SchemaType
	WireFormatted bool
}

func NewSerdes[T Serializer | Deserializer](function T, class string, schemaType srclient.SchemaType, wireFormatted bool) *SerdeType[T] {
	return &SerdeType[T]{function, class, schemaType, wireFormatted}
}

func (s *SerdeType[Serializer]) GetSerializer() Serializer {
	return s.Function
}

func (s *SerdeType[Deserializer]) GetDeserializer() Deserializer {
	return s.Function
}

func (s *SerdeType[T]) GetSchemaType() srclient.SchemaType {
	return s.SchemaType
}

func (s *SerdeType[T]) IsWireFormatted() bool {
	return s.WireFormatted
}
