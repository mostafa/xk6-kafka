package kafka

import (
	"github.com/riferrei/srclient"
)

const (
	String srclient.SchemaType = "STRING"

	StringSerializer   string = "org.apache.kafka.common.serialization.StringSerializer"
	StringDeserializer string = "org.apache.kafka.common.serialization.StringDeserializer"
)

// SerializeString serializes a string to bytes.
func SerializeString(
	configuration Configuration, topic string, data interface{},
	element Element, schema string, version int) ([]byte, *Xk6KafkaError) {
	switch data := data.(type) {
	case string:
		return []byte(data), nil
	default:
		return nil, ErrorInvalidDataType
	}
}

// DeserializeString deserializes a string from bytes.
func DeserializeString(
	configuration Configuration, topic string, data []byte,
	element Element, schema string, version int) (interface{}, *Xk6KafkaError) {
	return string(data), nil
}
