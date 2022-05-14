package kafka

import (
	"fmt"

	"github.com/riferrei/srclient"
)

const (
	ByteArray srclient.SchemaType = "BYTEARRAY"

	ByteArraySerializer   string = "org.apache.kafka.common.serialization.ByteArraySerializer"
	ByteArrayDeserializer string = "org.apache.kafka.common.serialization.ByteArrayDeserializer"
)

func SerializeByteArray(configuration Configuration, topic string, data interface{}, element Element, schema string, version int) ([]byte, *Xk6KafkaError) {
	switch data.(type) {
	case []interface{}:
		bArray := data.([]interface{})
		arr := make([]byte, len(bArray))
		for i, u := range bArray {
			arr[i] = byte(u.(int64))
		}
		return arr, nil
	default:
		return nil, NewXk6KafkaError(
			invalidDataType,
			"Invalid data type provided for byte array serializer (requires []byte)",
			fmt.Errorf("Expected: []byte, got: %T", data))
	}
}

func DeserializeByteArray(configuration Configuration, topic string, data []byte, element Element, schema string, version int) (interface{}, *Xk6KafkaError) {
	return data, nil
}
