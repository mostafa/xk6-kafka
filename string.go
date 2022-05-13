package kafka

import "fmt"

func SerializeString(configuration Configuration, topic string, data interface{}, element Element, schema string, version int) ([]byte, *Xk6KafkaError) {
	switch data := data.(type) {
	case string:
		return []byte(data), nil
	default:
		return nil, NewXk6KafkaError(
			invalidDataType,
			"Invalid data type provided for string serializer (requires string)",
			fmt.Errorf("Expected: string, got: %T", data))
	}
}

func DeserializeString(configuration Configuration, topic string, data []byte, element Element, schema string, version int) (interface{}, *Xk6KafkaError) {
	return string(data), nil
}
