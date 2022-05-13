package kafka

import "fmt"

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

func DeserializeByteArray(configuration Configuration, data []byte, element Element, schema string, version int) (interface{}, *Xk6KafkaError) {
	return data, nil
}
