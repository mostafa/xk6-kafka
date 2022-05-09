package kafka

import "errors"

func SerializeByteArray(configuration Configuration, topic string, data interface{}, element Element, schema string, version int) ([]byte, error) {
	switch data.(type) {
	case []interface{}:
		bArray := data.([]interface{})
		arr := make([]byte, len(bArray))
		for i, u := range bArray {
			arr[i] = byte(u.(int64))
		}
		return arr, nil
	default:
		return nil, errors.New("Invalid data type provided for byte array serializer (requires []byte)")
	}
}

func DeserializeByteArray(configuration Configuration, data []byte, element Element, schema string, version int) interface{} {
	return data
}
