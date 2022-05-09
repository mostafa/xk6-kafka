package kafka

import "errors"

func SerializeString(configuration Configuration, topic string, data interface{}, element Element, schema string, version int) ([]byte, error) {
	switch data := data.(type) {
	case string:
		return []byte(data), nil
	default:
		return nil, errors.New("Invalid data type provided for string serializer (requires string)")
	}
}

func DeserializeString(configuration Configuration, data []byte, element Element, schema string, version int) interface{} {
	return string(data)
}
