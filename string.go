package kafka

import "errors"

func SerializeString(configuration Configuration, topic string, data interface{}, keyOrValue string, schema string) ([]byte, error) {
	switch data.(type) {
	case string:
		return []byte(data.(string)), nil
	default:
		return nil, errors.New("Invalid data type provided for string serializer (requires string)")
	}
}

func DeserializeString(configuration Configuration, data []byte, keyOrValue string, schema string) interface{} {
	return string(data)
}
