package kafka

import "github.com/riferrei/srclient"

type StringSerde struct {
	Serdes
}

const (
	String srclient.SchemaType = "STRING"
)

// Serialize serializes a string to bytes.
func (*StringSerde) Serialize(data interface{}, schema *Schema) ([]byte, *Xk6KafkaError) {
	switch data := data.(type) {
	case string:
		return []byte(data), nil
	default:
		return nil, ErrInvalidDataType
	}
}

// Deserialize deserializes a string from bytes.
func (*StringSerde) Deserialize(data []byte, schema *Schema) (interface{}, *Xk6KafkaError) {
	return string(data), nil
}
