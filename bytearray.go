package kafka

import "github.com/riferrei/srclient"

type ByteArraySerde struct {
	Serdes
}

const (
	Bytes srclient.SchemaType = "BYTES"
)

// Serialize serializes the given data into a byte array.
func (*ByteArraySerde) Serialize(data interface{}, schema *Schema) ([]byte, *Xk6KafkaError) {
	switch data := data.(type) {
	case []byte:
		return data, nil
	case []interface{}:
		arr := make([]byte, len(data))
		for i, u := range data {
			if u, ok := u.(float64); ok {
				arr[i] = byte(u)
			} else {
				return nil, ErrFailedTypeCast
			}
		}
		return arr, nil
	default:
		return nil, ErrInvalidDataType
	}
}

// DeserializeByteArray returns the data as-is, because it is already a byte array.
func (*ByteArraySerde) Deserialize(data []byte, schema *Schema) (interface{}, *Xk6KafkaError) {
	return data, nil
}
