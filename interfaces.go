package kafka

import (
	"github.com/riferrei/srclient"
)

type Serdes interface {
	Serialize(data interface{}, schema *Schema) ([]byte, *Xk6KafkaError)
	Deserialize(data []byte, schema *Schema) (interface{}, *Xk6KafkaError)
}

var TypesRegistry map[string]Serdes = map[string]Serdes{
	String.String():        &StringSerde{},
	Bytes.String():         &ByteArraySerde{},
	srclient.Json.String(): &JSONSerde{},
	srclient.Avro.String(): &AvroSerde{},
}

func GetSerdes(schemaType string) (Serdes, *Xk6KafkaError) {
	if serdes, ok := TypesRegistry[schemaType]; ok {
		return serdes, nil
	} else {
		return nil, ErrUnknownSerdesType
	}
}
