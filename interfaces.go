package kafka

import (
	"github.com/riferrei/srclient"
)

type Serdes interface {
	Serialize(data interface{}, schema *Schema) ([]byte, *Xk6KafkaError)
	Deserialize(data []byte, schema *Schema) (interface{}, *Xk6KafkaError)
}

var TypesRegistry map[srclient.SchemaType]Serdes = map[srclient.SchemaType]Serdes{
	String:        &StringSerde{},
	Bytes:         &ByteArraySerde{},
	srclient.Json: &JSONSerde{},
	srclient.Avro: &AvroSerde{},
}

func GetSerdes(schemaType srclient.SchemaType) (Serdes, *Xk6KafkaError) {
	if serdes, ok := TypesRegistry[schemaType]; ok {
		return serdes, nil
	} else {
		return nil, ErrUnknownSerdesType
	}
}
