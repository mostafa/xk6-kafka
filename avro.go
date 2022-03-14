package kafka

import (
	"log"

	"github.com/linkedin/goavro/v2"
)

func SerializeAvro(configuration Configuration, topic string, data interface{}, keyOrValue string, schema string) ([]byte, error) {
	key := []byte(data.(string))
	if schema != "" {
		key = ToAvro(data.(string), schema)
	}

	byteData, err := addMagicByteAndSchemaIdPrefix(configuration, key, topic, keyOrValue, schema)
	if err != nil {
		return nil, err
	}

	return byteData, nil
}

func ToAvro(value string, schema string) []byte {
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		log.Fatal(err)
	}

	native, _, err := codec.NativeFromTextual([]byte(value))
	if err != nil {
		log.Fatal(err)
	}

	binary, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		log.Fatal(err)
	}

	return binary
}

func DeserializeAvro(configuration Configuration, data []byte, keyOrValue string, schema string) interface{} {
	dataWithoutPrefix := removeMagicByteAndSchemaIdPrefix(configuration, data, keyOrValue)

	if schema != "" {
		return FromAvro(dataWithoutPrefix, schema)
	}

	return dataWithoutPrefix
}

func FromAvro(message []byte, schema string) interface{} {
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		log.Fatal(err)
	}

	native, _, err := codec.NativeFromBinary(message)
	if err != nil {
		log.Fatal(err)
	}

	return native
}
