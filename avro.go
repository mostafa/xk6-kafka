package kafka

import (
	"github.com/linkedin/goavro/v2"
)

func SerializeAvro(configuration Configuration, topic string, data interface{}, element Element, schema string, version int) ([]byte, *Xk6KafkaError) {
	bytesData := []byte(data.(string))
	if schema != "" {
		codec, err := goavro.NewCodec(schema)
		if err != nil {
			return nil, NewXk6KafkaError(failedCreateAvroCodec,
				"Failed to create codec for encoding Avro",
				err)
		}

		avroEncodedData, _, err := codec.NativeFromTextual(bytesData)
		if err != nil {
			return nil, NewXk6KafkaError(failedEncodeToAvro,
				"Failed to encode data into Avro",
				err)
		}

		bytesData, err = codec.BinaryFromNative(nil, avroEncodedData)
		if err != nil {
			return nil, NewXk6KafkaError(failedEncodeAvroToBinary,
				"Failed to encode Avro data into binary",
				err)
		}
	}

	byteData, err := encodeWireFormat(configuration, bytesData, topic, element, schema, version)
	if err != nil {
		return nil, NewXk6KafkaError(failedEncodeToWireFormat,
			"Failed to encode data into wire format",
			err)
	}

	return byteData, nil
}

func DeserializeAvro(configuration Configuration, data []byte, element Element, schema string, version int) (interface{}, *Xk6KafkaError) {
	bytesDecodedData, err := decodeWireFormat(configuration, data, element)
	if err != nil {
		return nil, NewXk6KafkaError(failedDecodeFromWireFormat,
			"Failed to remove wire format from the binary data",
			err)
	}

	if schema != "" {
		codec, err := goavro.NewCodec(schema)
		if err != nil {
			return nil, NewXk6KafkaError(failedCreateAvroCodec,
				"Failed to create codec for decoding Avro",
				err)
		}

		avroDecodedData, _, err := codec.NativeFromBinary(bytesDecodedData)
		if err != nil {
			return nil, NewXk6KafkaError(failedDecodeAvroFromBinary,
				"Failed to decode data from Avro",
				err)
		}

		return avroDecodedData, nil
	}

	return bytesDecodedData, nil
}
