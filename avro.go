package kafka

import (
	"github.com/linkedin/goavro/v2"
)

func SerializeAvro(configuration Configuration, topic string, data interface{}, element Element, schema string, version int) ([]byte, error) {
	bytesData := []byte(data.(string))
	if schema != "" {
		codec, err := goavro.NewCodec(schema)
		if err != nil {
			ReportError(err, "Failed to create codec for encoding Avro")
		}

		avroEncodedData, _, err := codec.NativeFromTextual(bytesData)
		if err != nil {
			ReportError(err, "Failed to encode data into Avro")
		}

		bytesData, err = codec.BinaryFromNative(nil, avroEncodedData)
		if err != nil {
			ReportError(err, "Failed to encode Avro data into binary")
		}
	}

	byteData, err := encodeWireFormat(configuration, bytesData, topic, element, schema, version)
	if err != nil {
		ReportError(err, "Failed to add wire format to the binary data")
		return nil, err
	}

	return byteData, nil
}

func DeserializeAvro(configuration Configuration, data []byte, element Element, schema string, version int) interface{} {
	bytesDecodedData, err := decodeWireFormat(configuration, data, element)
	if err != nil {
		ReportError(err, "Failed to remove wire format from the binary data")
		return nil
	}

	if schema != "" {
		codec, err := goavro.NewCodec(schema)
		if err != nil {
			ReportError(err, "Failed to create codec for decoding Avro")
		}

		avroDecodedData, _, err := codec.NativeFromBinary(bytesDecodedData)
		if err != nil {
			ReportError(err, "Failed to decode data from Avro")
		}

		return avroDecodedData
	}

	return bytesDecodedData
}
