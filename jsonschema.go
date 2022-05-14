package kafka

import (
	"encoding/json"

	"github.com/santhosh-tekuri/jsonschema/v5"
)

const (
	JsonSchemaSerializer   string = "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer"
	JsonSchemaDeserializer string = "io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer"
)

func SerializeJsonSchema(configuration Configuration, topic string, data interface{}, element Element, schema string, version int) ([]byte, *Xk6KafkaError) {
	bytesData := []byte(data.(string))
	subject := topic + "-" + string(element)
	// TODO: fix this after Avro
	if schema != "" {
		codec, err := jsonschema.CompileString(subject, schema)
		if err != nil {
			return nil, NewXk6KafkaError(failedCreateJsonSchemaCodec,
				"Failed to create codec for encoding JSONSchema",
				err)
		}

		var jsonBytes interface{}
		if err := json.Unmarshal(bytesData, &jsonBytes); err != nil {
			return nil, NewXk6KafkaError(failedUnmarshalJsonSchema,
				"Failed to unmarshal JSONSchema data",
				err)
		}

		if err := codec.Validate(jsonBytes); err != nil {
			return nil, NewXk6KafkaError(failedValidateJsonSchema,
				"Failed to validate JSONSchema data",
				err)
		}
	}

	return EncodeWireFormat(bytesData, 0), nil
}

func DeserializeJsonSchema(configuration Configuration, topic string, data []byte, element Element, schema string, version int) (interface{}, *Xk6KafkaError) {
	bytesDecodedData, err := DecodeWireFormat(data)
	// TODO: fix this after Avro
	if err != nil {
		return nil, NewXk6KafkaError(failedDecodeFromWireFormat,
			"Failed to remove wire format from the binary data",
			err)
	}

	if schema != "" {
		codec, err := jsonschema.CompileString(string(element), schema)
		if err != nil {
			return nil, NewXk6KafkaError(failedCreateJsonSchemaCodec,
				"Failed to create codec for decoding JSONSchema",
				err)
		}

		var jsonBytes interface{}
		if err := json.Unmarshal(bytesDecodedData, &jsonBytes); err != nil {
			return nil, NewXk6KafkaError(failedUnmarshalJsonSchema,
				"Failed to unmarshal JSONSchema data",
				err)
		}

		if err := codec.Validate(jsonBytes); err != nil {
			return jsonBytes, NewXk6KafkaError(failedValidateJsonSchema,
				"Failed to validate JSONSchema data, yet returning the data",
				err)
		}

		return jsonBytes, nil
	}

	return bytesDecodedData, nil
}
