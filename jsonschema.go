package kafka

import (
	"encoding/json"

	"github.com/santhosh-tekuri/jsonschema/v5"
)

func SerializeJsonSchema(configuration Configuration, topic string, data interface{}, element Element, schema string, version int) ([]byte, error) {
	bytesData := []byte(data.(string))
	subject := topic + "-" + string(element)
	if schema != "" {
		codec, err := jsonschema.CompileString(subject, schema)
		if err != nil {
			ReportError(err, "Failed to create codec for encoding JSONSchema")
		}

		var jsonBytes interface{}
		if err := json.Unmarshal(bytesData, &jsonBytes); err != nil {
			ReportError(err, "Failed to unmarshal JSONSchema data")
		}

		if err := codec.Validate(jsonBytes); err != nil {
			ReportError(err, "Failed to validate JSONSchema data")
		}
	}

	byteData, err := encodeWireFormat(configuration, bytesData, topic, element, schema, version)
	if err != nil {
		ReportError(err, "Failed to add wire format to the binary data")
		return nil, err
	}

	return byteData, nil
}

func DeserializeJsonSchema(configuration Configuration, data []byte, element Element, schema string, version int) interface{} {
	bytesDecodedData, err := decodeWireFormat(configuration, data, element)
	if err != nil {
		ReportError(err, "Failed to remove wire format from the binary data")
		return nil
	}

	if schema != "" {
		codec, err := jsonschema.CompileString(string(element), schema)
		if err != nil {
			ReportError(err, "Failed to create codec for decoding JSONSchema")
		}

		var jsonBytes interface{}
		if err := json.Unmarshal(bytesDecodedData, &jsonBytes); err != nil {
			ReportError(err, "Failed to unmarshal JSONSchema data")
			return nil
		}

		if err := codec.Validate(jsonBytes); err != nil {
			ReportError(err, "Failed to validate JSONSchema data, yet returning the data")
		}

		return jsonBytes
	}

	return bytesDecodedData
}
