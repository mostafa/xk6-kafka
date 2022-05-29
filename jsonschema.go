package kafka

import (
	"encoding/json"

	"github.com/riferrei/srclient"
	"github.com/santhosh-tekuri/jsonschema/v5"
	"github.com/sirupsen/logrus"
)

const (
	JsonSchemaSerializer   string = "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer"
	JsonSchemaDeserializer string = "io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer"
)

// SerializeJson serializes the data to JSON and adds the wire format to the data and
// returns the serialized data. It uses the given version to retrieve the schema from
// Schema Registry, otherwise it uses the given schema to manually create the codec and
// encode the data. The configuration is used to configure the Schema Registry client.
// The element is used to define the subject. The data should be a string.
func SerializeJson(configuration Configuration, topic string, data interface{}, element Element, schema string, version int) ([]byte, *Xk6KafkaError) {
	bytesData := []byte(data.(string))

	client := SchemaRegistryClientWithConfiguration(configuration.SchemaRegistry)
	subject := topic + "-" + string(element)
	var schemaInfo *srclient.Schema
	schemaID := 0

	var xk6KafkaError *Xk6KafkaError

	if schema != "" {
		// Schema is provided, so we need to create it and get the schema ID
		schemaInfo, xk6KafkaError = CreateSchema(client, subject, schema, srclient.Json)
	} else {
		// Schema is not provided, so we need to fetch the schema from the Schema Registry
		schemaInfo, xk6KafkaError = GetSchema(client, subject, schema, srclient.Json, version)
	}

	if xk6KafkaError != nil {
		logrus.New().WithError(xk6KafkaError).Warn("Failed to create or get schema, manually encoding the data")
		codec, err := jsonschema.CompileString(subject, schema)
		if err != nil {
			return nil, NewXk6KafkaError(failedCreateJsonSchemaCodec,
				"Failed to create codec for encoding JSON",
				err)
		}

		var jsonBytes interface{}
		if err := json.Unmarshal(bytesData, &jsonBytes); err != nil {
			return nil, NewXk6KafkaError(failedUnmarshalJson,
				"Failed to unmarshal JSON data",
				err)
		}

		if err := codec.Validate(jsonBytes); err != nil {
			return nil, NewXk6KafkaError(failedValidateJson,
				"Failed to validate JSON data",
				err)
		}
	}

	if schemaInfo != nil {
		schemaID = schemaInfo.ID()

		// Encode the data into JSON and then the wire format
		jsonEncodedData, _, err := schemaInfo.Codec().NativeFromTextual(bytesData)
		if err != nil {
			return nil, NewXk6KafkaError(failedEncodeToJson,
				"Failed to encode data into JSON",
				err)
		}

		bytesData, err = schemaInfo.Codec().BinaryFromNative(nil, jsonEncodedData)
		if err != nil {
			return nil, NewXk6KafkaError(failedEncodeJsonToBinary,
				"Failed to encode JSON data into binary",
				err)
		}
	}

	return EncodeWireFormat(bytesData, schemaID), nil

}

// DeserializeJson deserializes the data from JSON and returns the decoded data. It
// uses the given version to retrieve the schema from Schema Registry, otherwise it
// uses the given schema to manually create the codec and decode the data. The
// configuration is used to configure the Schema Registry client. The element is
// used to define the subject. The data should be a byte array.
func DeserializeJson(configuration Configuration, topic string, data []byte, element Element, schema string, version int) (interface{}, *Xk6KafkaError) {
	bytesDecodedData, err := DecodeWireFormat(data)
	if err != nil {
		return nil, NewXk6KafkaError(failedDecodeFromWireFormat,
			"Failed to remove wire format from the binary data",
			err)
	}

	client := SchemaRegistryClientWithConfiguration(configuration.SchemaRegistry)
	subject := topic + "-" + string(element)
	var schemaInfo *srclient.Schema

	var xk6KafkaError *Xk6KafkaError

	if schema != "" {
		// Schema is provided, so we need to create it and get the schema ID
		schemaInfo, xk6KafkaError = CreateSchema(client, subject, schema, srclient.Json)
	} else {
		// Schema is not provided, so we need to fetch the schema from the Schema Registry
		schemaInfo, xk6KafkaError = GetSchema(client, subject, schema, srclient.Json, version)
	}

	if xk6KafkaError != nil {
		logrus.New().WithError(xk6KafkaError).Warn("Failed to create or get schema, manually decoding the data")
		codec, err := jsonschema.CompileString(string(element), schema)
		if err != nil {
			return nil, NewXk6KafkaError(failedCreateJsonSchemaCodec,
				"Failed to create codec for decoding JSON data",
				err)
		}

		var jsonBytes interface{}
		if err := json.Unmarshal(bytesDecodedData, &jsonBytes); err != nil {
			return nil, NewXk6KafkaError(failedUnmarshalJson,
				"Failed to unmarshal JSON data",
				err)
		}

		if err := codec.Validate(jsonBytes); err != nil {
			return jsonBytes, NewXk6KafkaError(failedValidateJson,
				"Failed to validate JSON data, yet returning the data",
				err)
		}

		return jsonBytes, nil
	}

	if schemaInfo != nil {
		// Decode the data from Json
		jsonDecodedData, _, err := schemaInfo.Codec().NativeFromBinary(bytesDecodedData)
		if err != nil {
			return nil, NewXk6KafkaError(failedDecodeJsonFromBinary,
				"Failed to decode data from JSON",
				err)
		}
		return jsonDecodedData, nil
	}

	return bytesDecodedData, nil
}
