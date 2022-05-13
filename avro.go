package kafka

import (
	"github.com/linkedin/goavro/v2"
	"github.com/riferrei/srclient"
	"github.com/sirupsen/logrus"
)

func SerializeAvro(configuration Configuration, topic string, data interface{}, element Element, schema string, version int) ([]byte, *Xk6KafkaError) {
	bytesData := []byte(data.(string))

	client := SchemaRegistryClient(configuration.SchemaRegistry.Url,
		configuration.SchemaRegistry.BasicAuth.Username,
		configuration.SchemaRegistry.BasicAuth.Password)
	subject := topic + "-" + string(element)
	var schemaInfo *srclient.Schema
	schemaID := 0

	var schemaTypes map[Element]srclient.SchemaType = map[Element]srclient.SchemaType{
		Key:   GetSchemaType(configuration.Producer.KeySerializer),
		Value: GetSchemaType(configuration.Producer.ValueSerializer),
	}

	var xk6KafkaError *Xk6KafkaError

	if schema != "" {
		// Schema is provided, so we need to create it and get the schema ID
		schemaInfo, xk6KafkaError = CreateSchema(client, subject, schema, schemaTypes[element])
	} else {
		// Schema is not provided, so we need to fetch the schema from the Schema Registry
		schemaInfo, xk6KafkaError = GetSchema(client, subject, schema, schemaTypes[element], version)
	}

	if xk6KafkaError != nil {
		logrus.New().WithError(xk6KafkaError).Error("Failed to create or get schema, manually encoding the data")
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

	if schemaInfo != nil {
		schemaID = schemaInfo.ID()

		// Encode the data into Avro and then the wire format
		avroEncodedData, _, err := schemaInfo.Codec().NativeFromTextual(bytesData)
		if err != nil {
			return nil, NewXk6KafkaError(failedEncodeToAvro,
				"Failed to encode data into Avro",
				err)
		}

		bytesData, err = schemaInfo.Codec().BinaryFromNative(nil, avroEncodedData)
		if err != nil {
			return nil, NewXk6KafkaError(failedEncodeAvroToBinary,
				"Failed to encode Avro data into binary",
				err)
		}
	}

	return EncodeWireFormat(bytesData, schemaID), nil
}

func DeserializeAvro(configuration Configuration, topic string, data []byte, element Element, schema string, version int) (interface{}, *Xk6KafkaError) {
	bytesDecodedData, err := DecodeWireFormat(data)
	if err != nil {
		return nil, NewXk6KafkaError(failedDecodeFromWireFormat,
			"Failed to remove wire format from the binary data",
			err)
	}

	client := SchemaRegistryClient(configuration.SchemaRegistry.Url,
		configuration.SchemaRegistry.BasicAuth.Username,
		configuration.SchemaRegistry.BasicAuth.Password)
	subject := topic + "-" + string(element)
	var schemaInfo *srclient.Schema

	var schemaTypes map[Element]srclient.SchemaType = map[Element]srclient.SchemaType{
		Key:   GetSchemaType(configuration.Producer.KeySerializer),
		Value: GetSchemaType(configuration.Producer.ValueSerializer),
	}

	var xk6KafkaError *Xk6KafkaError

	if schema != "" {
		// Schema is provided, so we need to create it and get the schema ID
		schemaInfo, xk6KafkaError = CreateSchema(client, subject, schema, schemaTypes[element])
	} else {
		// Schema is not provided, so we need to fetch the schema from the Schema Registry
		schemaInfo, xk6KafkaError = GetSchema(client, subject, schema, schemaTypes[element], version)
	}

	if xk6KafkaError != nil {
		logrus.New().WithError(xk6KafkaError).Error("Failed to create or get schema, manually decoding the data")
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

	if schemaInfo != nil {
		// Decode the data from Avro
		avroDecodedData, _, err := schemaInfo.Codec().NativeFromBinary(bytesDecodedData)
		if err != nil {
			return nil, NewXk6KafkaError(failedDecodeAvroFromBinary,
				"Failed to decode data from Avro",
				err)
		}
		return avroDecodedData, nil
	}

	return bytesDecodedData, nil
}
