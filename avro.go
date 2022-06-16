package kafka

import (
	"github.com/linkedin/goavro/v2"
	"github.com/riferrei/srclient"
	"github.com/sirupsen/logrus"
)

const (
	AvroSerializer   string = "io.confluent.kafka.serializers.KafkaAvroSerializer"
	AvroDeserializer string = "io.confluent.kafka.serializers.KafkaAvroDeserializer"
)

// SerializeAvro serializes the given data to wire-formatted Avro binary format and returns it
// as a byte array. It uses the given version to retrieve the schema from Schema Registry, otherwise
// it uses the given schema to manually create the codec and encode the data. The configuration
// is used to configure the Schema Registry client. The element is used to define the subject.
// The data should be a string.
func SerializeAvro(configuration Configuration, topic string, data interface{}, element Element, schema string, version int) ([]byte, *Xk6KafkaError) {
	bytesData := []byte(data.(string))

	client := SchemaRegistryClientWithConfiguration(configuration.SchemaRegistry)

	var subject, subjectNameError = GetSubjectName(schema, topic, element, configuration.Producer.SubjectNameStrategy)
	if subjectNameError != nil {
		return nil, subjectNameError
	}

	var schemaInfo *srclient.Schema
	schemaID := 0

	var xk6KafkaError *Xk6KafkaError

	if schema != "" {
		// Schema is provided, so we need to create it and get the schema ID
		schemaInfo, xk6KafkaError = CreateSchema(client, subject, schema, srclient.Avro)
	} else {
		// Schema is not provided, so we need to fetch the schema from the Schema Registry
		schemaInfo, xk6KafkaError = GetSchema(client, subject, schema, srclient.Avro, version)
	}

	if xk6KafkaError != nil {
		logrus.New().WithField("error", xk6KafkaError).Warn("Failed to create or get schema, manually encoding the data")
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

// DeserializeAvro deserializes the given data from wire-formatted Avro binary format and returns it
// as a byte array. It uses the given version to retrieve the schema from Schema Registry, otherwise
// it uses the given schema to manually create the codec and decode the data. The configuration
// is used to configure the Schema Registry client. The element is used to define the subject.
// The data should be a byte array.
func DeserializeAvro(configuration Configuration, topic string, data []byte, element Element, schema string, version int) (interface{}, *Xk6KafkaError) {
	schemaID, bytesDecodedData, err := DecodeWireFormat(data)
	if err != nil {
		return nil, NewXk6KafkaError(failedDecodeFromWireFormat,
			"Failed to remove wire format from the binary data",
			err)
	}

	var schemaInfo *srclient.Schema
	var xk6KafkaError *Xk6KafkaError
	var getSchemaError error

	client := SchemaRegistryClientWithConfiguration(configuration.SchemaRegistry)

	var subject, subjectNameError = GetSubjectName(schema, topic, element, configuration.Consumer.SubjectNameStrategy)
	if subjectNameError != nil {
		return nil, subjectNameError
	}

	if schema != "" {
		// Schema is provided, so we need to create it and get the schema ID
		schemaInfo, xk6KafkaError = CreateSchema(client, subject, schema, srclient.Avro)
	} else if configuration.Consumer.UseMagicPrefix {
		// Schema not provided and no valid version flag, so we use te schemaID in the magic prefix
		schemaInfo, getSchemaError = client.GetSchema(schemaID)
		if getSchemaError != nil {
			xk6KafkaError = NewXk6KafkaError(failedCreateAvroCodec,
				"Failed to get schema by magic prefix",
				getSchemaError)
		}
	} else {
		// Schema is not provided, so we need to fetch the schema from the Schema Registry
		schemaInfo, xk6KafkaError = GetSchema(client, subject, schema, srclient.Avro, version)
	}

	if xk6KafkaError != nil {
		logrus.New().WithField("error", xk6KafkaError).Warn("Failed to create or get schema, manually decoding the data")
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
