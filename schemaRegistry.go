package kafka

import (
	"errors"

	"github.com/riferrei/srclient"
)

type Element string

const (
	Key   Element = "key"
	Value Element = "value"
)

type BasicAuth struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type SchemaRegistryConfiguration struct {
	Url          string    `json:"url"`
	BasicAuth    BasicAuth `json:"basicAuth"`
	UseLatest    bool      `json:"useLatest"`
	CacheSchemas bool      `json:"cacheSchemas"`
}

func i32tob(val uint32) []byte {
	r := make([]byte, 4)
	for i := uint32(0); i < 4; i++ {
		r[3-i] = byte((val >> (8 * i)) & 0xff)
	}
	return r
}

// Account for proprietary 5-byte prefix before the Avro, ProtoBuf or JSONSchema payload:
// https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#wire-format
func decodeWireFormat(configuration Configuration, messageData []byte, element Element) ([]byte, error) {
	if !useDeserializer(configuration, element) {
		return messageData, nil
	}

	if element == Key && isWireFormatted(configuration.Consumer.KeyDeserializer) ||
		element == Value && isWireFormatted(configuration.Consumer.ValueDeserializer) {
		if len(messageData) < 5 {
			return nil, errors.New("Invalid message data")
		}
		return messageData[5:], nil
	}
	return messageData, nil
}

// Add proprietary 5-byte prefix before the Avro, ProtoBuf or JSONSchema payload:
// https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#wire-format
func encodeWireFormat(configuration Configuration, avroData []byte, topic string, element Element, schema string, version int) ([]byte, error) {
	if !useSerializer(configuration, element) {
		return avroData, nil
	}

	if element == Key && isWireFormatted(configuration.Producer.KeySerializer) ||
		element == "value " && isWireFormatted(configuration.Producer.ValueSerializer) {
		var schemaInfo, err = getSchema(
			configuration, topic, element, schema, srclient.Avro, version)
		if err != nil {
			ReportError(err, "Retrieval of schema id failed.")
			return nil, err
		}
		if schemaInfo.ID() != 0 {
			return append(append([]byte{0}, i32tob(uint32(schemaInfo.ID()))...), avroData...), nil
		}
	}
	return avroData, nil
}

func schemaRegistryClient(configuration Configuration) *srclient.SchemaRegistryClient {
	srClient := srclient.CreateSchemaRegistryClient(configuration.SchemaRegistry.Url)
	srClient.CachingEnabled(configuration.SchemaRegistry.CacheSchemas)

	if GivenCredentials(configuration) {
		srClient.SetCredentials(
			configuration.SchemaRegistry.BasicAuth.Username,
			configuration.SchemaRegistry.BasicAuth.Password)
	}
	return srClient
}

func getSchema(
	configuration Configuration, topic string, element Element,
	schema string, schemaType srclient.SchemaType, version int) (*srclient.Schema, error) {

	// Default schema type is Avro
	if schemaType == "" {
		schemaType = srclient.Avro
	}

	srClient := schemaRegistryClient(configuration)

	var schemaInfo *srclient.Schema
	subject := topic + "-" + string(element)
	// Default version of the schema is the latest version
	// If CacheSchemas is true, the client will cache the schema
	if version == 0 {
		schemaInfo, _ = srClient.GetLatestSchema(subject)
	} else {
		schemaInfo, _ = srClient.GetSchemaByVersion(subject, version)
	}

	if schemaInfo == nil {
		schemaInfo, err := srClient.CreateSchema(subject, schema, schemaType)
		if err != nil {
			ReportError(err, "Creation of schema failed.")
			return nil, err
		}
		return schemaInfo, nil
	}

	return schemaInfo, nil
}
