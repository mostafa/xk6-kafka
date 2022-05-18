package kafka

import (
	"encoding/binary"

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
	Url       string    `json:"url"`
	BasicAuth BasicAuth `json:"basicAuth"`
	UseLatest bool      `json:"useLatest"`
}

// Account for proprietary 5-byte prefix before the Avro, ProtoBuf or JSONSchema payload:
// https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#wire-format
func DecodeWireFormat(message []byte) ([]byte, *Xk6KafkaError) {
	if len(message) < 5 {
		return nil, NewXk6KafkaError(messageTooShort,
			"Invalid message: message too short to contain schema id.", nil)
	}
	return message[5:], nil
}

// Add proprietary 5-byte prefix before the Avro, ProtoBuf or JSONSchema payload:
// https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#wire-format
func EncodeWireFormat(data []byte, schemaID int) []byte {
	schemaIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(schemaIDBytes, uint32(schemaID))
	return append(append([]byte{0}, schemaIDBytes...), data...)
}

func SchemaRegistryClient(url, username, password string) *srclient.SchemaRegistryClient {
	srClient := srclient.CreateSchemaRegistryClient(url)
	if username != "" && password != "" {
		srClient.SetCredentials(username, password)
	}
	return srClient
}

func GetSchema(
	client *srclient.SchemaRegistryClient, subject string, schema string, schemaType srclient.SchemaType, version int) (*srclient.Schema, *Xk6KafkaError) {
	// The client always caches the schema
	var schemaInfo *srclient.Schema
	var err error
	// Default version of the schema is the latest version
	if version == 0 {
		schemaInfo, err = client.GetLatestSchema(subject)
	} else {
		schemaInfo, err = client.GetSchemaByVersion(subject, version)
	}
	if err != nil {
		return nil, NewXk6KafkaError(schemaNotFound,
			"Failed to get schema from schema registry", err)
	}

	return schemaInfo, nil
}

func CreateSchema(
	client *srclient.SchemaRegistryClient, subject string, schema string, schemaType srclient.SchemaType) (*srclient.Schema, *Xk6KafkaError) {
	schemaInfo, err := client.CreateSchema(subject, schema, schemaType)
	if err != nil {
		return nil, NewXk6KafkaError(schemaCreationFailed, "Failed to create schema.", err)
	}
	return schemaInfo, nil
}
