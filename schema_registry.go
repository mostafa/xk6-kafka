package kafka

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/riferrei/srclient"
	"github.com/sirupsen/logrus"
	"net/http"
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
	TLSConfig TLSConfig `json:"tlsConfig"`
}

const (
	TopicNameStrategy       string = "TopicNameStrategy"
	RecordNameStrategy      string = "RecordNameStrategy"
	TopicRecordNameStrategy string = "TopicRecordNameStrategy"
)

// DecodeWireFormat removes the proprietary 5-byte prefix from the Avro, ProtoBuf
// or JSONSchema payload.
// https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#wire-format
func DecodeWireFormat(message []byte) (int, []byte, *Xk6KafkaError) {
	if len(message) < 5 {
		return 0, nil, NewXk6KafkaError(messageTooShort,
			"Invalid message: message too short to contain schema id.", nil)
	}
	if message[0] != 0 {
		return 0, nil, NewXk6KafkaError(messageTooShort,
			"Invalid message: invalid start byte.", nil)
	}
	magicPrefix := int(binary.BigEndian.Uint32(message[1:5]))
	return magicPrefix, message[5:], nil
}

// EncodeWireFormat adds the proprietary 5-byte prefix to the Avro, ProtoBuf or
// JSONSchema payload.
// https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#wire-format
func EncodeWireFormat(data []byte, schemaID int) []byte {
	schemaIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(schemaIDBytes, uint32(schemaID))
	return append(append([]byte{0}, schemaIDBytes...), data...)
}

// SchemaRegistryClientWithConfiguration creates a SchemaRegistryClient instance
// with the given configuration. It will also configure auth and TLS credentials if exists.
func SchemaRegistryClientWithConfiguration(configuration SchemaRegistryConfiguration) *srclient.SchemaRegistryClient {
	var srClient *srclient.SchemaRegistryClient

	tlsConfig, err := GetTLSConfig(configuration.TLSConfig)
	if err != nil {
		logrus.New().WithField("error", err).Warn("Failed to get TLS config. Continuing without TLS.")
		srClient = srclient.CreateSchemaRegistryClient(configuration.Url)
	}

	if tlsConfig != nil {
		httpClient := &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: tlsConfig,
			},
		}
		srClient = srclient.CreateSchemaRegistryClientWithOptions(configuration.Url, httpClient, 16)
	}

	if configuration.BasicAuth.Username != "" && configuration.BasicAuth.Password != "" {
		srClient.SetCredentials(configuration.BasicAuth.Username, configuration.BasicAuth.Password)
	}

	return srClient
}

// GetSchema returns the schema for the given subject and schema ID and version
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

// CreateSchema creates a new schema in the schema registry
func CreateSchema(
	client *srclient.SchemaRegistryClient, subject string, schema string, schemaType srclient.SchemaType) (*srclient.Schema, *Xk6KafkaError) {
	schemaInfo, err := client.CreateSchema(subject, schema, schemaType)
	if err != nil {
		return nil, NewXk6KafkaError(schemaCreationFailed, "Failed to create schema.", err)
	}
	return schemaInfo, nil
}

func GetSubjectName(schema string, topic string, element Element, subjectNameStrategy string) (string, *Xk6KafkaError) {
	if subjectNameStrategy == "" || subjectNameStrategy == TopicNameStrategy {
		return topic + "-" + string(element), nil
	}

	var schemaMap map[string]interface{}
	err := json.Unmarshal([]byte(schema), &schemaMap)
	if err != nil {
		return "", NewXk6KafkaError(failedToUnmarshalSchema, "Failed to unmarshal schema", nil)
	}
	var recordName = ""
	if namespace, ok := schemaMap["namespace"]; ok {
		recordName = namespace.(string) + "."
	}
	recordName += schemaMap["name"].(string)

	if subjectNameStrategy == RecordNameStrategy {
		return recordName, nil
	}
	if subjectNameStrategy == TopicRecordNameStrategy {
		return topic + "-" + recordName, nil
	}

	return "", NewXk6KafkaError(failedEncodeToAvro, fmt.Sprintf(
		"Unknown subject name strategy: %v", subjectNameStrategy), nil)
}
