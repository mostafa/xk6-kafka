package kafka

import (
	"testing"

	"github.com/riferrei/srclient"
	"github.com/stretchr/testify/assert"
)

func TestDecodeWireFormat(t *testing.T) {
	encoded := []byte{1, 2, 3, 4, 5, 6}
	decoded := []byte{6}

	result, err := DecodeWireFormat(encoded)
	assert.Nil(t, err)
	assert.Equal(t, decoded, result)
}

func TestDecodeWireFormatFails(t *testing.T) {
	encoded := []byte{1, 2, 3, 4} // too short

	result, err := DecodeWireFormat(encoded)
	assert.Nil(t, result)
	assert.NotNil(t, err)
	assert.Equal(t, "Invalid message: message too short to contain schema id.", err.Message)
	assert.Equal(t, messageTooShort, err.Code)
	assert.Nil(t, err.Unwrap())
}

func TestEncodeWireFormat(t *testing.T) {
	data := []byte{6}
	schemaID := 5
	encoded := []byte{0, 0, 0, 0, 5, 6}

	result := EncodeWireFormat(data, schemaID)
	assert.Equal(t, encoded, result)
}

func TestSchemaRegistryClient(t *testing.T) {
	srConfig := SchemaRegistryConfiguration{
		Url: "http://localhost:8081",
		BasicAuth: BasicAuth{
			Username: "username",
			Password: "password",
		},
	}
	srClient := SchemaRegistryClientWithConfiguration(srConfig)
	assert.NotNil(t, srClient)
}

func TestSchemaRegistryClientWithTLSConfig(t *testing.T) {
	srConfig := SchemaRegistryConfiguration{
		Url: "http://localhost:8081",
		BasicAuth: BasicAuth{
			Username: "username",
			Password: "password",
		},
		TLSConfig: &TLSConfig{
			ClientCertPem: "fixtures/client.cer",
			ClientKeyPem:  "fixtures/client.pem",
			ServerCaPem:   "fixtures/caroot.cer",
		},
	}
	srClient := SchemaRegistryClientWithConfiguration(srConfig)
	assert.NotNil(t, srClient)
}

func TestGetLatestSchemaFails(t *testing.T) {
	srConfig := SchemaRegistryConfiguration{
		Url: "http://localhost:8081",
		BasicAuth: BasicAuth{
			Username: "username",
			Password: "password",
		},
	}
	srClient := SchemaRegistryClientWithConfiguration(srConfig)
	schema, err := GetSchema(srClient, "test-subject", "test-schema", srclient.Avro, 0)
	assert.Nil(t, schema)
	assert.NotNil(t, err)
	assert.Equal(t, "Failed to get schema from schema registry", err.Message)
}

func TestGetSchemaFails(t *testing.T) {
	srConfig := SchemaRegistryConfiguration{
		Url: "http://localhost:8081",
		BasicAuth: BasicAuth{
			Username: "username",
			Password: "password",
		},
	}
	srClient := SchemaRegistryClientWithConfiguration(srConfig)
	schema, err := GetSchema(srClient, "test-subject", "test-schema", srclient.Avro, 1)
	assert.Nil(t, schema)
	assert.NotNil(t, err)
	assert.Equal(t, "Failed to get schema from schema registry", err.Message)
}

func TestCreateSchemaFails(t *testing.T) {
	srConfig := SchemaRegistryConfiguration{
		Url: "http://localhost:8081",
		BasicAuth: BasicAuth{
			Username: "username",
			Password: "password",
		},
	}
	srClient := SchemaRegistryClientWithConfiguration(srConfig)
	schema, err := CreateSchema(srClient, "test-subject", "test-schema", srclient.Avro)
	assert.Nil(t, schema)
	assert.NotNil(t, err)
	assert.Equal(t, "Failed to create schema.", err.Message)
}
