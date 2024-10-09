package kafka

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/grafana/sobek"
	"github.com/linkedin/goavro/v2"
	"github.com/riferrei/srclient"
	"github.com/santhosh-tekuri/jsonschema/v5"
	"go.k6.io/k6/js/common"
)

type Element string

const (
	Key                Element = "key"
	Value              Element = "value"
	MagicPrefixSize    int     = 5
	ConcurrentRequests int     = 16
)

type BasicAuth struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type SchemaRegistryConfig struct {
	EnableCaching bool      `json:"enableCaching"`
	URL           string    `json:"url"`
	BasicAuth     BasicAuth `json:"basicAuth"`
	TLS           TLSConfig `json:"tls"`
}

const (
	TopicNameStrategy       string = "TopicNameStrategy"
	RecordNameStrategy      string = "RecordNameStrategy"
	TopicRecordNameStrategy string = "TopicRecordNameStrategy"
)

// Schema is a wrapper around the schema registry schema.
// The Codec() and JsonSchema() methods will return the respective codecs (duck-typing).
type Schema struct {
	EnableCaching bool                 `json:"enableCaching"`
	ID            int                  `json:"id"`
	Schema        string               `json:"schema"`
	SchemaType    *srclient.SchemaType `json:"schemaType"`
	Version       int                  `json:"version"`
	References    []srclient.Reference `json:"references"`
	Subject       string               `json:"subject"`
	codec         *goavro.Codec
	jsonSchema    *jsonschema.Schema
}

type SubjectNameConfig struct {
	Schema              string  `json:"schema"`
	Topic               string  `json:"topic"`
	Element             Element `json:"element"`
	SubjectNameStrategy string  `json:"subjectNameStrategy"`
}

type WireFormat struct {
	SchemaID int    `json:"schemaId"`
	Data     []byte `json:"data"`
}

// Codec ensures access to Codec
// Will try to initialize a new one if it hasn't been initialized before
// Will return nil if it can't initialize a codec from the schema
func (s *Schema) Codec() *goavro.Codec {
	if s.codec == nil {
		codec, err := goavro.NewCodec(s.Schema)
		if err == nil {
			s.codec = codec
		}
	}
	return s.codec
}

// JsonSchema ensures access to JsonSchema
// Will try to initialize a new one if it hasn't been initialized before
// Will return nil if it can't initialize a json schema from the schema
func (s *Schema) JsonSchema() *jsonschema.Schema {
	if s.jsonSchema == nil {
		jsonSchema, err := jsonschema.CompileString("schema.json", s.Schema)
		if err == nil {
			s.jsonSchema = jsonSchema
		}
	}
	return s.jsonSchema
}

func (k *Kafka) schemaRegistryClientClass(call sobek.ConstructorCall) *sobek.Object {
	runtime := k.vu.Runtime()
	var configuration *SchemaRegistryConfig
	var schemaRegistryClient *srclient.SchemaRegistryClient

	if len(call.Arguments) == 1 {
		if params, ok := call.Argument(0).Export().(map[string]interface{}); ok {
			if b, err := json.Marshal(params); err != nil {
				common.Throw(runtime, err)
			} else {
				if err = json.Unmarshal(b, &configuration); err != nil {
					common.Throw(runtime, err)
				}
			}
		}

		schemaRegistryClient = k.schemaRegistryClient(configuration)
	}

	schemaRegistryClientObject := runtime.NewObject()
	// This is the schema registry client object itself
	if err := schemaRegistryClientObject.Set("This", schemaRegistryClient); err != nil {
		common.Throw(runtime, err)
	}

	err := schemaRegistryClientObject.Set("getSchema", func(call sobek.FunctionCall) sobek.Value {
		if len(call.Arguments) == 0 {
			common.Throw(runtime, ErrNotEnoughArguments)
		}

		if schemaRegistryClient == nil {
			common.Throw(runtime, ErrNoSchemaRegistryClient)
		}

		var schema *Schema
		if params, ok := call.Argument(0).Export().(map[string]interface{}); ok {
			if b, err := json.Marshal(params); err != nil {
				common.Throw(runtime, err)
			} else {
				if err = json.Unmarshal(b, &schema); err != nil {
					common.Throw(runtime, err)
				}
			}
		}

		return runtime.ToValue(k.getSchema(schemaRegistryClient, schema))
	})
	if err != nil {
		common.Throw(runtime, err)
	}

	err = schemaRegistryClientObject.Set("createSchema", func(call sobek.FunctionCall) sobek.Value {
		if len(call.Arguments) == 0 {
			common.Throw(runtime, ErrNotEnoughArguments)
		}

		if schemaRegistryClient == nil {
			common.Throw(runtime, ErrNoSchemaRegistryClient)
		}

		var schema *Schema
		if params, ok := call.Argument(0).Export().(map[string]interface{}); ok {
			if b, err := json.Marshal(params); err != nil {
				common.Throw(runtime, err)
			} else {
				if err = json.Unmarshal(b, &schema); err != nil {
					common.Throw(runtime, err)
				}
			}
		}

		return runtime.ToValue(k.createSchema(schemaRegistryClient, schema))
	})
	if err != nil {
		common.Throw(runtime, err)
	}

	var subjectNameConfig *SubjectNameConfig
	err = schemaRegistryClientObject.Set("getSubjectName", func(call sobek.FunctionCall) sobek.Value {
		if len(call.Arguments) == 0 {
			common.Throw(runtime, ErrNotEnoughArguments)
		}

		if params, ok := call.Argument(0).Export().(map[string]interface{}); ok {
			if b, err := json.Marshal(params); err != nil {
				common.Throw(runtime, err)
			} else {
				if err = json.Unmarshal(b, &subjectNameConfig); err != nil {
					common.Throw(runtime, err)
				}
			}
		}

		return runtime.ToValue(k.getSubjectName(subjectNameConfig))
	})
	if err != nil {
		common.Throw(runtime, err)
	}

	err = schemaRegistryClientObject.Set("serialize", func(call sobek.FunctionCall) sobek.Value {
		if len(call.Arguments) == 0 {
			common.Throw(runtime, ErrNotEnoughArguments)
		}

		var metadata *Container
		if params, ok := call.Argument(0).Export().(map[string]interface{}); ok {
			if b, err := json.Marshal(params); err != nil {
				common.Throw(runtime, err)
			} else {
				if err = json.Unmarshal(b, &metadata); err != nil {
					common.Throw(runtime, err)
				}
			}
		}

		return runtime.ToValue(k.serialize(metadata))
	})
	if err != nil {
		common.Throw(runtime, err)
	}

	err = schemaRegistryClientObject.Set("deserialize", func(call sobek.FunctionCall) sobek.Value {
		if len(call.Arguments) == 0 {
			common.Throw(runtime, ErrNotEnoughArguments)
		}

		var metadata *Container
		if params, ok := call.Argument(0).Export().(map[string]interface{}); ok {
			if b, err := json.Marshal(params); err != nil {
				common.Throw(runtime, err)
			} else {
				if err = json.Unmarshal(b, &metadata); err != nil {
					common.Throw(runtime, err)
				}
			}
		}

		return runtime.ToValue(k.deserialize(metadata))
	})
	if err != nil {
		common.Throw(runtime, err)
	}

	return schemaRegistryClientObject
}

// schemaRegistryClient creates a schemaRegistryClient instance
// with the given configuration. It will also configure auth and TLS credentials if exists.
func (k *Kafka) schemaRegistryClient(config *SchemaRegistryConfig) *srclient.SchemaRegistryClient {
	runtime := k.vu.Runtime()
	var srClient *srclient.SchemaRegistryClient

	tlsConfig, err := GetTLSConfig(config.TLS)
	if err != nil {
		// Ignore the error if we're not using TLS
		if err.Code != noTLSConfig {
			common.Throw(runtime, err)
		}
		srClient = srclient.CreateSchemaRegistryClient(config.URL)
	}

	if tlsConfig != nil {
		httpClient := &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: tlsConfig,
			},
		}
		srClient = srclient.CreateSchemaRegistryClientWithOptions(
			config.URL, httpClient, ConcurrentRequests)
	}

	if config.BasicAuth.Username != "" && config.BasicAuth.Password != "" {
		srClient.SetCredentials(config.BasicAuth.Username, config.BasicAuth.Password)
	}

	// The default value for a boolean is false, so the caching
	// feature of the srclient package will be disabled.
	srClient.CachingEnabled(config.EnableCaching)

	return srClient
}

// getSchema returns either the latest schema for the given subject or a specific version (if given) or the schema for the given schema string (if given)
func (k *Kafka) getSchema(client *srclient.SchemaRegistryClient, schema *Schema) *Schema {
	// If EnableCache is set, check if the schema is in the cache.
	if schema.EnableCaching {
		if cachedSchema, ok := k.schemaCache[schema.Subject]; ok {
			// the cache should contain the latest version of a schema for the given subject
			// we must not return the cached schema if it does not match the requested version or schema string
			if (schema.Version == 0 && schema.Schema != "") || schema.Version == cachedSchema.Version || schema.Schema == cachedSchema.Schema {
				return cachedSchema;
			}
		}
	}

	runtime := k.vu.Runtime()
	// The client always caches the schema.
	var schemaInfo *srclient.Schema
	var err error
	var isLatestSchema = false;
	if schema.Schema != "" { // fetch schema for given schema string
		var schemaType srclient.SchemaType
		if schema.SchemaType != nil {
			schemaType = *schema.SchemaType
		} else {
			schemaType = srclient.Avro
		}
	    schemaInfo, err = client.LookupSchema(schema.Subject, schema.Schema, schemaType, schema.References...)
	} else if schema.Version == 0 { // fetch schema by version
		schemaInfo, err = client.GetLatestSchema(schema.Subject)
	} else { // fetch latest schema for given subject
		schemaInfo, err = client.GetSchemaByVersion(schema.Subject, schema.Version)
		isLatestSchema = true;
	}

	if err == nil {
		wrappedSchema := &Schema{
			EnableCaching: schema.EnableCaching,
			ID:            schemaInfo.ID(),
			Version:       schemaInfo.Version(),
			Schema:        schemaInfo.Schema(),
			SchemaType:    schemaInfo.SchemaType(),
			References:    schemaInfo.References(),
			Subject:       schema.Subject,
		}
		// If the Cache is set, cache the schema.
		if wrappedSchema.EnableCaching && isLatestSchema {
			k.schemaCache[wrappedSchema.Subject] = wrappedSchema
		}
		return wrappedSchema
	} else {
		err := NewXk6KafkaError(schemaNotFound, "Failed to get schema from schema registry", err)
		common.Throw(runtime, err)
		return nil
	}
}

// createSchema creates a new schema in the schema registry.
func (k *Kafka) createSchema(client *srclient.SchemaRegistryClient, schema *Schema) *Schema {
	runtime := k.vu.Runtime()
	schemaInfo, err := client.CreateSchema(
		schema.Subject,
		schema.Schema,
		*schema.SchemaType,
		schema.References...)
	if err != nil {
		err := NewXk6KafkaError(schemaCreationFailed, "Failed to create schema.", err)
		common.Throw(runtime, err)
		return nil
	}

	wrappedSchema := &Schema{
		EnableCaching: schema.EnableCaching,
		ID:            schemaInfo.ID(),
		Version:       schemaInfo.Version(),
		Schema:        schemaInfo.Schema(),
		SchemaType:    schemaInfo.SchemaType(),
		References:    schemaInfo.References(),
		Subject:       schema.Subject,
	}
	if schema.EnableCaching {
		k.schemaCache[schema.Subject] = wrappedSchema
	}
	return wrappedSchema
}

// getSubjectName returns the subject name for the given schema and topic.
func (k *Kafka) getSubjectName(subjectNameConfig *SubjectNameConfig) string {
	if subjectNameConfig.SubjectNameStrategy == "" ||
		subjectNameConfig.SubjectNameStrategy == TopicNameStrategy {
		return subjectNameConfig.Topic + "-" + string(subjectNameConfig.Element)
	}

	runtime := k.vu.Runtime()
	var schemaMap map[string]interface{}
	err := json.Unmarshal([]byte(subjectNameConfig.Schema), &schemaMap)
	if err != nil {
		common.Throw(runtime, NewXk6KafkaError(
			failedUnmarshalSchema, "Failed to unmarshal schema", err))
	}
	recordName := ""
	if namespace, ok := schemaMap["namespace"]; ok {
		if namespace, ok := namespace.(string); ok {
			recordName = namespace + "."
		} else {
			err := NewXk6KafkaError(failedTypeCast, "Failed to cast to string", nil)
			common.Throw(runtime, err)
		}
	}
	if name, ok := schemaMap["name"]; ok {
		if name, ok := name.(string); ok {
			recordName += name
		} else {
			err := NewXk6KafkaError(failedTypeCast, "Failed to cast to string", nil)
			common.Throw(runtime, err)
		}
	}

	if subjectNameConfig.SubjectNameStrategy == RecordNameStrategy {
		return recordName
	}
	if subjectNameConfig.SubjectNameStrategy == TopicRecordNameStrategy {
		return subjectNameConfig.Topic + "-" + recordName
	}

	err = NewXk6KafkaError(failedToEncode, fmt.Sprintf(
		"Unknown subject name strategy: %v", subjectNameConfig.SubjectNameStrategy), nil)
	common.Throw(runtime, err)
	return ""
}

// encodeWireFormat adds the proprietary 5-byte prefix to the Avro, ProtoBuf or
// JSONSchema payload.
// https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#wire-format
func (k *Kafka) encodeWireFormat(data []byte, schemaID int) []byte {
	schemaIDBytes := make([]byte, MagicPrefixSize-1)
	binary.BigEndian.PutUint32(schemaIDBytes, uint32(schemaID))
	return append(append([]byte{0}, schemaIDBytes...), data...)
}

// decodeWireFormat removes the proprietary 5-byte prefix from the Avro, ProtoBuf
// or JSONSchema payload.
// https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#wire-format
func (k *Kafka) decodeWireFormat(message []byte) []byte {
	runtime := k.vu.Runtime()
	if len(message) < MagicPrefixSize {
		err := NewXk6KafkaError(messageTooShort,
			"Invalid message: message too short to contain schema id.", nil)
		common.Throw(runtime, err)
		return nil
	}
	if message[0] != 0 {
		err := NewXk6KafkaError(messageTooShort, "Invalid message: invalid start byte.", nil)
		common.Throw(runtime, err)
		return nil
	}
	return message[MagicPrefixSize:]
}
