package kafka

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/grafana/sobek"
	"github.com/hamba/avro/v2"
	"github.com/riferrei/srclient"
	"github.com/santhosh-tekuri/jsonschema/v5"
	"github.com/sirupsen/logrus"
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
	avroSchema    avro.Schema
	jsonSchema    *jsonschema.Schema

	// resolver is a function that can resolve referenced schemas by name
	resolver func(name string) (*Schema, error)
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

// createResolver creates a resolver function for a schema that can fetch referenced schemas.
// The resolver is independent of any specific schema and can fetch any schema by name.
func (k *Kafka) createResolver(
	client *srclient.SchemaRegistryClient,
	enableCaching bool,
) func(name string) (*Schema, error) {
	return func(name string) (*Schema, error) {
		// Try to find the referenced schema in the cache first
		if enableCaching {
			for subject, cachedSchema := range k.schemaCache {
				// Check if cached schema matches the reference name by subject
				if subject == name {
					return cachedSchema, nil
				}
				// Also check by parsed schema full name
				if cachedSchema.avroSchema != nil {
					if namedSchema, ok := cachedSchema.avroSchema.(avro.NamedSchema); ok {
						if namedSchema.FullName() == name {
							return cachedSchema, nil
						}
					}
				}

				// Also check by extracting name from schema JSON
				var schemaMap map[string]any
				if json.Unmarshal([]byte(cachedSchema.Schema), &schemaMap) == nil {
					if ns, ok := schemaMap["namespace"].(string); ok {
						if n, ok := schemaMap["name"].(string); ok {
							fullName := n
							if ns != "" {
								fullName = ns + "." + n
							}
							if fullName == name {
								return cachedSchema, nil
							}
						}
					}
				}
			}
		}

		// Try to fetch by subject name (subject name often matches schema full name in RecordNameStrategy)
		refSchemaInfo, refErr := client.GetLatestSchema(name)
		if refErr == nil {
			refSchema := &Schema{
				EnableCaching: enableCaching,
				ID:            refSchemaInfo.ID(),
				Version:       refSchemaInfo.Version(),
				Schema:        refSchemaInfo.Schema(),
				SchemaType:    refSchemaInfo.SchemaType(),
				References:    refSchemaInfo.References(),
				Subject:       name,
				resolver:      k.createResolver(client, enableCaching), // Recursive resolver setup
			}
			if refSchema.EnableCaching {
				k.schemaCache[name] = refSchema
			}
			return refSchema, nil
		}

		// If GetLatestSchema failed, try to search through all cached schemas' references
		// This handles the case where a nested reference is specified in a parent schema's references
		if enableCaching {
			for _, cachedSchema := range k.schemaCache {
				for _, ref := range cachedSchema.References {
					if ref.Name == name {
						// Fetch the referenced schema from the registry using the reference info
						refSchemaInfo, refErr := client.GetSchemaByVersion(ref.Subject, ref.Version)
						if refErr != nil {
							continue
						}
						refSchema := &Schema{
							EnableCaching: enableCaching,
							ID:            refSchemaInfo.ID(),
							Version:       refSchemaInfo.Version(),
							Schema:        refSchemaInfo.Schema(),
							SchemaType:    refSchemaInfo.SchemaType(),
							References:    refSchemaInfo.References(),
							Subject:       ref.Subject,
							resolver:      k.createResolver(client, enableCaching),
						}
						if refSchema.EnableCaching {
							k.schemaCache[ref.Subject] = refSchema
						}
						return refSchema, nil
					}
				}
			}
		}

		return nil, fmt.Errorf("%w: %s (GetLatestSchema error: %w)", ErrReferenceNotFound, name, refErr)
	}
}

// Codec ensures access to parsed Avro Schema.
// Will try to initialize a new one if it hasn't been initialized before.
// Will return nil if it can't initialize a schema from the schema string.
//
//nolint:maintidx
func (s *Schema) Codec() avro.Schema {
	if s.avroSchema != nil {
		return s.avroSchema
	}

	var (
		schema avro.Schema
		err    error
		cache  *avro.SchemaCache
	)

	// Extract namespace from schema JSON
	extractNamespace := func(schemaStr string) string {
		var schemaMap map[string]any
		if json.Unmarshal([]byte(schemaStr), &schemaMap) == nil {
			if ns, ok := schemaMap["namespace"]; ok {
				if ns, ok := ns.(string); ok {
					return ns
				}
			}
		}
		return ""
	}

	if len(s.References) > 0 && s.resolver != nil {
		// Build a schema cache with referenced schemas and all their nested references
		cache = &avro.SchemaCache{}
		var resolveErrors []error

		// Helper function to recursively resolve all nested references FIRST, then parse schemas
		var resolveAllReferences func(
			refSchema *Schema,
			refName string,
			visited map[string]bool,
		) error

		resolveAllReferences = func(
			refSchema *Schema,
			refName string,
			visited map[string]bool,
		) error {
			if refSchema == nil {
				return nil
			}

			// Mark as visited to avoid infinite recursion
			if visited == nil {
				visited = make(map[string]bool)
			}

			// Get the schema's full name for visited tracking
			var schemaFullName string
			if refSchema.avroSchema != nil {
				if namedSchema, ok := refSchema.avroSchema.(avro.NamedSchema); ok {
					schemaFullName = namedSchema.FullName()
				}
			}

			// If not parsed yet, try to extract from schema JSON
			if schemaFullName == "" {
				var schemaMap map[string]any
				if json.Unmarshal([]byte(refSchema.Schema), &schemaMap) == nil {
					if ns, ok := schemaMap["namespace"].(string); ok {
						if n, ok := schemaMap["name"].(string); ok {
							schemaFullName = n
							if ns != "" {
								schemaFullName = ns + "." + n
							}
						}
					}
				}
			}

			if schemaFullName != "" && visited[schemaFullName] {
				return nil // Already processed
			}

			if schemaFullName != "" {
				visited[schemaFullName] = true
			}

			// FIRST: Recursively resolve all nested references BEFORE parsing
			for _, nestedRef := range refSchema.References {
				if !visited[nestedRef.Name] {
					nestedRefSchema, nestedErr := s.resolver(nestedRef.Name)
					if nestedErr != nil {
						// Log but don't fail - might be optional
						continue
					}
					if nestedRefSchema != nil {
						if err := resolveAllReferences(nestedRefSchema, nestedRef.Name, visited); err != nil {
							// Log nested reference errors but continue
							resolveErrors = append(resolveErrors, fmt.Errorf("nested reference %s: %w", nestedRef.Name, err))
						}
					}
				}
			}

			// NOW parse the schema (all its dependencies should be resolved)
			// Parse directly with ParseWithCache using the shared cache so dependencies are available
			var (
				refAvroSchema avro.Schema
				parseErr      error
			)

			refNamespace := extractNamespace(refSchema.Schema)

			// Parse with the shared cache (which should have all dependencies)
			refAvroSchema, parseErr = avro.ParseWithCache(refSchema.Schema, refNamespace, cache)
			if parseErr != nil {
				return fmt.Errorf("failed to parse referenced schema %s: %w", refSchema.Subject, parseErr)
			}
			if refAvroSchema == nil {
				return fmt.Errorf("%w: %s", ErrFailedParseReferencedSchema, refSchema.Subject)
			}

			// Add to cache with multiple keys for different lookup scenarios
			if namedSchema, ok := refAvroSchema.(avro.NamedSchema); ok {
				fullName := namedSchema.FullName()
				namespace := namedSchema.Namespace()
				name := namedSchema.Name()

				// Add with full name (primary key)
				cache.Add(fullName, refAvroSchema)

				// Add with the reference name (as specified in the references array)
				if refName != "" && refName != fullName {
					cache.Add(refName, refAvroSchema)
				}

				// Add with namespace.name combination
				if namespace != "" && name != "" {
					namespaceName := namespace + "." + name
					if namespaceName != fullName && namespaceName != refName {
						cache.Add(namespaceName, refAvroSchema)
					}
					// Add with just the name - critical for namespace-relative lookups
					cache.Add(name, refAvroSchema)
				}
			} else {
				// For non-named schemas, just add with the reference name
				cache.Add(refSchema.Subject, refAvroSchema)
			}

			return nil
		}

		// Helper function to add a schema to cache (now that all references are resolved)
		addSchemaToCache := func(refSchema *Schema, refName string, visited map[string]bool) error {
			return resolveAllReferences(refSchema, refName, visited)
		}

		// Resolve all direct references
		for _, ref := range s.References {
			refSchema, resolveErr := s.resolver(ref.Name)
			if resolveErr != nil {
				resolveErrors = append(
					resolveErrors,
					fmt.Errorf("failed to resolve reference %s: %w", ref.Name, resolveErr),
				)
				continue
			}
			if err := addSchemaToCache(refSchema, ref.Name, nil); err != nil {
				resolveErrors = append(resolveErrors, err)
			}
		}

		// If we had errors resolving references, set error and skip parsing
		if len(resolveErrors) > 0 {
			err = fmt.Errorf("%w: %d reference(s): %v", ErrFailedResolveReferences, len(resolveErrors), resolveErrors)
		}
	}

	// Parse the schema (with or without cache depending on whether references were resolved)
	if err == nil {
		namespace := extractNamespace(s.Schema)
		if cache != nil {
			schema, err = avro.ParseWithCache(s.Schema, namespace, cache)
		} else {
			schema, err = avro.Parse(s.Schema)
		}
	}

	if err == nil {
		s.avroSchema = schema
	} else {
		logger.WithFields(logrus.Fields{
			"subject": s.Subject,
			"error":   err.Error(),
		}).Error("Failed to parse Avro schema")
	}

	return s.avroSchema
}

// JSONSchema ensures access to JsonSchema.
// Will try to initialize a new one if it hasn't been initialized before.
// Will return nil if it can't initialize a json schema from the schema.
func (s *Schema) JSONSchema() *jsonschema.Schema {
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
		if params, ok := call.Argument(0).Export().(map[string]any); ok {
			if b, err := json.Marshal(params); err != nil {
				common.Throw(runtime, err)
			} else {
				if err = json.Unmarshal(b, &configuration); err != nil {
					common.Throw(runtime, err)
				}
			}
		}

		schemaRegistryClient = k.schemaRegistryClient(configuration)

		// Store the client in Kafka struct so we can use it to create resolvers later
		k.currentSchemaRegistry = schemaRegistryClient
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
		if params, ok := call.Argument(0).Export().(map[string]any); ok {
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
		if params, ok := call.Argument(0).Export().(map[string]any); ok {
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

		if params, ok := call.Argument(0).Export().(map[string]any); ok {
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
		if params, ok := call.Argument(0).Export().(map[string]any); ok {
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
		if params, ok := call.Argument(0).Export().(map[string]any); ok {
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

// getSchema returns the schema for the given subject and schema ID and version.
func (k *Kafka) getSchema(client *srclient.SchemaRegistryClient, schema *Schema) *Schema {
	// If EnableCache is set, check if the schema is in the cache.
	if schema.EnableCaching {
		if schema, ok := k.schemaCache[schema.Subject]; ok {
			return schema
		}
	}

	runtime := k.vu.Runtime()
	// The client always caches the schema.
	var schemaInfo *srclient.Schema
	var err error
	// Default version of the schema is the latest version.
	if schema.Version == 0 {
		schemaInfo, err = client.GetLatestSchema(schema.Subject)
	} else {
		schemaInfo, err = client.GetSchemaByVersion(
			schema.Subject, schema.Version)
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
			resolver:      k.createResolver(client, schema.EnableCaching),
		}
		// If the Cache is set, cache the schema.
		if wrappedSchema.EnableCaching {
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
		resolver:      k.createResolver(client, schema.EnableCaching),
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
	var schemaMap map[string]any
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
