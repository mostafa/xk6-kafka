package kafka

import cschemaregistry "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"

type SchemaRegistryClient interface {
	GetLatestSchema(subject string) (*RegisteredSchema, error)
	GetSchemaByVersion(subject string, version int) (*RegisteredSchema, error)
	CreateSchema(
		subject string,
		schema string,
		schemaType SchemaType,
		references ...Reference,
	) (*RegisteredSchema, error)
	Close() error
}

type confluentSchemaRegistryAdapter struct {
	cacheEnabled bool
	client       cschemaregistry.Client
}

func newConfluentSchemaRegistryAdapter(
	client cschemaregistry.Client,
	cacheEnabled bool,
) SchemaRegistryClient {
	if client == nil {
		return nil
	}

	return &confluentSchemaRegistryAdapter{
		cacheEnabled: cacheEnabled,
		client:       client,
	}
}

func (a *confluentSchemaRegistryAdapter) GetLatestSchema(subject string) (*RegisteredSchema, error) {
	defer a.clearCachesIfDisabled()

	metadata, err := a.client.GetLatestSchemaMetadata(subject)
	if err != nil {
		return nil, err
	}

	return newRegisteredSchema(
		metadata.ID,
		metadata.Version,
		metadata.Schema,
		metadata.SchemaType,
		metadata.References,
	), nil
}

func (a *confluentSchemaRegistryAdapter) GetSchemaByVersion(subject string, version int) (*RegisteredSchema, error) {
	defer a.clearCachesIfDisabled()

	metadata, err := a.client.GetSchemaMetadata(subject, version)
	if err != nil {
		return nil, err
	}

	return newRegisteredSchema(
		metadata.ID,
		metadata.Version,
		metadata.Schema,
		metadata.SchemaType,
		metadata.References,
	), nil
}

func (a *confluentSchemaRegistryAdapter) CreateSchema(
	subject string,
	schema string,
	schemaType SchemaType,
	references ...Reference,
) (*RegisteredSchema, error) {
	defer a.clearCachesIfDisabled()

	metadata, err := a.client.RegisterFullResponse(subject, cschemaregistry.SchemaInfo{
		Schema:     schema,
		SchemaType: string(normalizeSchemaType(string(schemaType))),
		References: references,
	}, false)
	if err != nil {
		return nil, err
	}

	return newRegisteredSchema(
		metadata.ID,
		metadata.Version,
		metadata.Schema,
		metadata.SchemaType,
		metadata.References,
	), nil
}

func (a *confluentSchemaRegistryAdapter) Close() error {
	return a.client.Close()
}

func (a *confluentSchemaRegistryAdapter) clearCachesIfDisabled() {
	if a.cacheEnabled || a.client == nil {
		return
	}

	_ = a.client.ClearCaches()
}
