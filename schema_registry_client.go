package kafka

import "github.com/riferrei/srclient"

type SchemaRegistryClient interface {
	GetLatestSchema(subject string) (*srclient.Schema, error)
	GetSchemaByVersion(subject string, version int) (*srclient.Schema, error)
	CreateSchema(
		subject string,
		schema string,
		schemaType srclient.SchemaType,
		references ...srclient.Reference,
	) (*srclient.Schema, error)
	Close() error
}

type srclientSchemaRegistryAdapter struct {
	client *srclient.SchemaRegistryClient
}

func newSRClientAdapter(client *srclient.SchemaRegistryClient) SchemaRegistryClient {
	if client == nil {
		return nil
	}

	return &srclientSchemaRegistryAdapter{client: client}
}

func (a *srclientSchemaRegistryAdapter) GetLatestSchema(subject string) (*srclient.Schema, error) {
	return a.client.GetLatestSchema(subject)
}

func (a *srclientSchemaRegistryAdapter) GetSchemaByVersion(subject string, version int) (*srclient.Schema, error) {
	return a.client.GetSchemaByVersion(subject, version)
}

func (a *srclientSchemaRegistryAdapter) CreateSchema(
	subject string,
	schema string,
	schemaType srclient.SchemaType,
	references ...srclient.Reference,
) (*srclient.Schema, error) {
	return a.client.CreateSchema(subject, schema, schemaType, references...)
}

func (a *srclientSchemaRegistryAdapter) Close() error {
	return nil
}
