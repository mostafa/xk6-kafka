package kafka

import (
	"encoding/json"
)

type ConsumerConfiguration struct {
	KeyDeserializer   string `json:"keyDeserializer"`
	ValueDeserializer string `json:"valueDeserializer"`
}

type ProducerConfiguration struct {
	KeySerializer   string `json:"keySerializer"`
	ValueSerializer string `json:"valueSerializer"`
}

type Configuration struct {
	Consumer       ConsumerConfiguration       `json:"consumer"`
	Producer       ProducerConfiguration       `json:"producer"`
	SchemaRegistry SchemaRegistryConfiguration `json:"schemaRegistry"`
}

func UnmarshalConfiguration(jsonConfiguration string) (Configuration, *Xk6KafkaError) {
	var configuration Configuration
	err := json.Unmarshal([]byte(jsonConfiguration), &configuration)
	return configuration, NewXk6KafkaError(
		configurationError, "Cannot unmarshal configuration.", err)
}

func ValidateConfiguration(configuration Configuration) *Xk6KafkaError {
	if (Configuration{}) == configuration {
		// No configuration, fallback to default
		return nil
	}

	if useSerializer(configuration, Key) || useSerializer(configuration, Value) {
		if (SchemaRegistryConfiguration{}) == configuration.SchemaRegistry {
			return NewXk6KafkaError(
				configurationError,
				"You must provide a value for the \"SchemaRegistry\" configuration property to use available serializers",
				nil)
		}
	}
	return nil
}

func GivenCredentials(configuration Configuration) bool {
	if (Configuration{}) == configuration ||
		(SchemaRegistryConfiguration{}) == configuration.SchemaRegistry ||
		(BasicAuth{}) == configuration.SchemaRegistry.BasicAuth {
		return false
	}
	return configuration.SchemaRegistry.BasicAuth.Username != "" &&
		configuration.SchemaRegistry.BasicAuth.Password != ""
}
