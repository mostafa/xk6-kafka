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

// UnmarshalConfiguration unmarshalls the given JSON string into a Configuration.
func UnmarshalConfiguration(jsonConfiguration string) (Configuration, *Xk6KafkaError) {
	var configuration Configuration
	err := json.Unmarshal([]byte(jsonConfiguration), &configuration)
	if err != nil {
		return Configuration{}, NewXk6KafkaError(
			configurationError, "Cannot unmarshal configuration.", err)
	}
	return configuration, nil
}

// ValidateConfiguration validates the given configuration.
func ValidateConfiguration(configuration Configuration) *Xk6KafkaError {
	if (Configuration{}) == configuration {
		// No configuration, fallback to default
		return nil
	}

	if useSerializer(configuration, Key) || useSerializer(configuration, Value) {
		return nil
	}

	return nil
}

// GivenCredentials returns true if the given configuration has credentials.
func GivenCredentials(configuration Configuration) bool {
	if (Configuration{}) == configuration ||
		(SchemaRegistryConfiguration{}) == configuration.SchemaRegistry ||
		(BasicAuth{}) == configuration.SchemaRegistry.BasicAuth {
		return false
	}
	return configuration.SchemaRegistry.BasicAuth.Username != "" &&
		configuration.SchemaRegistry.BasicAuth.Password != ""
}
