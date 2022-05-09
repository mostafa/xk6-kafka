package kafka

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
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

func UnmarshalConfiguration(jsonConfiguration string) (Configuration, error) {
	var configuration Configuration
	err := json.Unmarshal([]byte(jsonConfiguration), &configuration)
	return configuration, err
}

func ValidateConfiguration(configuration Configuration) error {
	if (Configuration{}) == configuration {
		// No configuration, fallback to default
		return nil
	}

	if useSerializer(configuration, Key) || useSerializer(configuration, Value) {
		if (SchemaRegistryConfiguration{}) == configuration.SchemaRegistry {
			return errors.New("You must provide a value for the \"SchemaRegistry\" configuration property to use a serializer " +
				"of either of these types " + fmt.Sprintf("%q", reflect.ValueOf(Serializers).MapKeys()))
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
