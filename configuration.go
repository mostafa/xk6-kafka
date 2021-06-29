package kafka

import (
	"encoding/json"
	"errors"
)

type ConsumerConfiguration struct {
	KeyDeserializer       string    `json:"keyDeserializer"`
	ValueDeserializer     string    `json:"valueDeserializer"`
}

type ProducerConfiguration struct {
	KeySerializer       string    `json:"keySerializer"`
	ValueSerializer     string    `json:"valueSerializer"`
}

type BasicAuth struct {
	CredentialsSource string `json:"credentialsSource"`
	UserInfo          string `json:"userInfo"`
}

type SchemaRegistryConfiguration struct {
	Url       string    `json:"url"`
	BasicAuth BasicAuth `json:"basicAuth"`
}

type Configuration struct {
	Consumer ConsumerConfiguration `json:"consumer"`
	Producer ProducerConfiguration `json:"producer"`
	SchemaRegistry SchemaRegistryConfiguration `json:"schemaRegistry"`
}

func unmarshalConfiguration(jsonConfiguration string) (Configuration, error) {
	var configuration Configuration
	err := json.Unmarshal([]byte(jsonConfiguration), &configuration)
	return configuration, err
}

func useKafkaAvroDeserializer(configuration Configuration, keyOrValue string) bool {
	if (Configuration{}) == configuration ||
		(ConsumerConfiguration{}) == configuration.Consumer {
		return false
	}
	if keyOrValue == "key" && configuration.Consumer.KeyDeserializer == "io.confluent.kafka.serializers.KafkaAvroDeserializer" ||
	keyOrValue == "value" && configuration.Consumer.ValueDeserializer == "io.confluent.kafka.serializers.KafkaAvroDeserializer" {
		return true
	}
	return false
}

func useKafkaAvroSerializer(configuration Configuration, keyOrValue string) bool {
	if (Configuration{}) == configuration ||
		(ProducerConfiguration{}) == configuration.Producer {
		return false
	}
	if keyOrValue == "key" && configuration.Producer.KeySerializer == "io.confluent.kafka.serializers.KafkaAvroSerializer" ||
	keyOrValue == "value" && configuration.Producer.ValueSerializer == "io.confluent.kafka.serializers.KafkaAvroSerializer" {
		return true
	}
	return false
}

func useBasicAuthWithCredentialSourceUserInfo(configuration Configuration) bool {
	if (Configuration{}) == configuration ||
		(SchemaRegistryConfiguration{}) == configuration.SchemaRegistry ||
		(BasicAuth{}) == configuration.SchemaRegistry.BasicAuth {
		return false
	}
	return configuration.SchemaRegistry.BasicAuth.CredentialsSource == "USER_INFO"
}

func validateConfiguration(configuration Configuration) error {
	if useKafkaAvroSerializer(configuration, "key") || useKafkaAvroSerializer(configuration, "value") {
		if (SchemaRegistryConfiguration{}) == configuration.SchemaRegistry {
			return errors.New("you must provide a value for the \"SchemaRegistry\" configuration property to use a serializer " +
				"of type \"io.confluent.kafka.serializers.KafkaAvroSerializer\"")
		}
	}
	return nil
}
