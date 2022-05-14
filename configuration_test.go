package kafka

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnmarshalConfiguration(t *testing.T) {
	actual := Configuration{
		Consumer: ConsumerConfiguration{
			KeyDeserializer:   "org.apache.kafka.common.serialization.StringDeserializer",
			ValueDeserializer: "org.apache.kafka.common.serialization.StringDeserializer",
		},
		Producer: ProducerConfiguration{
			KeySerializer:   "org.apache.kafka.common.serialization.StringSerializer",
			ValueSerializer: "org.apache.kafka.common.serialization.StringSerializer",
		},
		SchemaRegistry: SchemaRegistryConfiguration{
			Url: "http://localhost:8081",
			BasicAuth: BasicAuth{
				Username: "username",
				Password: "password",
			},
			UseLatest: true,
		},
	}

	configJson, _ := json.Marshal(actual)
	config, err := UnmarshalConfiguration(string(configJson))
	assert.Nil(t, err)
	assert.Equal(t, actual, config)
}

func TestUnmarshalConfigurationsFails(t *testing.T) {
	configJson := `{"}`

	_, err := UnmarshalConfiguration(configJson)
	assert.NotNil(t, err)
	assert.Equal(t, "Cannot unmarshal configuration.", err.Message)
}

func TestValidateConfiguration(t *testing.T) {
	configuration := Configuration{
		Consumer: ConsumerConfiguration{
			KeyDeserializer:   "org.apache.kafka.common.serialization.StringDeserializer",
			ValueDeserializer: "org.apache.kafka.common.serialization.StringDeserializer",
		},
		Producer: ProducerConfiguration{
			KeySerializer:   "org.apache.kafka.common.serialization.StringSerializer",
			ValueSerializer: "org.apache.kafka.common.serialization.StringSerializer",
		},
		SchemaRegistry: SchemaRegistryConfiguration{
			Url: "http://localhost:8081",
			BasicAuth: BasicAuth{
				Username: "username",
				Password: "password",
			},
			UseLatest: true,
		},
	}

	err := ValidateConfiguration(configuration)
	assert.Nil(t, err)
}

func TestValidateConfigurationFallbackToDefaults(t *testing.T) {
	configuration := Configuration{}

	err := ValidateConfiguration(configuration)
	assert.Nil(t, err)
}

func TestValidateConfigurationFails(t *testing.T) {
	configuration := Configuration{
		Consumer: ConsumerConfiguration{
			KeyDeserializer:   "org.apache.kafka.common.serialization.StringDeserializer",
			ValueDeserializer: "org.apache.kafka.common.serialization.StringDeserializer",
		},
		Producer: ProducerConfiguration{
			KeySerializer:   "org.apache.kafka.common.serialization.StringSerializer",
			ValueSerializer: "org.apache.kafka.common.serialization.StringSerializer",
		},
	}

	err := ValidateConfiguration(configuration)
	assert.Nil(t, err)
}

func TestGivenCredentials(t *testing.T) {
	configuration := Configuration{
		SchemaRegistry: SchemaRegistryConfiguration{
			Url: "http://localhost:8081",
			BasicAuth: BasicAuth{
				Username: "username",
				Password: "password",
			},
			UseLatest: true,
		},
	}

	valid := GivenCredentials(configuration)
	assert.True(t, valid)
}

func TestGivenCredentialsFails(t *testing.T) {
	configuration := Configuration{
		SchemaRegistry: SchemaRegistryConfiguration{
			Url:       "http://localhost:8081",
			UseLatest: true,
		},
	}

	valid := GivenCredentials(configuration)
	assert.False(t, valid)
}
