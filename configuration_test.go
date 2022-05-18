package kafka

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

var configuration Configuration = Configuration{
	Consumer: ConsumerConfiguration{
		KeyDeserializer:   StringDeserializer,
		ValueDeserializer: StringDeserializer,
	},
	Producer: ProducerConfiguration{
		KeySerializer:   StringSerializer,
		ValueSerializer: StringSerializer,
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

func TestUnmarshalConfiguration(t *testing.T) {
	configJson, _ := json.Marshal(configuration)
	config, err := UnmarshalConfiguration(string(configJson))
	assert.Nil(t, err)
	assert.Equal(t, configuration, config)
}

func TestUnmarshalConfigurationsFails(t *testing.T) {
	configJson := `{"}`

	_, err := UnmarshalConfiguration(configJson)
	assert.NotNil(t, err)
	assert.Equal(t, "Cannot unmarshal configuration.", err.Message)
}

func TestValidateConfiguration(t *testing.T) {
	err := ValidateConfiguration(configuration)
	assert.Nil(t, err)
}

func TestValidateConfigurationFallbackToDefaults(t *testing.T) {
	configuration := Configuration{}

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
