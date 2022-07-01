package kafka

import (
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

// TestValidateConfiguration tests the validation of a given configuration.
func TestValidateConfiguration(t *testing.T) {
	err := ValidateConfiguration(configuration)
	assert.Nil(t, err)
}

// TestValidateConfigurationFallbackToDefaults tests the validation of a given configuration
// and falls back to default on invalid configuration.
func TestValidateConfigurationFallbackToDefaults(t *testing.T) {
	configuration := Configuration{}

	err := ValidateConfiguration(configuration)
	assert.Nil(t, err)
}

// TestGivenCredentials tests the validation of a given credentials.
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

// TestGivenCredentialsFails tests if credentials are given in Schema Registry config
// and fails on no auth creds.
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
