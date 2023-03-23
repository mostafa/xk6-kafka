package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type SimpleJKSConfig struct {
	jksConfig JKSConfig
	err       *Xk6KafkaError
}

func TestJKS(t *testing.T) {
	jksConfigs := []*SimpleJKSConfig{
		{
			jksConfig: JKSConfig{
				Path:              "fixtures/kafka-keystore.jks",
				Password:          "password",
				ClientCertAlias:   "localhost",
				ClientKeyAlias:    "localhost",
				ClientKeyPassword: "password",
				ServerCaAlias:     "caroot",
			},
			err: nil,
		},
		{
			jksConfig: JKSConfig{
				Path: "fixtures/kafka-keystore-not-exists.jks",
			},
			err: NewXk6KafkaError(
				fileNotFound, "File not found: fixtures/kafka-keystore-not-exists.jks", nil),
		},
		{
			jksConfig: JKSConfig{
				Path:     "fixtures/kafka-truststore.jks",
				Password: "wrong-password",
			},
			err: NewXk6KafkaError(
				failedDecodeJKSFile, "Failed to decode JKS file: fixtures/kafka-truststore.jks", nil),
		},
		{
			jksConfig: JKSConfig{
				Path:     "fixtures/kafka-truststore.jks",
				Password: "password",
			},
			err: NewXk6KafkaError(
				failedDecodeServerCa, "Failed to decode server's CA: fixtures/kafka-truststore.jks", nil),
		},
		{
			jksConfig: JKSConfig{
				Path:              "fixtures/kafka-keystore.jks",
				Password:          "password",
				ClientCertAlias:   "localhost",
				ClientKeyAlias:    "localhost",
				ClientKeyPassword: "wrong-password",
				ServerCaAlias:     "caroot",
			},
			err: NewXk6KafkaError(
				failedDecodePrivateKey, "Failed to decode client's private key: fixtures/kafka-keystore.jks", nil),
		},
	}

	k := &Kafka{}

	for _, jksConfig := range jksConfigs {
		jks, err := k.loadJKS(&jksConfig.jksConfig)
		if jksConfig.err != nil {
			assert.Equal(t, jksConfig.err.Code, err.Code)
			assert.Equal(t, jksConfig.err.Message, err.Message)

			if jksConfig.err.Code == failedDecodePrivateKey {
				assert.NotNil(t, jks)
				assert.Nil(t, jks.ClientCertsPem)
				assert.Empty(t, jks.ClientCertsPem)
				assert.Equal(t, jks.ClientKeyPem, "")
				assert.Empty(t, jks.ClientKeyPem)
				assert.NotNil(t, jks.ServerCaPem)
				assert.NotEmpty(t, jks.ServerCaPem)
			}
			continue
		}
		assert.Nil(t, err)
		assert.NotNil(t, jks)

		assert.NotNil(t, jks.ClientCertsPem)
		assert.NotEmpty(t, jks.ClientCertsPem)
		assert.NotNil(t, jks.ClientKeyPem)
		assert.NotEmpty(t, jks.ClientKeyPem)
		assert.NotNil(t, jks.ServerCaPem)
		assert.NotEmpty(t, jks.ServerCaPem)
	}
}
