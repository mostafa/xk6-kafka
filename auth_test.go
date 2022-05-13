package kafka

import (
	"testing"
	"time"

	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/stretchr/testify/assert"
)

func TestUnmarshalCredentials(t *testing.T) {
	creds, err := unmarshalCredentials(`{"username": "test", "password": "test", "algorithm": "plain", "clientCertPem": "client.pem", "clientKeyPem": "key.pem", "serverCaPem": "server.pem"}`)
	assert.Nil(t, err)
	assert.Equal(t, "test", creds.Username)
	assert.Equal(t, "test", creds.Password)
	assert.Equal(t, "plain", creds.Algorithm)
	assert.Equal(t, "client.pem", creds.ClientCertPem)
	assert.Equal(t, "key.pem", creds.ClientKeyPem)
	assert.Equal(t, "server.pem", creds.ServerCaPem)
}

func TestGetDialerFromCredsWithSASLPlain(t *testing.T) {
	creds := &Credentials{
		Username:  "test",
		Password:  "test",
		Algorithm: Plain,
	}
	dialer, err := getDialerFromCreds(creds)
	assert.Nil(t, err)
	assert.NotNil(t, dialer)
	assert.Equal(t, 10*time.Second, dialer.Timeout)
	assert.Equal(t, true, dialer.DualStack)
	assert.Nil(t, dialer.TLS)
	assert.Equal(t, "PLAIN", dialer.SASLMechanism.Name())
	assert.Equal(t, "test", dialer.SASLMechanism.(plain.Mechanism).Username)
	assert.Equal(t, "test", dialer.SASLMechanism.(plain.Mechanism).Password)
}

func TestGetDialerFromCredsWithSASLScram(t *testing.T) {
	creds := &Credentials{
		Username:  "test",
		Password:  "test",
		Algorithm: SHA256,
	}
	dialer, err := getDialerFromCreds(creds)
	assert.Nil(t, err)
	assert.NotNil(t, dialer)
	assert.Equal(t, 10*time.Second, dialer.Timeout)
	assert.Equal(t, true, dialer.DualStack)
	assert.Nil(t, dialer.TLS)
	assert.Equal(t, scram.SHA256.Name(), dialer.SASLMechanism.Name())
}

func TestGetDialerFromCredsFails(t *testing.T) {
	creds := &Credentials{
		Username:  "https://www.exa\t\r\n",
		Password:  "test",
		Algorithm: "sha256",
	}
	dialer, wrappedError := getDialerFromCreds(creds)
	assert.Equal(t, wrappedError.Message, "Unable to create SCRAM mechanism")
	assert.Nil(t, dialer)
}

func TestGetDialerFromAuth(t *testing.T) {
	auth := `{"username": "test", "password": "test", "algorithm": "plain", "clientCertPem": "client.pem", "clientKeyPem": "key.pem", "serverCaPem": "server.pem"}`
	dialer, err := getDialerFromAuth(auth)
	assert.Nil(t, err)
	assert.NotNil(t, dialer)
	assert.Equal(t, 10*time.Second, dialer.Timeout)
	assert.Equal(t, true, dialer.DualStack)
	assert.Nil(t, dialer.TLS)
	assert.Equal(t, "PLAIN", dialer.SASLMechanism.Name())
}

func TestGetDialerFromAuthNoAuthString(t *testing.T) {
	dialer, err := getDialerFromAuth("")
	assert.Nil(t, err)
	assert.NotNil(t, dialer)
	assert.Equal(t, 10*time.Second, dialer.Timeout)
	assert.Equal(t, false, dialer.DualStack)
	assert.Nil(t, dialer.TLS)
}

func TestFileExists(t *testing.T) {
	assert.True(t, fileExists("auth_test.go"))
	assert.False(t, fileExists("test.go.not"))
}
