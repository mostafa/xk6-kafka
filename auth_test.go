package kafka

import (
	"errors"
	"testing"
	"time"

	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/stretchr/testify/assert"
)

func TestUnmarshalCredentials(t *testing.T) {
	creds, err := UnmarshalCredentials(`{"username": "test", "password": "test", "algorithm": "plain", "clientCertPem": "client.pem", "clientKeyPem": "key.pem", "serverCaPem": "server.pem"}`)
	assert.Nil(t, err)
	assert.Equal(t, "test", creds.Username)
	assert.Equal(t, "test", creds.Password)
	assert.Equal(t, "plain", creds.Algorithm)
	assert.Equal(t, "client.pem", creds.ClientCertPem)
	assert.Equal(t, "key.pem", creds.ClientKeyPem)
	assert.Equal(t, "server.pem", creds.ServerCaPem)
}

func TestUnmarshalCredentialsFails(t *testing.T) {
	// This only fails on invalid JSON (apparently)
	creds, err := UnmarshalCredentials(`{"invalid": "invalid`)
	assert.Nil(t, creds)
	assert.NotNil(t, err)
	assert.Equal(t, err.Message, "Unable to unmarshal credentials")
	// This is the error we get from the json package wrapped inside the Xk6KafkaError
	assert.Equal(t, err.Unwrap().Error(), "unexpected end of JSON input")
}

func TestGetDialerFromCredsWithSASLPlain(t *testing.T) {
	creds := &Credentials{
		Username:  "test",
		Password:  "test",
		Algorithm: Plain,
	}
	dialer, err := GetDialerFromCreds(creds)
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
	dialer, err := GetDialerFromCreds(creds)
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
	dialer, wrappedError := GetDialerFromCreds(creds)
	assert.Equal(t, wrappedError.Message, "Unable to create SCRAM mechanism")
	// This is a stringprep (RFC3454) error wrapped inside the Xk6KafkaError
	assert.Equal(t, wrappedError.Unwrap().Error(), "Error SASLprepping username 'https://www.exa\t\r\n': prohibited character (rune: '\\u0009')")
	assert.Nil(t, dialer)
}

func TestGetDialerFromAuth(t *testing.T) {
	auth := `{"username": "test", "password": "test", "algorithm": "plain", "clientCertPem": "client.pem", "clientKeyPem": "key.pem", "serverCaPem": "server.pem"}`
	dialer, err := GetDialerFromAuth(auth)
	assert.Nil(t, err)
	assert.NotNil(t, dialer)
	assert.Equal(t, 10*time.Second, dialer.Timeout)
	assert.Equal(t, true, dialer.DualStack)
	assert.Nil(t, dialer.TLS)
	assert.Equal(t, "PLAIN", dialer.SASLMechanism.Name())
	assert.Equal(t, "test", dialer.SASLMechanism.(plain.Mechanism).Username)
	assert.Equal(t, "test", dialer.SASLMechanism.(plain.Mechanism).Password)
}

func TestGetDialerFromAuthNoAuthString(t *testing.T) {
	dialer, err := GetDialerFromAuth("")
	assert.Nil(t, err)
	assert.NotNil(t, dialer)
	assert.Equal(t, 10*time.Second, dialer.Timeout)
	assert.Equal(t, false, dialer.DualStack)
	assert.Nil(t, dialer.TLS)
}

func TestFileExists(t *testing.T) {
	assert.True(t, FileExists("auth_test.go"))
	assert.False(t, FileExists("test.go.not"))
}

type SimpleTLSConfig struct {
	creds *Credentials
	err   *Xk6KafkaError
}

func TestTlsConfig(t *testing.T) {
	creds := &Credentials{
		ClientCertPem: "fixtures/client.cer",
		ClientKeyPem:  "fixtures/client.pem",
		ServerCaPem:   "fixtures/caroot.cer",
	}
	tlsConfig, err := TLSConfig(creds)
	assert.Nil(t, err)
	assert.NotNil(t, tlsConfig)
}

func TestTlsConfigFails(t *testing.T) {
	creds := []*SimpleTLSConfig{
		{
			creds: &Credentials{},
			err: &Xk6KafkaError{
				Code:    fileNotFound,
				Message: "Client certificate file not found.",
			},
		},
		{
			creds: &Credentials{
				ClientCertPem: "fixtures/client.cer",
			},
			err: &Xk6KafkaError{
				Code:    fileNotFound,
				Message: "Client key file not found.",
			},
		},
		{
			creds: &Credentials{
				ClientCertPem: "fixtures/client.cer",
				ClientKeyPem:  "fixtures/client.pem",
			},
			err: &Xk6KafkaError{
				Code:    fileNotFound,
				Message: "CA certificate file not found.",
			},
		},
		{
			creds: &Credentials{
				ClientCertPem: "fixtures/invalid-client.cer",
				ClientKeyPem:  "fixtures/invalid-client.pem",
			},
			err: &Xk6KafkaError{
				Code:          failedLoadX509KeyPair,
				Message:       "Error creating x509 keypair from client cert file \"fixtures/invalid-client.cer\" and client key file \"fixtures/invalid-client.pem\"",
				OriginalError: errors.New("tls: failed to find any PEM data in certificate input"),
			},
		},
		{
			creds: &Credentials{
				ClientCertPem: "fixtures/client.cer",
				ClientKeyPem:  "fixtures/client.pem",
				ServerCaPem:   "fixtures/invalid-caroot.cer",
			},
			err: &Xk6KafkaError{
				Code:    failedAppendCaCertFile,
				Message: "Error appending CA certificate file \"fixtures/invalid-caroot.cer\"",
			},
		},
	}

	for _, c := range creds {
		tlsConfig, err := TLSConfig(c.creds)
		assert.NotNil(t, err)
		assert.Equal(t, c.err, err)
		assert.Nil(t, tlsConfig)
	}
}
