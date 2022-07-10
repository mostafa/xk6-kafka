package kafka

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/stretchr/testify/assert"
)

// TestGetDialerWithSASLPlainAndFullTLSConfig tests the creation of a dialer with SASL PLAIN and TLS config
func TestGetDialerWithSASLPlainAndFullTLSConfig(t *testing.T) {
	saslConfig := SASLConfig{
		Username:  "test",
		Password:  "test",
		Algorithm: SASL_PLAIN,
	}
	tlsConfig := TLSConfig{
		EnableTLS:             true,
		InsecureSkipTLSVerify: false,
		ClientCertPem:         "client.cer",
		ClientKeyPem:          "key.pem",
		ServerCaPem:           "caroot.cer",
	}
	dialer, err := GetDialer(saslConfig, tlsConfig)
	assert.Nil(t, err)
	assert.NotNil(t, dialer)
	assert.Equal(t, 10*time.Second, dialer.Timeout)
	assert.Equal(t, false, dialer.DualStack)
	assert.Nil(t, dialer.TLS)
	assert.Equal(t, "PLAIN", dialer.SASLMechanism.Name())
	assert.Equal(t, "test", dialer.SASLMechanism.(plain.Mechanism).Username)
	assert.Equal(t, "test", dialer.SASLMechanism.(plain.Mechanism).Password)
}

// TestGetDialerWithSASLPlainWithDefaultTLSConfig tests the creation of a dialer with SASL PLAIN
func TestGetDialerWithSASLPlainWithDefaultTLSConfig(t *testing.T) {
	saslConfig := SASLConfig{
		Username:  "test",
		Password:  "test",
		Algorithm: SASL_PLAIN,
	}
	dialer, err := GetDialer(saslConfig, TLSConfig{EnableTLS: true})
	assert.Nil(t, err)
	assert.NotNil(t, dialer)
	assert.Equal(t, 10*time.Second, dialer.Timeout)
	assert.Equal(t, true, dialer.DualStack)
	assert.NotNil(t, dialer.TLS)
	assert.Equal(t, "PLAIN", dialer.SASLMechanism.Name())
	assert.Equal(t, "test", dialer.SASLMechanism.(plain.Mechanism).Username)
	assert.Equal(t, "test", dialer.SASLMechanism.(plain.Mechanism).Password)
}

// TestGetDialerWithSASLScramWithDefaultTLSConfig tests the creation of a dialer with SASL SCRAM
func TestGetDialerWithSASLScram(t *testing.T) {
	saslConfig := SASLConfig{
		Username:  "test",
		Password:  "test",
		Algorithm: SASL_SCRAM_SHA256, // Same applies with SASL_SCRAM_SHA512
	}
	dialer, err := GetDialer(saslConfig, TLSConfig{EnableTLS: true})
	assert.Nil(t, err)
	assert.NotNil(t, dialer)
	assert.Equal(t, 10*time.Second, dialer.Timeout)
	assert.Equal(t, true, dialer.DualStack)
	assert.NotNil(t, dialer.TLS)
	assert.Equal(t, scram.SHA256.Name(), dialer.SASLMechanism.Name())
}

// TestGetDialerWithSASLSSLWithNoTLSConfigFails tests the creation of a dialer with SASL SSL
func TestGetDialerWithSASLSSLWithNoTLSConfigFails(t *testing.T) {
	saslConfig := SASLConfig{
		Username:  "test",
		Password:  "test",
		Algorithm: SASL_SSL,
	}
	dialer, err := GetDialer(saslConfig, TLSConfig{EnableTLS: false})
	assert.NotNil(t, err)
	assert.Nil(t, dialer)
}

// TestGetDialerFails tests the creation of a dialer with SASL PLAIN and fails on invalid credentials
func TestGetDialerFails(t *testing.T) {
	saslConfig := SASLConfig{
		Username:  "https://www.exa\t\r\n",
		Password:  "test",
		Algorithm: SASL_SCRAM_SHA256,
	}
	dialer, wrappedError := GetDialer(saslConfig, TLSConfig{})
	assert.Equal(t, wrappedError.Message, "Unable to create SCRAM mechanism")
	// This is a stringprep (RFC3454) error wrapped inside the Xk6KafkaError
	assert.Equal(t, wrappedError.Unwrap().Error(), "Error SASLprepping username 'https://www.exa\t\r\n': prohibited character (rune: '\\u0009')")
	assert.Nil(t, dialer)
}

// TestGetDialerUnauthenticatedNoTLS tests the creation of an unauthenticated dialer
func TestGetDialerUnauthenticatedNoTLS(t *testing.T) {
	dialer, err := GetDialer(SASLConfig{}, TLSConfig{})
	assert.Nil(t, err)
	assert.NotNil(t, dialer)
	assert.Equal(t, 10*time.Second, dialer.Timeout)
	assert.Equal(t, false, dialer.DualStack)
	assert.Nil(t, dialer.TLS)
}

// TestFileExists tests the file exists function
func TestFileExists(t *testing.T) {
	assert.Nil(t, fileExists("auth_test.go"))
	assert.NotNil(t, fileExists("test.go.not"))
}

type SimpleTLSConfig struct {
	saslConfig SASLConfig
	tlsConfig  TLSConfig
	err        *Xk6KafkaError
}

// TestTlsConfig tests the creation of a TLS config
func TestTlsConfig(t *testing.T) {
	tlsConfig := TLSConfig{
		EnableTLS:     true,
		ClientCertPem: "fixtures/client.cer",
		ClientKeyPem:  "fixtures/client.pem",
		ServerCaPem:   "fixtures/caroot.cer",
	}
	tlsObject, err := GetTLSConfig(tlsConfig)
	assert.Nil(t, err)
	assert.NotNil(t, tlsObject)
}

// TestTlsConfigFails tests the creation of a TLS config and fails on invalid files and configs
// nolint: funlen
func TestTlsConfigFails(t *testing.T) {
	saslConfig := []*SimpleTLSConfig{
		{
			saslConfig: SASLConfig{},
			tlsConfig:  TLSConfig{EnableTLS: true, ClientCertPem: "test.cer"},
			err: &Xk6KafkaError{
				Code:    fileNotFound,
				Message: "File not found: test.cer",
			},
		},
		{
			saslConfig: SASLConfig{},
			tlsConfig: TLSConfig{
				EnableTLS:     true,
				ClientCertPem: "fixtures/client.cer",
			},
			err: &Xk6KafkaError{
				Code:          fileNotFound,
				Message:       "File not found: ",
				OriginalError: &os.PathError{},
			},
		},
		{
			saslConfig: SASLConfig{},
			tlsConfig: TLSConfig{
				EnableTLS:     true,
				ClientCertPem: "fixtures/client.cer",
				ClientKeyPem:  "fixtures/client.pem",
			},
			err: &Xk6KafkaError{
				Code:          fileNotFound,
				Message:       "File not found: ",
				OriginalError: &os.PathError{},
			},
		},
		{
			saslConfig: SASLConfig{},
			tlsConfig: TLSConfig{
				EnableTLS:     true,
				ClientCertPem: "fixtures/invalid-client.cer",
				ClientKeyPem:  "fixtures/invalid-client.pem",
			},
			err: &Xk6KafkaError{
				Code:          failedLoadX509KeyPair,
				Message:       "Error creating x509 key pair from client cert file \"fixtures/invalid-client.cer\" and client key file \"fixtures/invalid-client.pem\".",
				OriginalError: errors.New("tls: failed to find any PEM data in certificate input"),
			},
		},
		{
			saslConfig: SASLConfig{},
			tlsConfig: TLSConfig{
				EnableTLS:     true,
				ClientCertPem: "fixtures/client.cer",
				ClientKeyPem:  "fixtures/client.pem",
				ServerCaPem:   "fixtures/invalid-caroot.cer",
			},
			err: &Xk6KafkaError{
				Code:    failedAppendCaCertFile,
				Message: "Error appending CA certificate file \"fixtures/invalid-caroot.cer\".",
			},
		},
	}

	for _, c := range saslConfig {
		tlsObject, err := GetTLSConfig(c.tlsConfig)
		assert.NotNil(t, err)
		assert.Equal(t, c.err.Code, err.Code)
		assert.Equal(t, c.err.Message, err.Message)
		assert.Nil(t, tlsObject)
	}
}
