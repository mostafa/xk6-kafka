package kafka

import (
	"os"
	"testing"
	"time"

	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/stretchr/testify/assert"
)

// TestGetDialerWithSASLPlainAndFullTLSConfig tests the creation of a dialer with SASL PLAIN and TLS config.
func TestGetDialerWithSASLPlainAndFullTLSConfig(t *testing.T) {
	saslConfig := SASLConfig{
		Username:  "test",
		Password:  "test",
		Algorithm: saslPlain,
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
	if mechanism, ok := dialer.SASLMechanism.(plain.Mechanism); ok {
		assert.Equal(t, "test", mechanism.Username)
		assert.Equal(t, "test", mechanism.Password)
	} else {
		assert.Fail(t, "Expected SASL mechanism to be plain.Mechanism")
	}
}

// TestGetDialerWithSASLPlainWithDefaultTLSConfig tests the creation of a dialer with SASL PLAIN.
func TestGetDialerWithSASLPlainWithDefaultTLSConfig(t *testing.T) {
	saslConfig := SASLConfig{
		Username:  "test",
		Password:  "test",
		Algorithm: saslPlain,
	}
	dialer, err := GetDialer(saslConfig, TLSConfig{EnableTLS: true})
	assert.Nil(t, err)
	assert.NotNil(t, dialer)
	assert.Equal(t, 10*time.Second, dialer.Timeout)
	assert.Equal(t, true, dialer.DualStack)
	assert.NotNil(t, dialer.TLS)
	assert.Equal(t, "PLAIN", dialer.SASLMechanism.Name())
	if mechanism, ok := dialer.SASLMechanism.(plain.Mechanism); ok {
		assert.Equal(t, "test", mechanism.Username)
		assert.Equal(t, "test", mechanism.Password)
	} else {
		assert.Fail(t, "Expected SASL mechanism to be plain.Mechanism")
	}
}

// TestGetDialerWithSASLScramWithDefaultTLSConfig tests the creation of a dialer with SASL SCRAM.
func TestGetDialerWithSASLScram(t *testing.T) {
	saslConfig := SASLConfig{
		Username:  "test",
		Password:  "test",
		Algorithm: saslScramSha256, // Same applies with SASL_SCRAM_SHA512.
	}
	dialer, err := GetDialer(saslConfig, TLSConfig{EnableTLS: true})
	assert.Nil(t, err)
	assert.NotNil(t, dialer)
	assert.Equal(t, 10*time.Second, dialer.Timeout)
	assert.Equal(t, true, dialer.DualStack)
	assert.NotNil(t, dialer.TLS)
	assert.Equal(t, scram.SHA256.Name(), dialer.SASLMechanism.Name())
}

// TestGetDialerWithSASLSSLWithNoTLSConfigFails tests the creation of a dialer with SASL SSL.
func TestGetDialerWithSASLSSLWithNoTLSConfigFails(t *testing.T) {
	saslConfig := SASLConfig{
		Username:  "test",
		Password:  "test",
		Algorithm: saslSsl,
	}
	dialer, err := GetDialer(saslConfig, TLSConfig{EnableTLS: false})
	assert.NotNil(t, err)
	assert.Nil(t, dialer)
}

// TestGetDialerFails tests the creation of a dialer with SASL PLAIN and fails on invalid credentials.
func TestGetDialerFails(t *testing.T) {
	saslConfig := SASLConfig{
		Username:  "https://www.exa\t\r\n",
		Password:  "test",
		Algorithm: saslScramSha256,
	}
	dialer, wrappedError := GetDialer(saslConfig, TLSConfig{})
	assert.Equal(t, wrappedError.Message, "Unable to create SCRAM mechanism")
	// This is a stringprep (RFC3454) error wrapped inside the Xk6KafkaError.
	assert.Equal(t, wrappedError.Unwrap().Error(),
		"Error SASLprepping username 'https://www.exa\t\r\n': "+
			"prohibited character (rune: '\\u0009')")
	assert.Nil(t, dialer)
}

// TestGetDialerUnauthenticatedNoTLS tests the creation of an unauthenticated dialer.
func TestGetDialerUnauthenticatedNoTLS(t *testing.T) {
	dialer, err := GetDialer(SASLConfig{}, TLSConfig{})
	assert.Nil(t, err)
	assert.NotNil(t, dialer)
	assert.Equal(t, 10*time.Second, dialer.Timeout)
	assert.Equal(t, false, dialer.DualStack)
	assert.Nil(t, dialer.TLS)
}

// TestFileExists tests the file exists function.
func TestFileExists(t *testing.T) {
	assert.Nil(t, fileExists("auth_test.go"))
	assert.NotNil(t, fileExists("test.go.not"))
}

type SimpleTLSConfig struct {
	tlsConfig TLSConfig
	err       *Xk6KafkaError
}

// TestTlsConfig tests the creation of a TLS config.
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

// TestTlsConfigFails tests the creation of a TLS config and fails on invalid files and configs.
// nolint: funlen
func TestTlsConfigFails(t *testing.T) {
	tlsConfigs := []*SimpleTLSConfig{
		{
			tlsConfig: TLSConfig{},
			err: &Xk6KafkaError{
				Code:          noTLSConfig,
				Message:       "No TLS config provided. Continuing with TLS disabled.",
				OriginalError: nil,
			},
		},
		{
			tlsConfig: TLSConfig{
				EnableTLS:     true,
				ServerCaPem:   "server.cer",
				ClientCertPem: "client.cer",
				ClientKeyPem:  "client.pem",
			},
			err: &Xk6KafkaError{
				Code:          fileNotFound,
				Message:       "File not found: client.cer",
				OriginalError: nil,
			},
		},
		{
			tlsConfig: TLSConfig{
				EnableTLS:     true,
				ServerCaPem:   "server.cer",
				ClientCertPem: "fixtures/client.cer",
				ClientKeyPem:  "test.pem",
			},
			err: &Xk6KafkaError{
				Code:          fileNotFound,
				Message:       "File not found: test.pem",
				OriginalError: nil,
			},
		},
		{
			tlsConfig: TLSConfig{
				EnableTLS:     true,
				ServerCaPem:   "server.cer",
				ClientCertPem: "fixtures/client.cer",
				ClientKeyPem:  "fixtures/client.pem",
			},
			err: &Xk6KafkaError{
				Code:          fileNotFound,
				Message:       "File not found: server.cer",
				OriginalError: nil,
			},
		},
		{
			tlsConfig: TLSConfig{
				EnableTLS:     true,
				ServerCaPem:   "fixtures/caroot.cer",
				ClientCertPem: "fixtures/invalid-client.cer",
				ClientKeyPem:  "fixtures/invalid-client.pem",
			},
			err: &Xk6KafkaError{
				Code: failedLoadX509KeyPair,
				Message: "Error creating x509 key pair from \"fixtures/invalid-client.cer\" " +
					"and \"fixtures/invalid-client.pem\".",
				OriginalError: ErrInvalidPEMData,
			},
		},
		{
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

	for _, c := range tlsConfigs {
		tlsObject, err := GetTLSConfig(c.tlsConfig)
		assert.NotNil(t, err)
		assert.Equal(t, c.err.Code, err.Code)
		assert.Equal(t, c.err.Message, err.Message)
		assert.Nil(t, tlsObject)
	}
}

// TestTlsConfigFromContent tests the creation of a TLS config.
func TestTlsConfigFromContent(t *testing.T) {
	paths := []string{"fixtures/client.cer", "fixtures/client.pem", "fixtures/caroot.cer"}
	files := make([]string, len(paths))

	for i, path := range paths {
		file, _ := os.ReadFile(path)
		files[i] = string(file)
	}

	tlsConfig := TLSConfig{
		EnableTLS:     true,
		ClientCertPem: files[0],
		ClientKeyPem:  files[1],
		ServerCaPem:   files[2],
	}
	tlsObject, err := GetTLSConfig(tlsConfig)

	assert.Nil(t, err)
	assert.NotNil(t, tlsObject)
}
