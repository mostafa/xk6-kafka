package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/aws_msk_iam_v2"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

// TLSVersions is a map of TLS versions to their numeric values.
var TLSVersions map[string]uint16

const (
	none            = "none"
	saslPlain       = "sasl_plain"
	saslScramSha256 = "sasl_scram_sha256"
	saslScramSha512 = "sasl_scram_sha512"
	saslSsl         = "sasl_ssl"
	saslAwsIam      = "sasl_aws_iam"

	Timeout = time.Second * 10
)

type SASLConfig struct {
	Username  string `json:"username"`
	Password  string `json:"password"`
	Algorithm string `json:"algorithm"`
}

type TLSConfig struct {
	EnableTLS             bool   `json:"enableTls"`
	InsecureSkipTLSVerify bool   `json:"insecureSkipTlsVerify"`
	MinVersion            string `json:"minVersion"`
	ClientCertPem         string `json:"clientCertPem"`
	ClientKeyPem          string `json:"clientKeyPem"`
	ServerCaPem           string `json:"serverCaPem"`
}

// GetDialer creates a kafka dialer from the given auth string or an unauthenticated dialer if the auth string is empty.
func GetDialer(saslConfig SASLConfig, tlsConfig TLSConfig) (*kafkago.Dialer, *Xk6KafkaError) {
	// Create a unauthenticated dialer with no TLS
	dialer := &kafkago.Dialer{
		Timeout:   Timeout,
		DualStack: false,
	}

	// Create a SASL-authenticated dialer with no TLS
	saslMechanism, err := GetSASLMechanism(saslConfig)
	if err != nil {
		return nil, err
	}
	if saslMechanism != nil {
		dialer.DualStack = true
		dialer.SASLMechanism = saslMechanism
	}

	// Create a TLS dialer, either with or without SASL authentication
	tlsObject, err := GetTLSConfig(tlsConfig)
	if err != nil {
		// Ignore the error if we're not using TLS
		if err.Code != noTLSConfig {
			logger.WithField("error", err).Error("Cannot process TLS config")
		}
	}
	if tlsObject == nil && saslConfig.Algorithm == saslSsl {
		return nil, NewXk6KafkaError(
			failedCreateDialerWithSaslSSL, "You must enable TLS to use SASL_SSL", nil)
	}
	dialer.TLS = tlsObject
	dialer.DualStack = (tlsObject != nil)

	return dialer, nil
}

// GetSASLMechanism returns a kafka SASL config from the given credentials.
func GetSASLMechanism(saslConfig SASLConfig) (sasl.Mechanism, *Xk6KafkaError) {
	if saslConfig.Algorithm == "" {
		saslConfig.Algorithm = none
	}

	switch saslConfig.Algorithm {
	case none:
		return nil, nil
	case saslPlain, saslSsl:
		mechanism := plain.Mechanism{
			Username: saslConfig.Username,
			Password: saslConfig.Password,
		}
		return mechanism, nil
	case saslScramSha256, saslScramSha512:
		hashes := make(map[string]scram.Algorithm)
		hashes[saslScramSha256] = scram.SHA256
		hashes[saslScramSha512] = scram.SHA512

		mechanism, err := scram.Mechanism(
			hashes[saslConfig.Algorithm],
			saslConfig.Username,
			saslConfig.Password,
		)
		if err != nil {
			return nil, NewXk6KafkaError(
				failedCreateDialerWithScram, "Unable to create SCRAM mechanism", err)
		}
		return mechanism, nil
	case saslAwsIam:
		cfg, err := config.LoadDefaultConfig(context.TODO())
		if err != nil {
			return nil, NewXk6KafkaError(
				failedCreateDialerWithAwsIam, "Unable to load AWS IAM config for AWS MSK", err)
		}
		return aws_msk_iam_v2.NewMechanism(cfg), nil
	default:
		// Should we fail silently?
		return nil, nil
	}
}

// GetTLSConfig creates a TLS config from the given TLS config struct and checks for errors.
// nolint: funlen
func GetTLSConfig(tlsConfig TLSConfig) (*tls.Config, *Xk6KafkaError) {
	tlsObject := newTLSObject(tlsConfig)

	if tlsConfig.EnableTLS {
		if tlsConfig.ServerCaPem == "" {
			return tlsObject, nil
		}
	} else {
		// TLS is disabled, and we continue with a unauthenticated dialer
		return nil, NewXk6KafkaError(
			noTLSConfig, "No TLS config provided. Continuing with TLS disabled.", nil)
	}

	if tlsConfig.ClientCertPem != "" && tlsConfig.ClientKeyPem != "" {
		// Try to load the certificates from string
		cert, err := tls.X509KeyPair([]byte(tlsConfig.ClientCertPem), []byte(tlsConfig.ClientKeyPem))
		if err != nil && err.Error() == "tls: failed to find any PEM data in certificate input" {
			// Fall back to loading the client certificate and key from the file
			if err := fileExists(tlsConfig.ClientCertPem); err != nil {
				return nil, err
			}

			if err := fileExists(tlsConfig.ClientKeyPem); err != nil {
				return nil, err
			}

			cert, err = tls.LoadX509KeyPair(tlsConfig.ClientCertPem, tlsConfig.ClientKeyPem)
			if err != nil {
				return nil, NewXk6KafkaError(
					failedLoadX509KeyPair,
					fmt.Sprintf(
						"Error creating x509 key pair from \"%s\" and \"%s\".",
						tlsConfig.ClientCertPem,
						tlsConfig.ClientKeyPem),
					err)
			}
		} else if err != nil {
			return nil, NewXk6KafkaError(
				failedLoadX509KeyPair,
				"Error creating x509 key pair from passed content.", err)
		}

		tlsObject.Certificates = []tls.Certificate{cert}
	}

	caCertPool := x509.NewCertPool()

	// Load the CA certificate as string if provided
	if ok := caCertPool.AppendCertsFromPEM([]byte(tlsConfig.ServerCaPem)); !ok {
		// Fall back if file path is provided
		if err := fileExists(tlsConfig.ServerCaPem); err != nil {
			return nil, err
		}

		caCert, err := os.ReadFile(tlsConfig.ServerCaPem)
		if err != nil {
			// This might happen on permissions issues or if the file is unreadable somehow
			return nil, NewXk6KafkaError(
				failedReadCaCertFile,
				fmt.Sprintf(
					"Error reading CA certificate file \"%s\".",
					tlsConfig.ServerCaPem),
				err)
		}

		if ok := caCertPool.AppendCertsFromPEM(caCert); !ok {
			return nil, NewXk6KafkaError(
				failedAppendCaCertFile,
				fmt.Sprintf(
					"Error appending CA certificate file \"%s\".",
					tlsConfig.ServerCaPem),
				nil)
		}
	}

	tlsObject.RootCAs = caCertPool
	return tlsObject, nil
}

// newTLSConfig returns a tls.Config object from the given TLS config.
func newTLSObject(tlsConfig TLSConfig) *tls.Config {
	// Create a TLS config with default settings
	// #nosec G402
	tlsObject := &tls.Config{
		InsecureSkipVerify: tlsConfig.InsecureSkipTLSVerify,
		MinVersion:         tls.VersionTLS12,
	}

	// Set the minimum TLS version
	if tlsConfig.MinVersion != "" {
		if minVersion, ok := TLSVersions[tlsConfig.MinVersion]; ok {
			tlsObject.MinVersion = minVersion
		}
	}

	return tlsObject
}

// fileExists returns nil if the given file exists and error otherwise.
func fileExists(filename string) *Xk6KafkaError {
	if _, err := os.Stat(filename); err != nil {
		return NewXk6KafkaError(fileNotFound, fmt.Sprintf("File not found: %s", filename), err)
	}
	return nil
}
