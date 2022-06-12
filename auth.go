package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/sirupsen/logrus"
)

const (
	None              = "none"
	SASL_Plain        = "sasl_plain"
	SASL_SCRAM_SHA256 = "sasl_scram_sha256"
	SASL_SCRAM_SHA512 = "sasl_scram_sha512"
	SASL_SSL          = "sasl_ssl"

	TLSv10 = "TLSv1.0"
	TLSv11 = "TLSv1.1"
	TLSv12 = "TLSv1.2"
	TLSv13 = "TLSv1.3"
)

type SASLConfig struct {
	Username  string `json:"username"`
	Password  string `json:"password"`
	Algorithm string `json:"algorithm"`
}

type TLSConfig struct {
	EnableTLS             bool   `json:"enableTLS"`
	InsecureSkipTLSVerify bool   `json:"insecureSkipTLSVerify"`
	MinVersion            string `json:"minVersion"`
	ClientCertPem         string `json:"clientCertPem"`
	ClientKeyPem          string `json:"clientKeyPem"`
	ServerCaPem           string `json:"serverCaPem"`
}

// GetDialer creates a kafka dialer from the given auth string or an unauthenticated dialer if the auth string is empty
func GetDialer(saslConfig SASLConfig, tlsConfig TLSConfig) (*kafkago.Dialer, *Xk6KafkaError) {
	logger := logrus.New()
	// Create a unauthenticated dialer with no TLS
	dialer := &kafkago.Dialer{
		Timeout:   10 * time.Second,
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
		logger.WithField("error", err).Info("Cannot process TLS config")
	}
	if tlsObject == nil && saslConfig.Algorithm == SASL_SSL {
		return nil, NewXk6KafkaError(
			failedCreateDialerWithSaslSSL, "You must enable TLS to use SASL_SSL", nil)
	}
	dialer.TLS = tlsObject
	dialer.DualStack = (tlsObject != nil)

	return dialer, nil
}

// GetSASLMechanism returns a kafka SASL config from the given credentials
func GetSASLMechanism(saslConfig SASLConfig) (sasl.Mechanism, *Xk6KafkaError) {
	if saslConfig.Algorithm == "" {
		saslConfig.Algorithm = None
	}

	switch saslConfig.Algorithm {
	case None:
		return nil, nil
	case SASL_Plain, SASL_SSL:
		mechanism := plain.Mechanism{
			Username: saslConfig.Username,
			Password: saslConfig.Password,
		}
		return mechanism, nil
	case SASL_SCRAM_SHA256, SASL_SCRAM_SHA512:
		hashes := make(map[string]scram.Algorithm)
		hashes[SASL_SCRAM_SHA256] = scram.SHA256
		hashes[SASL_SCRAM_SHA512] = scram.SHA512

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
	default:
		// Should we fail silently?
		return nil, nil
	}
}

// GetTLSConfig creates a TLS config from the given TLS config struct and checks for errors
func GetTLSConfig(tlsConfig TLSConfig) (*tls.Config, *Xk6KafkaError) {
	var tlsObject *tls.Config

	if tlsConfig.EnableTLS {
		// TODO: Export them as module level constants (flags)
		TLSVersions := make(map[string]uint16)
		TLSVersions[TLSv10] = tls.VersionTLS10
		TLSVersions[TLSv11] = tls.VersionTLS11
		TLSVersions[TLSv12] = tls.VersionTLS12
		TLSVersions[TLSv13] = tls.VersionTLS13

		// Create a TLS config with default settings
		tlsObject = &tls.Config{
			InsecureSkipVerify: tlsConfig.InsecureSkipTLSVerify,
			MinVersion:         tls.VersionTLS12,
		}

		// Set the minimum TLS version
		if tlsConfig.MinVersion != "" {
			if minVersion, ok := TLSVersions[tlsConfig.MinVersion]; ok {
				tlsObject.MinVersion = minVersion
			} else {
				return nil, NewXk6KafkaError(
					invalidTLSVersion,
					"Unable to create TLS config, because the TLS version is invalid", nil)
			}
		}

		if tlsConfig.ClientCertPem == "" &&
			tlsConfig.ClientKeyPem == "" &&
			tlsConfig.ServerCaPem == "" {
			return tlsObject, nil
		}
	} else {
		// TLS is disabled, and we continue with a unauthenticated dialer
		return nil, NewXk6KafkaError(
			noTLSConfig, "No TLS config provided. Continuing with TLS disabled.", nil)
	}

	// Load the client cert and key if they are provided
	var clientCertFile = &tlsConfig.ClientCertPem
	if !FileExists(*clientCertFile) {
		return nil, NewXk6KafkaError(
			fileNotFound,
			"Client certificate file not found. Continuing with default TLS settings.", nil)
	}

	var clientKeyFile = &tlsConfig.ClientKeyPem
	if !FileExists(*clientKeyFile) {
		return nil, NewXk6KafkaError(
			fileNotFound,
			"Client key file not found. Continuing with default TLS settings.", nil)
	}

	var cert, err = tls.LoadX509KeyPair(*clientCertFile, *clientKeyFile)
	if err != nil {
		return nil, NewXk6KafkaError(
			failedLoadX509KeyPair,
			fmt.Sprintf("Error creating x509 key pair from client cert file \"%s\" and client key file \"%s\". Continuing with default TLS settings.", *clientCertFile, *clientKeyFile),
			err)
	}

	// Load the CA cert if it is provided
	var caCertFile = &tlsConfig.ServerCaPem
	if !FileExists(*caCertFile) {
		return nil, NewXk6KafkaError(
			fileNotFound,
			"CA certificate file not found. Continuing with default TLS settings.", nil)
	}

	caCert, err := os.ReadFile(*caCertFile)
	if err != nil {
		// This might happen on permissions issues or if the file is unreadable somehow
		return nil, NewXk6KafkaError(
			failedReadCaCertFile,
			fmt.Sprintf(
				"Error reading CA certificate file \"%s\". Continuing with default TLS settings.",
				*caCertFile),
			err)
	}
	caCertPool := x509.NewCertPool()
	if ok := caCertPool.AppendCertsFromPEM(caCert); !ok {
		return nil, NewXk6KafkaError(
			failedAppendCaCertFile,
			fmt.Sprintf(
				"Error appending CA certificate file \"%s\". Continuing with default TLS settings.",
				*caCertFile),
			nil)
	}

	tlsObject.Certificates = []tls.Certificate{cert}
	tlsObject.RootCAs = caCertPool
	return tlsObject, nil
}

// FileExists returns true if the given file exists
func FileExists(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil
}
