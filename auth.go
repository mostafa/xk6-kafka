package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

const (
	None   = "none"
	Plain  = "plain"
	SHA256 = "sha256"
	SHA512 = "sha512"
)

type Credentials struct {
	Username      string `json:"username"`
	Password      string `json:"password"`
	Algorithm     string `json:"algorithm"`
	ClientCertPem string `json:"clientCertPem"`
	ClientKeyPem  string `json:"clientKeyPem"`
	ServerCaPem   string `json:"serverCaPem"`
}

func unmarshalCredentials(auth string) (creds *Credentials, err error) {
	creds = &Credentials{
		Algorithm: None,
	}

	err = json.Unmarshal([]byte(auth), &creds)

	return
}

func getDialer(creds *Credentials) (dialer *kafkago.Dialer) {
	dialer = &kafkago.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		TLS:       tlsConfig(creds),
	}

	if creds.Algorithm == Plain {
		mechanism := plain.Mechanism{
			Username: creds.Username,
			Password: creds.Password,
		}
		dialer.SASLMechanism = mechanism
		return
	} else if creds.Algorithm == SHA256 || creds.Algorithm == SHA512 {
		hashes := make(map[string]scram.Algorithm)
		hashes["sha256"] = scram.SHA256
		hashes["sha512"] = scram.SHA512

		mechanism, err := scram.Mechanism(
			hashes[creds.Algorithm],
			creds.Username,
			creds.Password,
		)
		if err != nil {
			ReportError(err, "authentication failed")
			return nil
		}
		dialer.SASLMechanism = mechanism
		return
	}
	return
}

func getAuthenticatedDialer(auth string) (dialer *kafkago.Dialer) {
	if auth != "" {
		creds, err := unmarshalCredentials(auth)
		if err != nil {
			ReportError(err, "Unable to unmarshal credentials")
			return nil
		}

		dialer = getDialer(creds)
		if dialer == nil {
			ReportError(nil, "Dialer cannot authenticate")
			return nil
		}
	}

	return
}

func fileExists(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil
}

func tlsConfig(creds *Credentials) *tls.Config {
	var clientCertFile = &creds.ClientCertPem
	if !fileExists(*clientCertFile) {
		ReportError(nil, "client certificate file not found")
		return nil
	}

	var clientKeyFile = &creds.ClientKeyPem
	if !fileExists(*clientKeyFile) {
		ReportError(nil, "client key file not found")
		return nil
	}

	var cert, err = tls.LoadX509KeyPair(*clientCertFile, *clientKeyFile)
	if err != nil {
		log.Fatalf("Error creating x509 keypair from client cert file %s and client key file %s", *clientCertFile, *clientKeyFile)
	}

	var caCertFile = &creds.ServerCaPem
	if !fileExists(*caCertFile) {
		ReportError(nil, "CA certificate file not found")
		return nil
	}

	caCert, err := ioutil.ReadFile(*caCertFile)
	if err != nil {
		log.Fatalf("Error opening cert file %s, Error: %s", *caCertFile, err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
		MinVersion:   tls.VersionTLS12,
	}
}
