package kafka

import (
	"crypto/tls"
	"encoding/json"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

const (
	Plain  = "plain"
	SHA256 = "sha256"
	SHA512 = "sha512"
)

type Credentials struct {
	Username  string `json:"username"`
	Password  string `json:"password"`
	Algorithm string `json:"algorithm"`
}

func unmarshalCredentials(auth string) (creds *Credentials, err error) {
	creds = &Credentials{
		Algorithm: Plain,
	}

	err = json.Unmarshal([]byte(auth), &creds)

	return
}

func getDialer(creds *Credentials) (dialer *kafkago.Dialer) {
	dialer = &kafkago.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		TLS: &tls.Config{},
	}

	if creds.Algorithm == Plain {
		mechanism := plain.Mechanism{
			Username: creds.Username,
			Password: creds.Password,
		}
		dialer.SASLMechanism = mechanism
		return
	} else {
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
}
