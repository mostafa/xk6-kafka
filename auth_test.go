package kafka

import (
	"os"
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
	dialer := getDialerFromCreds(creds)
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
	dialer := getDialerFromCreds(creds)
	assert.NotNil(t, dialer)
	assert.Equal(t, 10*time.Second, dialer.Timeout)
	assert.Equal(t, true, dialer.DualStack)
	assert.Nil(t, dialer.TLS)
	assert.Equal(t, scram.SHA256.Name(), dialer.SASLMechanism.Name())
}

func TestGetDialerFromCredsFails(t *testing.T) {
	// backup of the real stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	defer w.Close()
	defer r.Close()
	os.Stdout = w

	creds := &Credentials{
		Username:  "https://www.exa\t\r\n",
		Password:  "test",
		Algorithm: "sha256",
	}
	dialer := getDialerFromCreds(creds)
	assert.Nil(t, dialer)

	errMsg := "Unable to create SCRAM mechanism from given credentials: Error SASLprepping username 'https://www.exa\t\r\n': prohibited character (rune: '\\u0009')"
	// length of the string without	the newline character
	var buf []byte = make([]byte, len(errMsg))
	// read the output of fmt.Printf to os.Stdout from the pipe
	r.Read(buf)

	assert.Equal(t, errMsg, string(buf))

	// restore the real stdout
	os.Stdout = oldStdout
}

func TestGetDialerFromAuth(t *testing.T) {
	auth := `{"username": "test", "password": "test", "algorithm": "plain", "clientCertPem": "client.pem", "clientKeyPem": "key.pem", "serverCaPem": "server.pem"}`
	dialer := getDialerFromAuth(auth)
	assert.NotNil(t, dialer)
	assert.Equal(t, 10*time.Second, dialer.Timeout)
	assert.Equal(t, true, dialer.DualStack)
	assert.Nil(t, dialer.TLS)
	assert.Equal(t, "PLAIN", dialer.SASLMechanism.Name())
}

func TestGetDialerFromAuthNoAuthString(t *testing.T) {
	dialer := getDialerFromAuth("")
	assert.NotNil(t, dialer)
	assert.Equal(t, 10*time.Second, dialer.Timeout)
	assert.Equal(t, false, dialer.DualStack)
	assert.Nil(t, dialer.TLS)
}

func TestFileExists(t *testing.T) {
	assert.True(t, fileExists("auth_test.go"))
	assert.False(t, fileExists("test.go.not"))
}
