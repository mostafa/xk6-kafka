package kafka

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/dop251/goja"
	"github.com/pavlo-v-chernykh/keystore-go/v4"
	"go.k6.io/k6/js/common"
)

type JKSConfig struct {
	Path              string `json:"path"`
	Password          string `json:"password"`
	ClientCertAlias   string `json:"clientCertAlias"`
	ClientKeyAlias    string `json:"clientKeyAlias"`
	ClientKeyPassword string `json:"clientKeyPassword"`
	ServerCaAlias     string `json:"serverCaAlias"`
}

type JKS struct {
	ClientCertsPem []string `json:"clientCertsPem"`
	ClientKeyPem   string   `json:"clientKeyPem"`
	ServerCaPem    string   `json:"serverCaPem"`
}

func (*Kafka) loadJKS(jksConfig *JKSConfig) (*JKS, *Xk6KafkaError) {
	if jksConfig == nil {
		return nil, nil
	}

	if err := fileExists(jksConfig.Path); err != nil {
		return nil, NewXk6KafkaError(
			fileNotFound, fmt.Sprintf("File not found: %s", jksConfig.Path), err)
	}

	jksFile, err := os.Open(jksConfig.Path)
	if err != nil {
		return nil, NewXk6KafkaError(
			failedReadJKSFile, fmt.Sprintf("Failed to read JKS file: %s", jksConfig.Path), err)
	}

	ks := keystore.New()
	if err := ks.Load(jksFile, []byte(jksConfig.Password)); err != nil {
		return nil, NewXk6KafkaError(
			failedDecodeJKSFile, fmt.Sprintf("Failed to decode JKS file: %s", jksConfig.Path), err)
	}

	serverCa, err := ks.GetTrustedCertificateEntry(jksConfig.ServerCaAlias)
	if err != nil {
		return nil, NewXk6KafkaError(
			failedDecodeServerCa,
			fmt.Sprintf("Failed to decode server's CA: %s", jksConfig.Path), err)
	}

	clientKey, err := ks.GetPrivateKeyEntry(
		jksConfig.ClientKeyAlias, []byte(jksConfig.ClientKeyPassword))
	if err != nil {
		// Server CA is loaded, so it will be returned, even though the client key is not loaded.
		// This is because one might not have enabled mutual TLS (mTLS).
		return &JKS{
				ClientCertsPem: nil,
				ClientKeyPem:   "",
				ServerCaPem:    string(serverCa.Certificate.Content),
			}, NewXk6KafkaError(
				failedDecodePrivateKey,
				fmt.Sprintf("Failed to decode client's private key: %s", jksConfig.Path), err)
	}

	clientKeyPemFilenames := make([]string, 0, len(clientKey.CertificateChain))
	for idx, cert := range clientKey.CertificateChain {
		filename := fmt.Sprintf("client-cert-%d.pem", idx)
		if err := ioutil.WriteFile(filename, cert.Content, 0644); err != nil {
			return nil, NewXk6KafkaError(
				failedWriteCertFile, "Failed to write cert file", err)
		}
		clientKeyPemFilenames = append(clientKeyPemFilenames, filename)
	}

	clientKeyPemFilename := "client-key.pem"
	if err := ioutil.WriteFile(clientKeyPemFilename, clientKey.PrivateKey, 0644); err != nil {
		return nil, NewXk6KafkaError(
			failedWriteKeyFile, "Failed to write key file", err)
	}

	serverCaPemFilename := "server-ca.pem"
	if err := ioutil.WriteFile(
		serverCaPemFilename, serverCa.Certificate.Content, 0644); err != nil {
		return nil, NewXk6KafkaError(
			failedWriteServerCaFile, "Failed to write server CA file", err)
	}

	return &JKS{
		ClientCertsPem: clientKeyPemFilenames,
		ClientKeyPem:   clientKeyPemFilename,
		ServerCaPem:    serverCaPemFilename,
	}, nil
}

func (k *Kafka) loadJKSFunction(call goja.FunctionCall) goja.Value {
	runtime := k.vu.Runtime()
	var jksConfig *JKSConfig

	if len(call.Arguments) == 0 {
		common.Throw(runtime, ErrNotEnoughArguments)
	}

	if params, ok := call.Argument(0).Export().(map[string]interface{}); ok {
		if b, err := json.Marshal(params); err != nil {
			common.Throw(runtime, err)
		} else {
			if err = json.Unmarshal(b, &jksConfig); err != nil {
				common.Throw(runtime, err)
			}
		}
	}

	if jksConfig == nil {
		common.Throw(runtime, ErrNoJKSConfig)
	}

	jks, err := k.loadJKS(jksConfig)
	if err != nil {
		common.Throw(runtime, err)
	}

	obj := runtime.NewObject()
	// Zero-value fields are not returned to JS.
	if jks.ServerCaPem != "" {
		if err := obj.Set("serverCaPem", jks.ServerCaPem); err != nil {
			common.Throw(runtime, err)
		}
	}
	if jks.ClientCertsPem != nil {
		if err := obj.Set("clientCertsPem", jks.ClientCertsPem); err != nil {
			common.Throw(runtime, err)
		}
	}
	if jks.ClientKeyPem != "" {
		if err := obj.Set("clientKeyPem", jks.ClientKeyPem); err != nil {
			common.Throw(runtime, err)
		}
	}
	return runtime.ToValue(obj)
}
