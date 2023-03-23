package kafka

import (
	"encoding/json"
	"fmt"
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
		// Server CA is loaded and returnedm, even if client key is not loaded.
		// This is because one might not have enabled mutual TLS (mTLS).
		return &JKS{
				ClientCertsPem: nil,
				ClientKeyPem:   "",
				ServerCaPem:    string(serverCa.Certificate.Content),
			}, NewXk6KafkaError(
				failedDecodePrivateKey,
				fmt.Sprintf("Failed to decode client's private key: %s", jksConfig.Path), err)
	}

	clientCertsChain := make([]string, 0, len(clientKey.CertificateChain))
	for _, cert := range clientKey.CertificateChain {
		clientCertsChain = append(clientCertsChain, string(cert.Content))
	}

	return &JKS{
		ClientCertsPem: clientCertsChain,
		ClientKeyPem:   string(clientKey.PrivateKey),
		ServerCaPem:    string(serverCa.Certificate.Content),
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
	obj.Set("clientCertsPem", jks.ClientCertsPem)
	obj.Set("clientKeyPem", jks.ClientKeyPem)
	obj.Set("serverCaPem", jks.ServerCaPem)
	return runtime.ToValue(obj)
}
