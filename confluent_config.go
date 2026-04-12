package kafka

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const defaultConfluentTimeout = 5 * time.Second

func ensureContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}

	return ctx
}

func cloneConfluentConfigMap(src ckafka.ConfigMap) ckafka.ConfigMap {
	dst := make(ckafka.ConfigMap, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func newConfluentConfigMap(brokers []string) (ckafka.ConfigMap, error) {
	if len(brokers) == 0 {
		return nil, newInvalidConfigError("confluent config", errors.New("brokers must not be empty"))
	}

	return ckafka.ConfigMap{
		"bootstrap.servers": strings.Join(brokers, ","),
	}, nil
}

func setConfluentConfigValue(config ckafka.ConfigMap, key string, value any) error {
	return config.SetKey(key, value)
}

func applyConfluentSecurityConfig(
	config ckafka.ConfigMap,
	saslConfig SASLConfig,
	tlsConfig TLSConfig,
) error {
	securityProtocol, err := confluentSecurityProtocol(saslConfig, tlsConfig)
	if err != nil {
		return err
	}
	if err := setConfluentConfigValue(config, "security.protocol", securityProtocol); err != nil {
		return err
	}

	if tlsConfig.EnableTLS {
		if tlsConfig.ServerCaPem != "" {
			if err := setConfluentConfigValue(config, "ssl.ca.pem", tlsConfig.ServerCaPem); err != nil {
				return err
			}
		}
		if tlsConfig.ClientCertPem != "" {
			if err := setConfluentConfigValue(config, "ssl.certificate.pem", tlsConfig.ClientCertPem); err != nil {
				return err
			}
		}
		if tlsConfig.ClientKeyPem != "" {
			if err := setConfluentConfigValue(config, "ssl.key.pem", tlsConfig.ClientKeyPem); err != nil {
				return err
			}
		}
		if tlsConfig.InsecureSkipTLSVerify {
			if err := setConfluentConfigValue(config, "enable.ssl.certificate.verification", false); err != nil {
				return err
			}
		}
	}

	if saslConfig.Algorithm == "" || saslConfig.Algorithm == none {
		return nil
	}

	mechanism, err := confluentSASLMechanism(saslConfig)
	if err != nil {
		return err
	}
	if err := setConfluentConfigValue(config, "sasl.mechanism", mechanism); err != nil {
		return err
	}

	switch saslConfig.Algorithm {
	case saslPlain, saslSsl, saslScramSha256, saslScramSha512:
		if err := setConfluentConfigValue(config, "sasl.username", saslConfig.Username); err != nil {
			return err
		}
		if err := setConfluentConfigValue(config, "sasl.password", saslConfig.Password); err != nil {
			return err
		}
	case saslAwsIam:
		return NewXk6KafkaError(
			unsupportedOperation,
			"AWS IAM SASL is not wired for the Confluent scaffold yet",
			nil,
		)
	}

	return nil
}

func confluentSecurityProtocol(saslConfig SASLConfig, tlsConfig TLSConfig) (string, error) {
	switch saslConfig.Algorithm {
	case "", none:
		if tlsConfig.EnableTLS {
			return "SSL", nil
		}
		return "PLAINTEXT", nil
	case saslSsl:
		if !tlsConfig.EnableTLS {
			return "", NewXk6KafkaError(
				failedCreateDialerWithSaslSSL,
				"You must enable TLS to use SASL_SSL",
				nil,
			)
		}
		return "SASL_SSL", nil
	case saslPlain, saslScramSha256, saslScramSha512, saslAwsIam:
		if tlsConfig.EnableTLS {
			return "SASL_SSL", nil
		}
		return "SASL_PLAINTEXT", nil
	default:
		return "", NewXk6KafkaError(
			unsupportedOperation,
			"Unsupported SASL mechanism for Confluent scaffold",
			nil,
		)
	}
}

func confluentSASLMechanism(saslConfig SASLConfig) (string, error) {
	switch saslConfig.Algorithm {
	case saslPlain, saslSsl:
		return "PLAIN", nil
	case saslScramSha256:
		return "SCRAM-SHA-256", nil
	case saslScramSha512:
		return "SCRAM-SHA-512", nil
	case saslAwsIam:
		return "AWS_MSK_IAM", nil
	default:
		return "", NewXk6KafkaError(
			unsupportedOperation,
			"Unsupported SASL mechanism for Confluent scaffold",
			nil,
		)
	}
}

func writerConfigToConfluentConfigMap(writerConfig *WriterConfig) (ckafka.ConfigMap, error) {
	if writerConfig == nil {
		return nil, newMissingConfigError("writer config")
	}

	config, err := newConfluentConfigMap(writerConfig.Brokers)
	if err != nil {
		return nil, err
	}

	if err := applyConfluentSecurityConfig(config, writerConfig.SASL, writerConfig.TLS); err != nil {
		return nil, err
	}

	if writerConfig.BatchSize > 0 {
		if err := setConfluentConfigValue(config, "batch.num.messages", writerConfig.BatchSize); err != nil {
			return nil, err
		}
	}
	if writerConfig.BatchBytes > 0 {
		if err := setConfluentConfigValue(config, "batch.size", writerConfig.BatchBytes); err != nil {
			return nil, err
		}
	}
	if writerConfig.BatchTimeout > 0 {
		if err := setConfluentConfigValue(config, "linger.ms", int(writerConfig.BatchTimeout.Milliseconds())); err != nil {
			return nil, err
		}
	}
	if writerConfig.WriteTimeout > 0 {
		if err := setConfluentConfigValue(config, "message.timeout.ms", int(writerConfig.WriteTimeout.Milliseconds())); err != nil {
			return nil, err
		}
	}
	if writerConfig.ReadTimeout > 0 {
		if err := setConfluentConfigValue(config, "socket.timeout.ms", int(writerConfig.ReadTimeout.Milliseconds())); err != nil {
			return nil, err
		}
	}
	if writerConfig.Compression != "" {
		if err := setConfluentConfigValue(config, "compression.type", writerConfig.Compression); err != nil {
			return nil, err
		}
	}
	acksValue, err := confluentRequiredAcks(writerConfig.RequiredAcks)
	if err != nil {
		return nil, err
	}
	if err := setConfluentConfigValue(config, "acks", acksValue); err != nil {
		return nil, err
	}

	return config, nil
}

func readerConfigToConfluentConfigMap(readerConfig *ReaderConfig) (ckafka.ConfigMap, error) {
	if readerConfig == nil {
		return nil, newMissingConfigError("reader config")
	}

	config, err := newConfluentConfigMap(readerConfig.Brokers)
	if err != nil {
		return nil, err
	}

	if err := applyConfluentSecurityConfig(config, readerConfig.SASL, readerConfig.TLS); err != nil {
		return nil, err
	}

	if readerConfig.MinBytes > 0 {
		if err := setConfluentConfigValue(config, "fetch.min.bytes", readerConfig.MinBytes); err != nil {
			return nil, err
		}
	}
	if readerConfig.MaxBytes > 0 {
		if err := setConfluentConfigValue(config, "fetch.max.bytes", readerConfig.MaxBytes); err != nil {
			return nil, err
		}
	}
	if readerConfig.MaxWait.Duration > 0 {
		if err := setConfluentConfigValue(config, "fetch.wait.max.ms", int(readerConfig.MaxWait.Duration.Milliseconds())); err != nil {
			return nil, err
		}
	}
	if readerConfig.SessionTimeout > 0 {
		if err := setConfluentConfigValue(config, "session.timeout.ms", int(readerConfig.SessionTimeout.Milliseconds())); err != nil {
			return nil, err
		}
	}
	if readerConfig.HeartbeatInterval > 0 {
		if err := setConfluentConfigValue(config, "heartbeat.interval.ms", int(readerConfig.HeartbeatInterval.Milliseconds())); err != nil {
			return nil, err
		}
	}
	if readerConfig.CommitInterval > 0 {
		if err := setConfluentConfigValue(config, "auto.commit.interval.ms", int(readerConfig.CommitInterval.Milliseconds())); err != nil {
			return nil, err
		}
	}

	if readerConfig.GroupID != "" {
		if err := setConfluentConfigValue(config, "group.id", readerConfig.GroupID); err != nil {
			return nil, err
		}
		if err := setConfluentConfigValue(config, "enable.auto.commit", false); err != nil {
			return nil, err
		}

		autoOffsetReset := "earliest"
		switch readerConfig.StartOffset {
		case "", firstOffset:
			autoOffsetReset = "earliest"
		case lastOffset:
			autoOffsetReset = "latest"
		default:
			if _, parseErr := strconv.ParseInt(readerConfig.StartOffset, 10, 64); parseErr == nil {
				autoOffsetReset = "earliest"
			}
		}

		if err := setConfluentConfigValue(config, "auto.offset.reset", autoOffsetReset); err != nil {
			return nil, err
		}
	}

	return config, nil
}

func connectionConfigToConfluentConfigMap(connectionConfig *ConnectionConfig) (ckafka.ConfigMap, error) {
	if connectionConfig == nil {
		return nil, newMissingConfigError("connection config")
	}
	brokers := append([]string(nil), connectionConfig.Brokers...)
	if len(brokers) == 0 {
		if connectionConfig.Address == "" {
			return nil, newInvalidConfigError("connection config", errors.New("address must not be empty"))
		}
		brokers = []string{connectionConfig.Address}
	}
	if len(brokers) == 0 {
		return nil, newInvalidConfigError("connection config", errors.New("address must not be empty"))
	}

	config, err := newConfluentConfigMap(brokers)
	if err != nil {
		return nil, err
	}

	if err := applyConfluentSecurityConfig(config, connectionConfig.SASL, connectionConfig.TLS); err != nil {
		return nil, err
	}

	return config, nil
}

func confluentRequiredAcks(requiredAcks int) (string, error) {
	switch requiredAcks {
	case -1:
		return "all", nil
	case 0:
		return "0", nil
	case 1:
		return "1", nil
	default:
		return "", newInvalidConfigError(
			"writer config",
			errors.New("requiredAcks must be one of -1, 0, or 1"),
		)
	}
}

func confluentOffset(startOffset string, explicitOffset int64) (ckafka.Offset, error) {
	if explicitOffset > 0 {
		return ckafka.Offset(explicitOffset), nil
	}

	switch startOffset {
	case "", firstOffset:
		return ckafka.OffsetBeginning, nil
	case lastOffset:
		return ckafka.OffsetEnd, nil
	default:
		offset, err := strconv.ParseInt(startOffset, 10, 64)
		if err != nil {
			return ckafka.OffsetBeginning, newInvalidConfigError(
				"reader config",
				errors.New("startOffset must be FIRST_OFFSET, LAST_OFFSET, or a numeric offset"),
			)
		}
		return ckafka.Offset(offset), nil
	}
}

func confluentPollTimeout(ctx context.Context) time.Duration {
	const maxPollStep = 100 * time.Millisecond
	ctx = ensureContext(ctx)

	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return 0
		}
		if remaining < maxPollStep {
			return remaining
		}
	}

	return maxPollStep
}

func confluentMetadataTimeoutMs(ctx context.Context) int {
	ctx = ensureContext(ctx)

	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return 0
		}
		return int(remaining.Milliseconds())
	}

	return int(defaultConfluentTimeout.Milliseconds())
}
