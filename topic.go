package kafka

import (
	"net"
	"strconv"
	"strings"

	"github.com/segmentio/kafka-go"
	kafkago "github.com/segmentio/kafka-go"
)

// GetKafkaControllerConnection returns a kafka controller connection with a given node address.
// It will also try to use the auth and TLS settings to create a secure connection. The connection
// should be closed after use.
func (k *Kafka) GetKafkaControllerConnection(address string, saslConfig SASLConfig, tlsConfig TLSConfig) (*kafkago.Conn, *Xk6KafkaError) {
	dialer, wrappedError := GetDialer(saslConfig, tlsConfig)
	if wrappedError != nil {
		logger.WithField("error", wrappedError).Error(wrappedError)
		if dialer == nil {
			return nil, wrappedError
		}
	}

	ctx := k.vu.Context()
	if ctx == nil {
		err := NewXk6KafkaError(noContextError, "No context.", nil)
		logger.WithField("error", err).Info(err)
		return nil, err
	}

	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		wrappedError := NewXk6KafkaError(dialerError, "Failed to create dialer.", err)
		logger.WithField("error", wrappedError).Error(wrappedError)
		return nil, wrappedError
	}

	controller, err := conn.Controller()
	if err != nil {
		wrappedError := NewXk6KafkaError(failedGetController, "Failed to get controller.", err)
		logger.WithField("error", wrappedError).Error(wrappedError)
		return nil, wrappedError
	}

	controllerConn, err := dialer.DialContext(
		ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		wrappedError := NewXk6KafkaError(failedGetController, "Failed to get controller.", err)
		logger.WithField("error", wrappedError).Error(wrappedError)
		return nil, wrappedError
	}

	return controllerConn, nil
}

// CreateTopic creates a topic with the given name, partitions, replication factor and compression.
// It will also try to use the auth and TLS settings to create a secure connection. If the topic
// already exists, it will do no-op.
func (k *Kafka) CreateTopic(address, topic string, partitions, replicationFactor int, compression string, saslConfig SASLConfig, tlsConfig TLSConfig) *Xk6KafkaError {
	conn, wrappedError := k.GetKafkaControllerConnection(address, saslConfig, tlsConfig)
	if wrappedError != nil {
		return wrappedError
	}
	defer conn.Close()

	if partitions <= 0 {
		partitions = 1
	}

	if replicationFactor <= 0 {
		replicationFactor = 1
	}

	topicConfig := kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: replicationFactor,
	}

	if _, ok := CompressionCodecs[compression]; ok {
		topicConfig.ConfigEntries = append(topicConfig.ConfigEntries, kafkago.ConfigEntry{
			ConfigName:  "compression.type",
			ConfigValue: strings.ToLower(compression),
		})
	}

	err := conn.CreateTopics([]kafkago.TopicConfig{topicConfig}...)
	if err != nil {
		wrappedError := NewXk6KafkaError(failedCreateTopic, "Failed to create topic.", err)
		logger.WithField("error", wrappedError).Error(wrappedError)
		return wrappedError
	}

	return nil
}

// DeleteTopic deletes the given topic from the given address. It will also try to
// use the auth and TLS settings to create a secure connection. If the topic
// does not exist, it will raise an error.
func (k *Kafka) DeleteTopic(address, topic string, saslConfig SASLConfig, tlsConfig TLSConfig) *Xk6KafkaError {
	conn, wrappedError := k.GetKafkaControllerConnection(address, saslConfig, tlsConfig)
	if wrappedError != nil {
		return wrappedError
	}
	defer conn.Close()

	err := conn.DeleteTopics([]string{topic}...)
	if err != nil {
		return NewXk6KafkaError(failedDeleteTopic, "Failed to delete topic.", err)
	}

	return nil
}

// ListTopics lists the topics from the given address. It will also try to
// use the auth and TLS settings to create a secure connection. If the topic
// does not exist, it will raise an error.
func (k *Kafka) ListTopics(address string, saslConfig SASLConfig, tlsConfig TLSConfig) ([]string, *Xk6KafkaError) {
	conn, wrappedError := k.GetKafkaControllerConnection(address, saslConfig, tlsConfig)
	if wrappedError != nil {
		return nil, wrappedError
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		wrappedError := NewXk6KafkaError(failedReadPartitions, "Failed to read partitions.", err)
		logger.WithField("error", wrappedError).Error(wrappedError)
		return nil, wrappedError
	}

	// There should be a better way to return unique set of
	// topics instead of looping over them twice
	topicSet := map[string]struct{}{}

	for _, partition := range partitions {
		topicSet[partition.Topic] = struct{}{}
	}

	topics := make([]string, 0, len(topicSet))
	for topic := range topicSet {
		topics = append(topics, topic)
	}

	return topics, nil
}
