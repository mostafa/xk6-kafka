package kafka

import (
	"errors"
	"strings"

	"github.com/segmentio/kafka-go"
	kafkago "github.com/segmentio/kafka-go"
)

func (k *Kafka) CreateTopic(address, topic string, partitions, replicationFactor int, compression string, auth string) error {
	dialer := getAuthenticatedDialer(auth)

	ctx := k.vu.Context()
	err := errors.New("context is nil")

	if ctx == nil {
		ReportError(err, "Cannot determine context")
		return err
	}

	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return err
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

	if _, exists := CompressionCodecs[compression]; exists {
		topicConfig.ConfigEntries = append(topicConfig.ConfigEntries, kafkago.ConfigEntry{
			ConfigName:  "compression.type",
			ConfigValue: strings.ToLower(compression),
		})
	}

	err = conn.CreateTopics([]kafkago.TopicConfig{topicConfig}...)
	if err != nil {
		return err
	}

	return nil
}

func (k *Kafka) ListTopics(address string, auth string) ([]string, error) {
	dialer := getAuthenticatedDialer(auth)

	ctx := k.vu.Context()
	err := errors.New("context is nil")

	if ctx == nil {
		ReportError(err, "Cannot determine context")
		return nil, err
	}

	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		return nil, err
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
