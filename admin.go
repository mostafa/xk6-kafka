package kafka

import (
	"context"
	"errors"
	"sort"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type TopicConfig struct {
	Topic             string
	NumPartitions     int
	ReplicationFactor int
	ReplicaAssignment [][]int32
	Config            map[string]string
}

type TopicInfo struct {
	Topic      string
	Partitions int
	Error      error
}

type PartitionInfo struct {
	ID       int32
	Leader   int32
	Replicas []int32
	Isrs     []int32
	Error    error
}

type TopicMetadata struct {
	Topic      string
	Partitions []PartitionInfo
	Error      error
}

type AdminClient struct {
	client *ckafka.AdminClient
	config ckafka.ConfigMap
}

func NewAdminClientFromConnectionConfig(connectionConfig *ConnectionConfig) (*AdminClient, error) {
	config, err := connectionConfigToConfluentConfigMap(connectionConfig)
	if err != nil {
		return nil, err
	}

	client, err := ckafka.NewAdminClient(&config)
	if err != nil {
		return nil, NewXk6KafkaError(failedCreateAdminClient, "Failed to create admin client.", err)
	}

	return &AdminClient{
		client: client,
		config: cloneConfluentConfigMap(config),
	}, nil
}

func (a *AdminClient) CreateTopic(ctx context.Context, config TopicConfig) error {
	if a == nil || a.client == nil {
		return newMissingConfigError("admin client")
	}
	ctx = ensureContext(ctx)
	if config.Topic == "" {
		return newInvalidConfigError("topic config", errors.New("topic must not be empty"))
	}
	if config.NumPartitions <= 0 {
		config.NumPartitions = 1
	}
	if config.ReplicationFactor <= 0 {
		config.ReplicationFactor = 1
	}

	results, err := a.client.CreateTopics(ctx, []ckafka.TopicSpecification{{
		Topic:             config.Topic,
		NumPartitions:     config.NumPartitions,
		ReplicationFactor: config.ReplicationFactor,
		ReplicaAssignment: config.ReplicaAssignment,
		Config:            config.Config,
	}})
	if err != nil {
		return NewXk6KafkaError(failedCreateTopic, "Failed to create topic.", err)
	}

	return adminTopicResultError(results, failedCreateTopic)
}

func (a *AdminClient) DeleteTopic(ctx context.Context, name string) error {
	if a == nil || a.client == nil {
		return newMissingConfigError("admin client")
	}
	ctx = ensureContext(ctx)
	if name == "" {
		return newInvalidConfigError("topic config", errors.New("topic must not be empty"))
	}

	results, err := a.client.DeleteTopics(ctx, []string{name})
	if err != nil {
		return NewXk6KafkaError(failedDeleteTopic, "Failed to delete topic.", err)
	}

	return adminTopicResultError(results, failedDeleteTopic)
}

func (a *AdminClient) ListTopics(ctx context.Context) ([]TopicInfo, error) {
	if a == nil || a.client == nil {
		return nil, newMissingConfigError("admin client")
	}
	ctx = ensureContext(ctx)

	metadata, err := a.client.GetMetadata(nil, true, confluentMetadataTimeoutMs(ctx))
	if err != nil {
		return nil, NewXk6KafkaError(failedGetMetadata, "Failed to list topic metadata.", err)
	}

	topics := make([]TopicInfo, 0, len(metadata.Topics))
	for _, topicMetadata := range metadata.Topics {
		topics = append(topics, TopicInfo{
			Topic:      topicMetadata.Topic,
			Partitions: len(topicMetadata.Partitions),
			Error:      normalizeConfluentError(topicMetadata.Error),
		})
	}

	sort.Slice(topics, func(i, j int) bool {
		return topics[i].Topic < topics[j].Topic
	})

	return topics, nil
}

func (a *AdminClient) GetMetadata(ctx context.Context, topic string) (*TopicMetadata, error) {
	if a == nil || a.client == nil {
		return nil, newMissingConfigError("admin client")
	}
	ctx = ensureContext(ctx)
	if topic == "" {
		return nil, newInvalidConfigError("topic config", errors.New("topic must not be empty"))
	}

	metadata, err := a.client.GetMetadata(&topic, false, confluentMetadataTimeoutMs(ctx))
	if err != nil {
		return nil, NewXk6KafkaError(failedGetMetadata, "Failed to get topic metadata.", err)
	}

	topicMetadata, ok := metadata.Topics[topic]
	if !ok {
		return nil, NewXk6KafkaError(failedGetMetadata, "Topic metadata was not returned.", errors.New("topic metadata not found"))
	}

	converted := &TopicMetadata{
		Topic:      topicMetadata.Topic,
		Partitions: make([]PartitionInfo, 0, len(topicMetadata.Partitions)),
		Error:      normalizeConfluentError(topicMetadata.Error),
	}

	for _, partition := range topicMetadata.Partitions {
		converted.Partitions = append(converted.Partitions, PartitionInfo{
			ID:       partition.ID,
			Leader:   partition.Leader,
			Replicas: append([]int32(nil), partition.Replicas...),
			Isrs:     append([]int32(nil), partition.Isrs...),
			Error:    normalizeConfluentError(partition.Error),
		})
	}

	return converted, nil
}

func (a *AdminClient) Close() error {
	if a == nil || a.client == nil {
		return nil
	}

	a.client.Close()
	return nil
}

func adminTopicResultError(results []ckafka.TopicResult, code errCode) error {
	if len(results) == 0 {
		return NewXk6KafkaError(code, "Admin operation returned no topic results.", errors.New("empty topic result set"))
	}

	result := results[0]
	if result.Error.Code() == ckafka.ErrNoError {
		return nil
	}

	return NewXk6KafkaError(code, "Admin topic operation failed.", result.Error)
}

func normalizeConfluentError(err ckafka.Error) error {
	if err.Code() == ckafka.ErrNoError {
		return nil
	}

	return err
}
