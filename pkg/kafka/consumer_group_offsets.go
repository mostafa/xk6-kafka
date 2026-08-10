package kafka

import (
	"context"
	"fmt"
	"sort"
	"strings"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// ConsumerGroupOffsetsConfig identifies an inactive consumer group and the
// topics whose current end offsets should become its committed offsets.
type ConsumerGroupOffsetsConfig struct {
	GroupID string   `json:"groupId"`
	Topics  []string `json:"topics"`
}

// ConsumerGroupOffset is one topic-partition offset captured and committed by
// InitializeConsumerGroupOffsets.
type ConsumerGroupOffset struct {
	Topic     string
	Partition int32
	Offset    int64
}

// InitializeConsumerGroupOffsets captures the current end offset of every
// partition in config.Topics and commits the complete snapshot for
// config.GroupID. The group must not be actively subscribed to those topics.
func (a *AdminClient) InitializeConsumerGroupOffsets(
	ctx context.Context,
	config ConsumerGroupOffsetsConfig,
) ([]ConsumerGroupOffset, error) {
	if a == nil || a.client == nil {
		return nil, newMissingConfigError("admin client")
	}

	groupID := strings.TrimSpace(config.GroupID)
	if groupID == "" {
		return nil, newInvalidConfigError("consumer group offsets config", errGroupIDMustNotBeEmpty)
	}

	topics, err := normalizedTopics(config.Topics)
	if err != nil {
		return nil, err
	}

	ctx = ensureContext(ctx)
	requests := make(map[ckafka.TopicPartition]ckafka.OffsetSpec)
	for _, topic := range topics {
		metadata, metadataErr := a.GetMetadata(ctx, topic)
		if metadataErr != nil {
			return nil, metadataErr
		}
		if metadata.Error != nil {
			return nil, NewXk6KafkaError(
				failedGetMetadata,
				fmt.Sprintf("Topic metadata failed for %q.", topic),
				metadata.Error,
			)
		}
		for _, partition := range metadata.Partitions {
			if partition.Error != nil {
				return nil, NewXk6KafkaError(
					failedGetMetadata,
					fmt.Sprintf("Partition metadata failed for %s[%d].", topic, partition.ID),
					partition.Error,
				)
			}
			topicName := topic
			requests[ckafka.TopicPartition{Topic: &topicName, Partition: partition.ID}] = ckafka.LatestOffsetSpec
		}
	}

	listed, err := a.client.ListOffsets(ctx, requests)
	if err != nil {
		return nil, NewXk6KafkaError(failedListOffsets, "Failed to list partition end offsets.", err)
	}

	snapshot, err := consumerGroupOffsetSnapshot(requests, listed)
	if err != nil {
		return nil, err
	}

	partitions := make([]ckafka.TopicPartition, 0, len(snapshot))
	for _, item := range snapshot {
		topic := item.Topic
		partitions = append(partitions, ckafka.TopicPartition{
			Topic:     &topic,
			Partition: item.Partition,
			Offset:    ckafka.Offset(item.Offset),
		})
	}

	altered, err := a.client.AlterConsumerGroupOffsets(ctx, []ckafka.ConsumerGroupTopicPartitions{{
		Group:      groupID,
		Partitions: partitions,
	}})
	if err != nil {
		return nil, NewXk6KafkaError(failedAlterGroupOffsets, "Failed to alter consumer group offsets.", err)
	}
	if err := validateAlteredConsumerGroupOffsets(groupID, snapshot, altered); err != nil {
		return nil, err
	}

	return snapshot, nil
}

type topicPartitionKey struct {
	topic     string
	partition int32
}

func normalizedTopics(topics []string) ([]string, error) {
	if len(topics) == 0 {
		return nil, newInvalidConfigError("consumer group offsets config", errTopicsMustNotBeEmpty)
	}

	unique := make(map[string]struct{}, len(topics))
	for _, value := range topics {
		topic := strings.TrimSpace(value)
		if topic == "" {
			return nil, newInvalidConfigError("consumer group offsets config", errTopicMustNotBeEmpty)
		}
		unique[topic] = struct{}{}
	}

	result := make([]string, 0, len(unique))
	for topic := range unique {
		result = append(result, topic)
	}
	sort.Strings(result)
	return result, nil
}

func consumerGroupOffsetSnapshot(
	requests map[ckafka.TopicPartition]ckafka.OffsetSpec,
	result ckafka.ListOffsetsResult,
) ([]ConsumerGroupOffset, error) {
	expected := make(map[topicPartitionKey]struct{}, len(requests))
	for partition := range requests {
		if partition.Topic == nil {
			continue
		}
		expected[topicPartitionKey{topic: *partition.Topic, partition: partition.Partition}] = struct{}{}
	}

	snapshot := make([]ConsumerGroupOffset, 0, len(expected))
	for partition, info := range result.ResultInfos {
		if partition.Topic == nil {
			continue
		}
		key := topicPartitionKey{topic: *partition.Topic, partition: partition.Partition}
		if _, ok := expected[key]; !ok {
			continue
		}
		if info.Error.Code() != ckafka.ErrNoError {
			return nil, NewXk6KafkaError(
				failedListOffsets,
				fmt.Sprintf("Failed to list end offset for %s[%d].", key.topic, key.partition),
				info.Error,
			)
		}
		snapshot = append(snapshot, ConsumerGroupOffset{
			Topic:     key.topic,
			Partition: key.partition,
			Offset:    int64(info.Offset),
		})
		delete(expected, key)
	}

	if len(expected) > 0 {
		missing := make([]string, 0, len(expected))
		for key := range expected {
			missing = append(missing, fmt.Sprintf("%s[%d]", key.topic, key.partition))
		}
		sort.Strings(missing)
		return nil, NewXk6KafkaError(
			failedListOffsets,
			"Partition end offsets were not returned.",
			fmt.Errorf("missing partitions: %s", strings.Join(missing, ", ")),
		)
	}

	sort.Slice(snapshot, func(i, j int) bool {
		if snapshot[i].Topic == snapshot[j].Topic {
			return snapshot[i].Partition < snapshot[j].Partition
		}
		return snapshot[i].Topic < snapshot[j].Topic
	})
	return snapshot, nil
}

func validateAlteredConsumerGroupOffsets(
	groupID string,
	expected []ConsumerGroupOffset,
	result ckafka.AlterConsumerGroupOffsetsResult,
) error {
	if len(result.ConsumerGroupsTopicPartitions) != 1 {
		return NewXk6KafkaError(
			failedAlterGroupOffsets,
			"Consumer group offset operation returned an unexpected number of groups.",
			fmt.Errorf("expected 1 group, got %d", len(result.ConsumerGroupsTopicPartitions)),
		)
	}

	group := result.ConsumerGroupsTopicPartitions[0]
	if group.Group != groupID {
		return NewXk6KafkaError(
			failedAlterGroupOffsets,
			"Consumer group offset operation returned an unexpected group.",
			fmt.Errorf("expected group %q, got %q", groupID, group.Group),
		)
	}

	committed := make(map[topicPartitionKey]struct{}, len(group.Partitions))
	for _, partition := range group.Partitions {
		if partition.Topic == nil {
			continue
		}
		key := topicPartitionKey{topic: *partition.Topic, partition: partition.Partition}
		if partition.Error != nil {
			return NewXk6KafkaError(
				failedAlterGroupOffsets,
				fmt.Sprintf("Failed to alter consumer group offset for %s[%d].", key.topic, key.partition),
				partition.Error,
			)
		}
		committed[key] = struct{}{}
	}

	for _, item := range expected {
		key := topicPartitionKey{topic: item.Topic, partition: item.Partition}
		if _, ok := committed[key]; !ok {
			return NewXk6KafkaError(
				failedAlterGroupOffsets,
				"Consumer group offset result omitted a partition.",
				fmt.Errorf("missing partition: %s[%d]", item.Topic, item.Partition),
			)
		}
	}

	return nil
}
