package kafka

import (
	"context"
	"errors"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type ConsumerStats struct {
	Assignments int
}

type Consumer struct {
	client *ckafka.Consumer
	config ckafka.ConfigMap
	topic  string
}

func NewConsumerFromReaderConfig(readerConfig *ReaderConfig) (*Consumer, error) {
	config, err := readerConfigToConfluentConfigMap(readerConfig)
	if err != nil {
		return nil, err
	}

	client, err := ckafka.NewConsumer(&config)
	if err != nil {
		return nil, NewXk6KafkaError(failedCreateConsumer, "Failed to create consumer.", err)
	}

	consumer := &Consumer{
		client: client,
		config: cloneConfluentConfigMap(config),
	}

	if readerConfig == nil {
		return nil, newMissingConfigError("reader config")
	}

	switch {
	case readerConfig.GroupID != "":
		topics := append([]string(nil), readerConfig.GroupTopics...)
		if len(topics) == 0 && readerConfig.Topic != "" {
			topics = []string{readerConfig.Topic}
		}
		if len(topics) == 0 {
			_ = client.Close()
			return nil, newInvalidConfigError("reader config", errors.New("groupTopics must not be empty"))
		}
		if len(topics) == 1 {
			consumer.topic = topics[0]
		}
		if err := client.SubscribeTopics(topics, nil); err != nil {
			_ = client.Close()
			return nil, NewXk6KafkaError(failedCreateConsumer, "Failed to subscribe consumer.", err)
		}
	default:
		if readerConfig.Topic == "" {
			_ = client.Close()
			return nil, newInvalidConfigError("reader config", errors.New("topic must not be empty"))
		}

		offset, err := confluentOffset(readerConfig.StartOffset, readerConfig.Offset)
		if err != nil {
			_ = client.Close()
			return nil, err
		}

		consumer.topic = readerConfig.Topic
		if err := client.Assign([]ckafka.TopicPartition{{
			Topic:     &consumer.topic,
			Partition: int32(readerConfig.Partition),
			Offset:    offset,
		}}); err != nil {
			_ = client.Close()
			return nil, NewXk6KafkaError(failedCreateConsumer, "Failed to assign consumer.", err)
		}
	}

	return consumer, nil
}

func (c *Consumer) Consume(ctx context.Context, limit int) ([]Message, error) {
	if c == nil || c.client == nil {
		return nil, newMissingConfigError("consumer")
	}
	if limit <= 0 {
		limit = 1
	}
	ctx = ensureContext(ctx)

	messages := make([]Message, 0, limit)
	for len(messages) < limit {
		if err := ctx.Err(); err != nil {
			return messages, NewXk6KafkaError(failedReadMessage, "Consumer context cancelled.", err)
		}

		timeout := confluentPollTimeout(ctx)
		if timeout == 0 {
			return messages, NewXk6KafkaError(failedReadMessage, "Consumer context cancelled.", ctx.Err())
		}

		msg, err := c.client.ReadMessage(timeout)
		if err != nil {
			var kafkaErr ckafka.Error
			if errors.As(err, &kafkaErr) && kafkaErr.IsTimeout() {
				continue
			}
			return messages, NewXk6KafkaError(failedReadMessage, "Failed to consume message.", err)
		}

		messages = append(messages, confluentMessageToMessage(msg))
	}

	return messages, nil
}

func (c *Consumer) Seek(partition int, offset int64) error {
	if c == nil || c.client == nil {
		return newMissingConfigError("consumer")
	}
	if c.topic == "" {
		return newInvalidConfigError("consumer", errors.New("seek requires a single configured topic"))
	}

	err := c.client.Seek(ckafka.TopicPartition{
		Topic:     &c.topic,
		Partition: int32(partition),
		Offset:    ckafka.Offset(offset),
	}, -1)
	if err != nil {
		return NewXk6KafkaError(failedSetOffset, "Failed to seek consumer offset.", err)
	}

	return nil
}

func (c *Consumer) Position(partition int) (int64, error) {
	if c == nil || c.client == nil {
		return 0, newMissingConfigError("consumer")
	}
	if c.topic == "" {
		return 0, newInvalidConfigError("consumer", errors.New("position requires a single configured topic"))
	}

	positions, err := c.client.Position([]ckafka.TopicPartition{{
		Topic:     &c.topic,
		Partition: int32(partition),
	}})
	if err != nil {
		return 0, NewXk6KafkaError(failedSetOffset, "Failed to query consumer position.", err)
	}
	if len(positions) == 0 {
		return 0, NewXk6KafkaError(failedSetOffset, "Failed to query consumer position.", errors.New("no positions returned"))
	}

	return int64(positions[0].Offset), nil
}

func (c *Consumer) CommitOffsets(ctx context.Context) error {
	if c == nil || c.client == nil {
		return newMissingConfigError("consumer")
	}
	ctx = ensureContext(ctx)
	if err := ctx.Err(); err != nil {
		return NewXk6KafkaError(failedCommitConsumer, "Consumer context cancelled.", err)
	}

	if _, err := c.client.Commit(); err != nil {
		return NewXk6KafkaError(failedCommitConsumer, "Failed to commit consumer offsets.", err)
	}

	return nil
}

func (c *Consumer) Close() error {
	if c == nil || c.client == nil {
		return nil
	}

	if err := c.client.Close(); err != nil {
		return NewXk6KafkaError(failedCreateConsumer, "Failed to close consumer.", err)
	}
	return nil
}

func (c *Consumer) Stats() ConsumerStats {
	if c == nil || c.client == nil {
		return ConsumerStats{}
	}

	assignments, err := c.client.Assignment()
	if err != nil {
		return ConsumerStats{}
	}

	return ConsumerStats{Assignments: len(assignments)}
}

func confluentMessageToMessage(msg *ckafka.Message) Message {
	converted := Message{
		Key:       msg.Key,
		Value:     msg.Value,
		Time:      msg.Timestamp,
		Partition: int(msg.TopicPartition.Partition),
		Offset:    int64(msg.TopicPartition.Offset),
		Headers:   map[string]any{},
	}

	if msg.TopicPartition.Topic != nil {
		converted.Topic = *msg.TopicPartition.Topic
	}

	for _, header := range msg.Headers {
		converted.Headers[header.Key] = header.Value
	}

	return converted
}
