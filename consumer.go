package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type ConsumerStats struct {
	Assignments int
}

type Consumer struct {
	client *ckafka.Consumer
	config ckafka.ConfigMap
	topic  string

	mu          sync.Mutex
	closeCond   *sync.Cond
	activeCalls int
	closing     bool
}

var errConsumerClosing = errors.New("consumer is closing")

func NewConsumerFromReaderConfig(readerConfig *ReaderConfig) (*Consumer, error) {
	config, err := readerConfigToConfluentConfigMap(readerConfig)
	if err != nil {
		return nil, err
	}
	if readerConfig != nil && readerConfig.GroupID == "" {
		if err := setConfluentConfigValue(
			config,
			"group.id",
			fmt.Sprintf("xk6-kafka-reader-%d", time.Now().UnixNano()),
		); err != nil {
			return nil, err
		}
		if err := setConfluentConfigValue(config, "enable.auto.commit", false); err != nil {
			return nil, err
		}
	}

	client, err := ckafka.NewConsumer(&config)
	if err != nil {
		return nil, NewXk6KafkaError(failedCreateConsumer, "Failed to create consumer.", err)
	}

	consumer := &Consumer{
		client: client,
		config: cloneConfluentConfigMap(config),
	}
	consumer.closeCond = sync.NewCond(&consumer.mu)

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
	if c == nil {
		return nil, newMissingConfigError("consumer")
	}
	client, err := c.beginOperation()
	if err != nil {
		if errors.Is(err, errConsumerClosing) {
			return nil, consumerReadError(err)
		}
		return nil, err
	}
	defer c.endOperation()

	if limit <= 0 {
		limit = 1
	}
	ctx = ensureContext(ctx)

	messages := make([]Message, 0, limit)
	for len(messages) < limit {
		if c.closeRequested() {
			return messages, consumerReadError(errConsumerClosing)
		}
		if err := ctx.Err(); err != nil {
			return messages, consumerContextError(err)
		}

		timeout := confluentPollTimeout(ctx)
		if timeout == 0 {
			return messages, consumerContextError(ctx.Err())
		}

		msg, err := client.ReadMessage(timeout)
		if err != nil {
			var kafkaErr ckafka.Error
			if errors.As(err, &kafkaErr) && kafkaErr.IsTimeout() {
				continue
			}
			return messages, normalizeConsumerReadError(ctx, err)
		}

		messages = append(messages, confluentMessageToMessage(msg))
	}

	return messages, nil
}

func consumerContextError(err error) error {
	return NewXk6KafkaError(failedReadMessage, "Consumer context cancelled.", err)
}

func consumerReadError(err error) error {
	return NewXk6KafkaError(failedReadMessage, "Failed to consume message.", err)
}

func normalizeConsumerReadError(ctx context.Context, err error) error {
	if ctx != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return consumerContextError(ctxErr)
		}
	}

	return consumerReadError(err)
}

func (c *Consumer) Seek(partition int, offset int64) error {
	if c == nil {
		return newMissingConfigError("consumer")
	}
	if c.topic == "" {
		return newInvalidConfigError("consumer", errors.New("seek requires a single configured topic"))
	}
	client, err := c.beginOperation()
	if err != nil {
		if errors.Is(err, errConsumerClosing) {
			return NewXk6KafkaError(failedSetOffset, "Failed to seek consumer offset.", err)
		}
		return err
	}
	defer c.endOperation()

	err = client.Seek(ckafka.TopicPartition{
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
	if c == nil {
		return 0, newMissingConfigError("consumer")
	}
	if c.topic == "" {
		return 0, newInvalidConfigError("consumer", errors.New("position requires a single configured topic"))
	}
	client, err := c.beginOperation()
	if err != nil {
		if errors.Is(err, errConsumerClosing) {
			return 0, NewXk6KafkaError(failedSetOffset, "Failed to query consumer position.", err)
		}
		return 0, err
	}
	defer c.endOperation()

	positions, err := client.Position([]ckafka.TopicPartition{{
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
	if c == nil {
		return newMissingConfigError("consumer")
	}
	client, err := c.beginOperation()
	if err != nil {
		if errors.Is(err, errConsumerClosing) {
			return NewXk6KafkaError(failedCommitConsumer, "Failed to commit consumer offsets.", err)
		}
		return err
	}
	defer c.endOperation()

	ctx = ensureContext(ctx)
	if err := ctx.Err(); err != nil {
		return NewXk6KafkaError(failedCommitConsumer, "Consumer context cancelled.", err)
	}

	if _, err := client.Commit(); err != nil {
		return NewXk6KafkaError(failedCommitConsumer, "Failed to commit consumer offsets.", err)
	}

	return nil
}

func (c *Consumer) Close() error {
	if c == nil {
		return nil
	}

	c.mu.Lock()
	if c.client == nil {
		c.mu.Unlock()
		return nil
	}

	c.closing = true
	for c.activeCalls > 0 {
		c.closeCond.Wait()
	}

	client := c.client
	c.client = nil
	c.mu.Unlock()

	if err := client.Close(); err != nil {
		return NewXk6KafkaError(failedCreateConsumer, "Failed to close consumer.", err)
	}
	return nil
}

func (c *Consumer) Stats() ConsumerStats {
	if c == nil {
		return ConsumerStats{}
	}
	client, err := c.beginOperation()
	if err != nil {
		return ConsumerStats{}
	}
	defer c.endOperation()

	assignments, err := client.Assignment()
	if err != nil {
		return ConsumerStats{}
	}

	return ConsumerStats{Assignments: len(assignments)}
}

func (c *Consumer) beginOperation() (*ckafka.Consumer, error) {
	if c == nil {
		return nil, newMissingConfigError("consumer")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.client == nil {
		return nil, newMissingConfigError("consumer")
	}
	if c.closing {
		return nil, errConsumerClosing
	}

	c.activeCalls++
	return c.client, nil
}

func (c *Consumer) endOperation() {
	if c == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.activeCalls > 0 {
		c.activeCalls--
	}
	if c.activeCalls == 0 && c.closeCond != nil {
		c.closeCond.Broadcast()
	}
}

func (c *Consumer) closeRequested() bool {
	if c == nil {
		return false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	return c.closing
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
