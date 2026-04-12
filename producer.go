package kafka

import (
	"context"
	"fmt"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type ProducerStats struct {
	Pending int
}

type Producer struct {
	client       *ckafka.Producer
	config       ckafka.ConfigMap
	defaultTopic string
}

func NewProducerFromWriterConfig(writerConfig *WriterConfig) (*Producer, error) {
	config, err := writerConfigToConfluentConfigMap(writerConfig)
	if err != nil {
		return nil, err
	}

	client, err := ckafka.NewProducer(&config)
	if err != nil {
		return nil, NewXk6KafkaError(failedCreateProducer, "Failed to create producer.", err)
	}

	defaultTopic := ""
	if writerConfig != nil {
		defaultTopic = writerConfig.Topic
	}

	return &Producer{
		client:       client,
		config:       cloneConfluentConfigMap(config),
		defaultTopic: defaultTopic,
	}, nil
}

func (p *Producer) Produce(ctx context.Context, msgs []Message) error {
	if p == nil || p.client == nil {
		return newMissingConfigError("producer")
	}
	if len(msgs) == 0 {
		return nil
	}
	ctx = ensureContext(ctx)

	deliveryChan := make(chan ckafka.Event, len(msgs))
	defer close(deliveryChan)

	for _, msg := range msgs {
		topic := msg.Topic
		if topic == "" {
			topic = p.defaultTopic
		}
		if topic == "" {
			return newInvalidConfigError("producer message", fmt.Errorf("topic must not be empty"))
		}

		kafkaMsg := &ckafka.Message{
			TopicPartition: ckafka.TopicPartition{
				Topic:     &topic,
				Partition: ckafka.PartitionAny,
			},
			Key:       msg.Key,
			Value:     msg.Value,
			Timestamp: msg.Time,
			Headers:   confluentHeaders(msg.Headers),
		}

		if err := p.client.Produce(kafkaMsg, deliveryChan); err != nil {
			return NewXk6KafkaError(writerError, "Failed to produce message.", err)
		}
	}

	for pending := len(msgs); pending > 0; pending-- {
		select {
		case <-ctx.Done():
			return NewXk6KafkaError(writerError, "Producer context cancelled.", ctx.Err())
		case event := <-deliveryChan:
			switch produced := event.(type) {
			case *ckafka.Message:
				if produced.TopicPartition.Error != nil {
					return NewXk6KafkaError(writerError, "Failed to deliver produced message.", produced.TopicPartition.Error)
				}
			case ckafka.Error:
				return NewXk6KafkaError(writerError, "Producer reported an asynchronous error.", produced)
			}
		}
	}

	return nil
}

func (p *Producer) Flush(ctx context.Context) error {
	if p == nil || p.client == nil {
		return newMissingConfigError("producer")
	}
	ctx = ensureContext(ctx)

	for {
		select {
		case <-ctx.Done():
			return NewXk6KafkaError(failedFlushProducer, "Producer flush cancelled.", ctx.Err())
		default:
		}

		if remaining := p.client.Flush(100); remaining == 0 {
			return nil
		}
	}
}

func (p *Producer) Close() error {
	if p == nil || p.client == nil {
		return nil
	}

	p.client.Close()
	return nil
}

func (p *Producer) Stats() ProducerStats {
	if p == nil || p.client == nil {
		return ProducerStats{}
	}

	return ProducerStats{Pending: p.client.Len()}
}

func confluentHeaders(headers map[string]any) []ckafka.Header {
	if len(headers) == 0 {
		return nil
	}

	kafkaHeaders := make([]ckafka.Header, 0, len(headers))
	for key, value := range headers {
		kafkaHeaders = append(kafkaHeaders, ckafka.Header{
			Key:   key,
			Value: fmt.Appendf(nil, "%v", value),
		})
	}

	return kafkaHeaders
}
