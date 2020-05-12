package main

import (
	"context"

	kafkago "github.com/segmentio/kafka-go"
)

type kafka struct{}

func New() *kafka {
	return &kafka{}
}

func (*kafka) Connect(brokers []string, topic string) *kafkago.Writer {
	return kafkago.NewWriter(kafkago.WriterConfig{
		Brokers:  brokers,
		Topic:    topic,
		Balancer: &kafkago.LeastBytes{},
	})
}

func (*kafka) Produce(writer *kafkago.Writer, messages []map[string]string) error {
	kafkaMessages := make([]kafkago.Message, len(messages))

	for i, message := range messages {
		kafkaMessages[i] = kafkago.Message{
			Key:   []byte(message["key"]),
			Value: []byte(message["value"]),
		}
	}

	err := writer.WriteMessages(context.Background(), kafkaMessages...)

	return err
}

func (*kafka) Close(writer *kafkago.Writer) {
	writer.Close()
}
