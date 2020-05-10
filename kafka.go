package main

import (
	"context"

	kafkago "github.com/segmentio/kafka-go"
)

type kafka struct{}

func New() *kafka {
	return &kafka{}
}

func (*kafka) Kafka(brokers []string, topic string, messages []map[string]string) string {
	w := kafkago.NewWriter(kafkago.WriterConfig{
		Brokers:  brokers,
		Topic:    topic,
		Balancer: &kafkago.LeastBytes{},
	})

	kafkaMessages := make([]kafkago.Message, len(messages))

	for i, message := range messages {
		kafkaMessages[i] = kafkago.Message{
			Key:   []byte(message["key"]),
			Value: []byte(message["value"]),
		}
	}

	err := w.WriteMessages(context.Background(), kafkaMessages...)

	if err != nil {
		return "Error"
	}

	w.Close()

	return "Sent"
}
