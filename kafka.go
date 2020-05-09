package main

import (
	"context"

	kafkago "github.com/segmentio/kafka-go"
)

type kafka struct{}

func New() *kafka {
	return &kafka{}
}

func (*kafka) Kafka(brokers []string, topic string, key string, value string) string {
	w := kafkago.NewWriter(kafkago.WriterConfig{
		Brokers:  brokers,
		Topic:    topic,
		Balancer: &kafkago.LeastBytes{},
	})

	err := w.WriteMessages(context.Background(), kafkago.Message{
		Key:   []byte(key),
		Value: []byte(value),
	})

	if err != nil {
		return "Error"
	}

	w.Close()

	return "Sent"
}
