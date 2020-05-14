package main

import (
	"context"

	"github.com/loadimpact/k6/lib"
	"github.com/loadimpact/k6/stats"
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

func (*kafka) Produce(ctx context.Context, writer *kafkago.Writer, messages []map[string]string) error {
	kafkaMessages := make([]kafkago.Message, len(messages))

	for i, message := range messages {
		kafkaMessages[i] = kafkago.Message{
			Key:   []byte(message["key"]),
			Value: []byte(message["value"]),
		}
	}

	err := writer.WriteMessages(ctx, kafkaMessages...)
	// start := time.Now()
	currentStats := writer.Stats()
	tags := map[string]string{}
	tags["clientid"] = currentStats.ClientID
	tags["topic"] = currentStats.Topic
	state := lib.GetState(ctx)

	stats.PushIfNotDone(ctx, state.Samples, stats.ConnectedSamples{
		Samples: []stats.Sample{
			{
				// Time:   now,
				Metric: Dials,
				Value:  float64(currentStats.Dials),
			},
			{
				// Time:   now,
				Metric: Writes,
				Value:  float64(currentStats.Writes),
			},
		},
		Tags: stats.IntoSampleTags(&tags),
		// Time: start
	})

	return err
}

func (*kafka) Close(writer *kafkago.Writer) {
	writer.Close()
}
