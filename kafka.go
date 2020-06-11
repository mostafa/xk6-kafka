package main

import (
	"context"
	"errors"
	"time"

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
	state, err := GetState(ctx)

	if err == nil {
		kafkaMessages := make([]kafkago.Message, len(messages))
		for i, message := range messages {
			kafkaMessages[i] = kafkago.Message{
				Key:   []byte(message["key"]),
				Value: []byte(message["value"]),
			}
		}

		err := writer.WriteMessages(ctx, kafkaMessages...)
		currentStats := writer.Stats()

		tags := make(map[string]string)
		tags["clientid"] = currentStats.ClientID
		tags["topic"] = currentStats.Topic

		now := time.Now()

		stats.PushIfNotDone(ctx, state.Samples, stats.Sample{
			Time:   now,
			Metric: Dials,
			Tags:   stats.IntoSampleTags(&tags),
			Value:  float64(currentStats.Dials),
		})

		stats.PushIfNotDone(ctx, state.Samples, stats.Sample{
			Time:   now,
			Metric: Writes,
			Tags:   stats.IntoSampleTags(&tags),
			Value:  float64(currentStats.Writes),
		})

		stats.PushIfNotDone(ctx, state.Samples, stats.Sample{
			Time:   now,
			Metric: Messages,
			Tags:   stats.IntoSampleTags(&tags),
			Value:  float64(currentStats.Messages),
		})

		stats.PushIfNotDone(ctx, state.Samples, stats.Sample{
			Time:   now,
			Metric: Bytes,
			Tags:   stats.IntoSampleTags(&tags),
			Value:  float64(currentStats.Bytes),
		})

		stats.PushIfNotDone(ctx, state.Samples, stats.Sample{
			Time:   now,
			Metric: Rebalances,
			Tags:   stats.IntoSampleTags(&tags),
			Value:  float64(currentStats.Rebalances),
		})

		stats.PushIfNotDone(ctx, state.Samples, stats.Sample{
			Time:   now,
			Metric: Errors,
			Tags:   stats.IntoSampleTags(&tags),
			Value:  float64(currentStats.Errors),
		})

		return err
	}

	return errors.New("State is nil")
}

func (*kafka) Close(writer *kafkago.Writer) {
	writer.Close()
}
