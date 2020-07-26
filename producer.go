package main

import (
	"context"
	"errors"
	"time"

	"github.com/loadimpact/k6/stats"
	kafkago "github.com/segmentio/kafka-go"
)

func (*kafka) Writer(brokers []string, topic string) *kafkago.Writer {
	return kafkago.NewWriter(kafkago.WriterConfig{
		Brokers:   brokers,
		Topic:     topic,
		Balancer:  &kafkago.LeastBytes{},
		BatchSize: 1,
	})
}

func (*kafka) Produce(
	ctx context.Context, writer *kafkago.Writer, messages []map[string]string,
	keySchema string, valueSchema string) error {
	// This is part of a hack for v0.26.2
	state, err := GetState(ctx)

	if state == nil {
		ReportError(nil, "Cannot determine state")
		return errors.New("Cannot determine state")
	}

	kafkaMessages := make([]kafkago.Message, len(messages))
	for i, message := range messages {
		key := []byte(message["key"])
		if keySchema != "" {
			key = ToAvro(message["value"], keySchema)
		}

		value := []byte(message["value"])
		if valueSchema != "" {
			value = ToAvro(message["value"], valueSchema)
		}

		kafkaMessages[i] = kafkago.Message{
			Key:   key,
			Value: value,
		}
	}

	err = writer.WriteMessages(ctx, kafkaMessages...)
	if err == ctx.Err() {
		// context is cancellled, so stop
		ReportWriterStats(ctx, writer.Stats())
		return nil
	}

	if err != nil {
		ReportError(err, "Failed to write message")
		ReportWriterStats(ctx, writer.Stats())
		return err
	}

	return nil
}

func ReportWriterStats(ctx context.Context, currentStats kafkago.WriterStats) error {
	// This is part of a hack for v0.26.2
	state, err := GetState(ctx)

	if state == nil {
		ReportError(err, "Cannot determine state")
		return err
	}

	tags := make(map[string]string)
	tags["clientid"] = currentStats.ClientID
	tags["topic"] = currentStats.Topic

	now := time.Now()

	stats.PushIfNotDone(ctx, state.Samples, stats.Sample{
		Time:   now,
		Metric: WriterDials,
		Tags:   stats.IntoSampleTags(&tags),
		Value:  float64(currentStats.Dials),
	})

	stats.PushIfNotDone(ctx, state.Samples, stats.Sample{
		Time:   now,
		Metric: WriterWrites,
		Tags:   stats.IntoSampleTags(&tags),
		Value:  float64(currentStats.Writes),
	})

	stats.PushIfNotDone(ctx, state.Samples, stats.Sample{
		Time:   now,
		Metric: WriterMessages,
		Tags:   stats.IntoSampleTags(&tags),
		Value:  float64(currentStats.Messages),
	})

	stats.PushIfNotDone(ctx, state.Samples, stats.Sample{
		Time:   now,
		Metric: WriterBytes,
		Tags:   stats.IntoSampleTags(&tags),
		Value:  float64(currentStats.Bytes),
	})

	stats.PushIfNotDone(ctx, state.Samples, stats.Sample{
		Time:   now,
		Metric: WriterRebalances,
		Tags:   stats.IntoSampleTags(&tags),
		Value:  float64(currentStats.Rebalances),
	})

	stats.PushIfNotDone(ctx, state.Samples, stats.Sample{
		Time:   now,
		Metric: WriterErrors,
		Tags:   stats.IntoSampleTags(&tags),
		Value:  float64(currentStats.Errors),
	})

	return nil
}
