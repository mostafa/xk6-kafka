package kafka

import (
	"context"
	"errors"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"go.k6.io/k6/lib"
	"go.k6.io/k6/stats"
)

func (*Kafka) Writer(brokers []string, topic string) *kafkago.Writer {
	return kafkago.NewWriter(kafkago.WriterConfig{
		Brokers:   brokers,
		Topic:     topic,
		Balancer:  &kafkago.LeastBytes{},
		BatchSize: 1,
	})
}

func (*Kafka) Produce(
	ctx context.Context, writer *kafkago.Writer, messages []map[string]string,
	keySchema string, valueSchema string) error {
	var properties = make(map[string]string);
	return ProduceInternal(ctx, writer, messages, properties, keySchema, valueSchema);
}

func (*Kafka) ProduceWithProps(
	ctx context.Context, writer *kafkago.Writer, messages []map[string]string,
	properties map[string]string, keySchema string, valueSchema string) error {
	return ProduceInternal(ctx, writer, messages, properties, keySchema, valueSchema);
}

func ProduceInternal(
	ctx context.Context, writer *kafkago.Writer, messages []map[string]string,
	properties map[string]string, keySchema string, valueSchema string) error {
	state := lib.GetState(ctx)
	err := errors.New("State is nil")

	keySchemaId := 0;
	valueSchemaId := 0;

	if (properties["key.serializer"] == "io.confluent.kafka.serializers.KafkaAvroSerializer" ||
		properties["value.serializer"] == "io.confluent.kafka.serializers.KafkaAvroSerializer") {
		if (properties["schema.registry.url"] == "") {
			ReportError(err, "You have to provide a value for schema.registry.url to use a serializer of type io.confluent.kafka.serializers.KafkaAvroSerializer")
			return err
		}
	}



	if state == nil {
		ReportError(err, "Cannot determine state")
		return err
	}

	kafkaMessages := make([]kafkago.Message, len(messages))
	for i, message := range messages {
		key := []byte(message["key"])
		if keySchema != "" {
			key = ToAvro(message["key"], keySchema)
		}

		value := []byte(message["value"])
		if valueSchema != "" {
			value = ToAvro(message["value"], valueSchema)
		}

		kafkaMessages[i] = kafkago.Message{
			Key:   append([]byte{ 0, 0, 0, 0, 3 }, key...),
			Value: append([]byte{ 0, 0, 0, 0, 4 }, value...),
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
	state := lib.GetState(ctx)
	err := errors.New("State is nil")

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
