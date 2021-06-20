package kafka

import (
	"context"
	"errors"
	"io"
	"time"

	"go.k6.io/k6/js/modules"
	"go.k6.io/k6/lib"
	"go.k6.io/k6/stats"
	kafkago "github.com/segmentio/kafka-go"
)

func init() {
	modules.Register("k6/x/kafka", new(Kafka))
}

type Kafka struct{}

func (*Kafka) Reader(
	brokers []string, topic string, partition int,
	minBytes int, maxBytes int, offset int64) *kafkago.Reader {

	if maxBytes == 0 {
		maxBytes = 10e6 // 10MB
	}

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:          brokers,
		Topic:            topic,
		Partition:        partition,
		MinBytes:         minBytes,
		MaxBytes:         maxBytes,
		MaxWait:          time.Millisecond * 200,
		RebalanceTimeout: time.Second * 5,
		QueueCapacity:    1,
	})

	if offset > 0 {
		reader.SetOffset(offset)
	}

	return reader
}

func (*Kafka) Consume(
	ctx context.Context, reader *kafkago.Reader, limit int64,
	keySchema string, valueSchema string) []map[string]interface{} {
	var properties = make(map[string]string);
	return ConsumeInternal(ctx, reader, limit, properties, keySchema, valueSchema);
}

func (*Kafka) ConsumeWithProps(
	ctx context.Context, reader *kafkago.Reader, limit int64, properties map[string]string,
	keySchema string, valueSchema string) []map[string]interface{} {
	return ConsumeInternal(ctx, reader, limit, properties, keySchema, valueSchema);
}

func ConsumeInternal(
	ctx context.Context, reader *kafkago.Reader, limit int64,
	properties map[string]string, keySchema string, valueSchema string) []map[string]interface{} {
	state := lib.GetState(ctx)

	if state == nil {
		ReportError(nil, "Cannot determine state")
		ReportReaderStats(ctx, reader.Stats())
		return nil
	}

	if limit <= 0 {
		limit = 1
	}

	messages := make([]map[string]interface{}, 0)

	for i := int64(0); i < limit; i++ {
		msg, err := reader.ReadMessage(ctx)

		if err == io.EOF {
			ReportError(err, "Reached the end of queue")
			// context is cancelled, so break
			ReportReaderStats(ctx, reader.Stats())
			return messages
		}

		if err != nil {
			ReportError(err, "There was an error fetching messages")
			ReportReaderStats(ctx, reader.Stats())
			return messages
		}

		message := make(map[string]interface{})
		if len(msg.Key) > 0 {

			// Account for proprietary 5-byte prefix before the Avro payload:
			// https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#wire-format
			if (properties["key.deserializer"] == "io.confluent.kafka.serializers.KafkaAvroDeserializer") {
				msg.Key = msg.Key[5:]
			}

			message["key"] = string(msg.Key)
			if keySchema != "" {
				message["key"] = FromAvro(msg.Key, keySchema)
			}
		}

		if len(msg.Value) > 0 {

			// Account for proprietary 5-byte prefix before the Avro payload:
			// https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#wire-format
			if (properties["key.deserializer"] == "io.confluent.kafka.serializers.KafkaAvroDeserializer") {
				msg.Value = msg.Value[5:]
			}

			message["value"] = string(msg.Value)
			if valueSchema != "" {
				message["value"] = FromAvro(msg.Value, valueSchema)
			}
		}

		messages = append(messages, message)
	}

	ReportReaderStats(ctx, reader.Stats())

	return messages
}

func ReportReaderStats(ctx context.Context, currentStats kafkago.ReaderStats) error {
	state := lib.GetState(ctx)
	err := errors.New("State is nil")

	if state == nil {
		ReportError(err, "Cannot determine state")
		return err
	}

	tags := make(map[string]string)
	tags["clientid"] = currentStats.ClientID
	tags["topic"] = currentStats.Topic
	tags["partition"] = currentStats.Partition

	now := time.Now()

	stats.PushIfNotDone(ctx, state.Samples, stats.Sample{
		Time:   now,
		Metric: ReaderDials,
		Tags:   stats.IntoSampleTags(&tags),
		Value:  float64(currentStats.Dials),
	})

	stats.PushIfNotDone(ctx, state.Samples, stats.Sample{
		Time:   now,
		Metric: ReaderFetches,
		Tags:   stats.IntoSampleTags(&tags),
		Value:  float64(currentStats.Fetches),
	})

	stats.PushIfNotDone(ctx, state.Samples, stats.Sample{
		Time:   now,
		Metric: ReaderMessages,
		Tags:   stats.IntoSampleTags(&tags),
		Value:  float64(currentStats.Messages),
	})

	stats.PushIfNotDone(ctx, state.Samples, stats.Sample{
		Time:   now,
		Metric: ReaderBytes,
		Tags:   stats.IntoSampleTags(&tags),
		Value:  float64(currentStats.Bytes),
	})

	stats.PushIfNotDone(ctx, state.Samples, stats.Sample{
		Time:   now,
		Metric: ReaderRebalances,
		Tags:   stats.IntoSampleTags(&tags),
		Value:  float64(currentStats.Rebalances),
	})

	stats.PushIfNotDone(ctx, state.Samples, stats.Sample{
		Time:   now,
		Metric: ReaderTimeouts,
		Tags:   stats.IntoSampleTags(&tags),
		Value:  float64(currentStats.Timeouts),
	})

	stats.PushIfNotDone(ctx, state.Samples, stats.Sample{
		Time:   now,
		Metric: ReaderErrors,
		Tags:   stats.IntoSampleTags(&tags),
		Value:  float64(currentStats.Errors),
	})

	return nil
}
