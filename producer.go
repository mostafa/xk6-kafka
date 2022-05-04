package kafka

import (
	"errors"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/compress"
	"go.k6.io/k6/stats"
)

var (
	CompressionCodecs = map[string]compress.Codec{
		"Gzip":   &compress.GzipCodec,
		"Snappy": &compress.SnappyCodec,
		"Lz4":    &compress.Lz4Codec,
		"Zstd":   &compress.ZstdCodec,
	}
)

func (*Kafka) Writer(brokers []string, topic string, auth string, compression string) *kafkago.Writer {
	var dialer *kafkago.Dialer

	if auth != "" {
		creds, err := unmarshalCredentials(auth)
		if err != nil {
			ReportError(err, "Unable to unmarshal credentials")
			return nil
		}

		dialer = getDialer(creds)
		if dialer == nil {
			ReportError(nil, "Dialer cannot authenticate")
			return nil
		}
	}

	writerConfig := kafkago.WriterConfig{
		Brokers:   brokers,
		Topic:     topic,
		Balancer:  &kafkago.LeastBytes{},
		BatchSize: 1,
		Dialer:    dialer,
		Async:     false,
	}

	if codec, exists := CompressionCodecs[compression]; exists {
		writerConfig.CompressionCodec = codec
	}

	return kafkago.NewWriter(writerConfig)
}

func (k *Kafka) Produce(
	writer *kafkago.Writer, messages []map[string]interface{},
	keySchema string, valueSchema string) error {
	return k.produceInternal(writer, messages, Configuration{}, keySchema, valueSchema)
}

func (k *Kafka) ProduceWithConfiguration(
	writer *kafkago.Writer, messages []map[string]interface{},
	configurationJson string, keySchema string, valueSchema string) error {
	configuration, err := unmarshalConfiguration(configurationJson)
	if err != nil {
		ReportError(err, "Cannot unmarshal configuration "+configurationJson)
		return nil
	}

	return k.produceInternal(writer, messages, configuration, keySchema, valueSchema)
}

func (k *Kafka) produceInternal(
	writer *kafkago.Writer, messages []map[string]interface{},
	configuration Configuration, keySchema string, valueSchema string) error {
	state := k.vu.State()
	err := errors.New("state is nil")

	if state == nil {
		ReportError(err, "Cannot determine state")
		err = k.reportWriterStats(writer.Stats())
		if err != nil {
			ReportError(err, "Cannot report writer stats")
		}
		return nil
	}

	err = validateConfiguration(configuration)
	if err != nil {
		ReportError(err, "Validation of properties failed.")
		return err
	}

	if state == nil {
		ReportError(err, "Cannot determine state")
		return err
	}

	ctx := k.vu.Context()
	err = errors.New("context is nil")

	if ctx == nil {
		ReportError(err, "Cannot determine context")
		return err
	}

	keySerializer := GetSerializer(configuration.Producer.KeySerializer, keySchema)
	valueSerializer := GetSerializer(configuration.Producer.ValueSerializer, valueSchema)

	kafkaMessages := make([]kafkago.Message, len(messages))
	for i, message := range messages {

		kafkaMessages[i] = kafkago.Message{}

		// If a key was provided, add it to the message. Keys are optional.
		if _, has_key := message["key"]; has_key {
			keyData, err := keySerializer(configuration, writer.Stats().Topic, message["key"], "key", keySchema)
			if err != nil {
				ReportError(err, "Creation of key bytes failed.")
				return err
			}

			kafkaMessages[i].Key = keyData
		}

		// Then add then message
		valueData, err := valueSerializer(configuration, writer.Stats().Topic, message["value"], "value", valueSchema)
		if err != nil {
			ReportError(err, "Creation of message bytes failed.")
			return err
		}

		kafkaMessages[i].Value = valueData
	}

	err = writer.WriteMessages(ctx, kafkaMessages...)
	if err == ctx.Err() {
		// context is cancelled, so stop
		err = k.reportWriterStats(writer.Stats())
		if err != nil {
			ReportError(err, "Cannot report writer stats")
		}
		return nil
	}

	if err != nil {
		ReportError(err, "Failed to write message")
		err = k.reportWriterStats(writer.Stats())
		if err != nil {
			ReportError(err, "Cannot report writer stats")
		}
		return err
	}

	return nil
}

func (k *Kafka) reportWriterStats(currentStats kafkago.WriterStats) error {
	state := k.vu.State()
	err := errors.New("state is nil")

	if state == nil {
		ReportError(err, "Cannot determine state")
		return err
	}

	ctx := k.vu.Context()
	err = errors.New("context is nil")
	if ctx == nil {
		ReportError(err, "Cannot determine context")
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
