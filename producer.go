package kafka

import (
	"errors"
	"fmt"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/compress"
	"go.k6.io/k6/metrics"
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
	writerConfig := kafkago.WriterConfig{
		Brokers:   brokers,
		Topic:     topic,
		Balancer:  &kafkago.LeastBytes{},
		BatchSize: 1,
		Dialer:    getDialerFromAuth(auth),
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

		// Topic can be explicitly set on the message
		if _, has_topic := message["Topic"]; has_topic {
			kafkaMessages[i].Topic = message["Topic"].(string)
		}

		if _, has_offset := message["offset"]; has_offset {
			kafkaMessages[i].Offset = message["offset"].(int64)
		}

		if _, has_highwatermark := message["highWaterMark"]; has_highwatermark {
			kafkaMessages[i].HighWaterMark = message["highWaterMark"].(int64)
		}

		// If time is set, use it to set the time on the message,
		// otherwise use the current time.
		if _, has_time := message["time"]; has_time {
			kafkaMessages[i].Time = time.UnixMilli(message["time"].(int64))
		}

		// If a key was provided, add it to the message. Keys are optional.
		if _, has_key := message["key"]; has_key {
			keyData, err := keySerializer(configuration, writer.Stats().Topic, message["key"], "key", keySchema)
			if err != nil {
				ReportError(err, "Creation of key bytes failed.")
				return err
			}

			kafkaMessages[i].Key = keyData
		}

		// Then add the message
		valueData, err := valueSerializer(configuration, writer.Stats().Topic, message["value"], "value", valueSchema)
		if err != nil {
			ReportError(err, "Creation of message bytes failed.")
			return err
		}

		kafkaMessages[i].Value = valueData

		// If headers are provided, add them to the message.
		if _, has_headers := message["headers"]; has_headers {
			for key, value := range message["headers"].(map[string]interface{}) {
				kafkaMessages[i].Headers = append(kafkaMessages[i].Headers, kafkago.Header{
					Key:   key,
					Value: []byte(fmt.Sprint(value)),
				})
			}
		}
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

	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		Time:   now,
		Metric: k.metrics.WriterDials,
		Tags:   metrics.IntoSampleTags(&tags),
		Value:  float64(currentStats.Dials),
	})

	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		Time:   now,
		Metric: k.metrics.WriterWrites,
		Tags:   metrics.IntoSampleTags(&tags),
		Value:  float64(currentStats.Writes),
	})

	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		Time:   now,
		Metric: k.metrics.WriterMessages,
		Tags:   metrics.IntoSampleTags(&tags),
		Value:  float64(currentStats.Messages),
	})

	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		Time:   now,
		Metric: k.metrics.WriterBytes,
		Tags:   metrics.IntoSampleTags(&tags),
		Value:  float64(currentStats.Bytes),
	})

	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		Time:   now,
		Metric: k.metrics.WriterRebalances,
		Tags:   metrics.IntoSampleTags(&tags),
		Value:  float64(currentStats.Rebalances),
	})

	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		Time:   now,
		Metric: k.metrics.WriterErrors,
		Tags:   metrics.IntoSampleTags(&tags),
		Value:  float64(currentStats.Errors),
	})

	return nil
}
