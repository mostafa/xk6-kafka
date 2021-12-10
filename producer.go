package kafka

import (
	"context"
	"errors"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/compress"
	"go.k6.io/k6/lib"
	"go.k6.io/k6/stats"
)

var (
	CompressionCodecs = map[string]compress.Codec{
		"Gzip":   &compress.GzipCodec,
		"Snappy": &compress.SnappyCodec,
		"Lz4":    &compress.Lz4Codec,
		"Zstd":   &compress.ZstdCodec,
	}

	Balancers = map[string]kafkago.Balancer{
		"RoundRobin":      &kafkago.RoundRobin{},
		"LeastBytes":      &kafkago.LeastBytes{},
		"Hash":            &kafkago.Hash{},
		"CRC32Balancer":   &kafkago.CRC32Balancer{},
		"Murmur2Balancer": &kafkago.Murmur2Balancer{},
	}
)

type WriterConfig struct {
	Brokers      []string      `json:"brokers"`
	Topic        string        `json:"topic"`
	Auth         string        `json:"auth"`
	Compression  string        `json:"compression"`
	BatchSize    int           `json:"batch_size"`
	Balancer     string        `json:"balancer"`
	MaxAttempts  int           `json:"max_attempts"`
	BatchBytes   int           `json:"batch_bytes"`
	BatchTimeout time.Duration `json:"batch_timeout"`
	ReadTimeout  time.Duration `json:"read_timeout"`
	WriteTimeout time.Duration `json:"write_timeout"`
	RequiredAcks int           `json:"required_acks"`
}

func (*Kafka) Writer(wc WriterConfig) *kafkago.Writer {
	var dialer *kafkago.Dialer

	if wc.Auth != "" {
		creds, err := unmarshalCredentials(wc.Auth)
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
		Brokers:      wc.Brokers,
		Topic:        wc.Topic,
		BatchSize:    wc.BatchSize,
		Dialer:       dialer,
		MaxAttempts:  wc.MaxAttempts,
		BatchBytes:   wc.BatchBytes,
		BatchTimeout: wc.BatchTimeout,
		ReadTimeout:  wc.ReadTimeout,
		WriteTimeout: wc.WriteTimeout,
		RequiredAcks: wc.RequiredAcks,
		Async:        false, // async is not supported yet
	}

	if codec, exists := CompressionCodecs[wc.Compression]; exists {
		writerConfig.CompressionCodec = codec
	}

	if balancer, exists := Balancers[wc.Balancer]; exists {
		writerConfig.Balancer = balancer
	}

	return kafkago.NewWriter(writerConfig)
}

func (*Kafka) Produce(
	ctx context.Context, writer *kafkago.Writer, messages []map[string]string,
	keySchema string, valueSchema string) error {
	return ProduceInternal(ctx, writer, messages, Configuration{}, keySchema, valueSchema)
}

func (*Kafka) ProduceWithConfiguration(
	ctx context.Context, writer *kafkago.Writer, messages []map[string]string,
	configurationJson string, keySchema string, valueSchema string) error {
	configuration, err := unmarshalConfiguration(configurationJson)
	if err != nil {
		ReportError(err, "Cannot unmarshal configuration "+configurationJson)
		return nil
	}

	return ProduceInternal(ctx, writer, messages, configuration, keySchema, valueSchema)
}

func ProduceInternal(
	ctx context.Context, writer *kafkago.Writer, messages []map[string]string,
	configuration Configuration, keySchema string, valueSchema string) error {
	state := lib.GetState(ctx)
	err := errors.New("state is nil")

	if state == nil {
		ReportError(err, "Cannot determine state")
		return err
	}

	err = validateConfiguration(configuration)
	if err != nil {
		ReportError(err, "Validation of properties failed.")
		return err
	}

	kafkaMessages := make([]kafkago.Message, len(messages))
	for i, message := range messages {

		kafkaMessages[i] = kafkago.Message{}

		// If a key was provided, add it to the message. Keys are optional.
		if _, has_key := message["key"]; has_key {
			key := []byte(message["key"])
			if keySchema != "" {
				key = ToAvro(message["key"], keySchema)
			}
			keyData, err := addMagicByteAndSchemaIdPrefix(configuration, key, writer.Stats().Topic, "key", keySchema)
			if err != nil {
				ReportError(err, "Creation of key bytes failed.")
				return err
			}

			kafkaMessages[i].Key = keyData
		}

		// Then add then message
		value := []byte(message["value"])
		if valueSchema != "" {
			value = ToAvro(message["value"], valueSchema)
		}

		valueData, err := addMagicByteAndSchemaIdPrefix(configuration, value, writer.Stats().Topic, "value", valueSchema)
		if err != nil {
			ReportError(err, "Creation of message bytes failed.")
			return err
		}

		kafkaMessages[i].Value = valueData

	}

	err = writer.WriteMessages(ctx, kafkaMessages...)
	if err == ctx.Err() {
		// context is cancelled, so stop
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
	err := errors.New("state is nil")

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
