package kafka

import (
	"fmt"
	"time"

	"github.com/dop251/goja"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/compress"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/metrics"
)

var (
	// Compression codecs
	CODEC_GZIP   = "gzip"
	CODEC_SNAPPY = "snappy"
	CODEC_LZ4    = "lz4"
	CODEC_ZSTD   = "zstd"

	// CompressionCodecs is a map of compression codec names to their respective codecs.
	CompressionCodecs map[string]compress.Compression

	// DefaultSerializer is string serializer
	DefaultSerializer = StringSerializer
)

// XWriter is a wrapper around kafkago.Writer and acts as a JS constructor
// for this extension, thus it must be called with new operator, e.g. new Writer(...).
func (k *Kafka) XWriter(call goja.ConstructorCall) *goja.Object {
	rt := k.vu.Runtime()
	var (
		brokers     []string
		topic       string
		saslConfig  SASLConfig
		tlsConfig   TLSConfig
		compression string
	)

	if len(call.Arguments) > 0 {
		b := call.Arguments[0].Export().([]interface{})
		brokers = make([]string, len(b))
		for i, v := range b {
			brokers[i] = v.(string)
		}
	}

	if len(call.Arguments) > 1 {
		topic = call.Arguments[1].Export().(string)
	}

	if len(call.Arguments) > 2 {
		saslConfig = call.Arguments[2].Export().(SASLConfig)
	}

	if len(call.Arguments) > 3 {
		tlsConfig = call.Arguments[3].Export().(TLSConfig)
	}

	if len(call.Arguments) > 4 {
		compression = call.Arguments[4].Export().(string)
	}

	writer, err := k.Writer(brokers, topic, saslConfig, tlsConfig, compression)
	if err != nil {
		common.Throw(rt, err)
	}
	return rt.ToValue(writer).ToObject(rt)
}

// Writer creates a new Kafka writer
// TODO: accept a configuration
// Deprecated: use XWriter instead
func (k *Kafka) Writer(brokers []string, topic string, saslConfig SASLConfig, tlsConfig TLSConfig, compression string) (*kafkago.Writer, *Xk6KafkaError) {
	dialer, err := GetDialer(saslConfig, tlsConfig)
	if err != nil {
		if err.Unwrap() != nil {
			logger.WithField("error", err).Error(err)
		}
		return nil, err
	}

	// TODO: add AllowAutoTopicCreation to writer configuration
	writerConfig := kafkago.WriterConfig{
		Brokers:   brokers,
		Topic:     topic,
		Balancer:  &kafkago.LeastBytes{},
		BatchSize: 1,
		Dialer:    dialer,
		Async:     false,
	}

	if codec, ok := CompressionCodecs[compression]; ok {
		writerConfig.CompressionCodec = compress.Codecs[codec]
	}

	// TODO: instantiate Writer directly
	return kafkago.NewWriter(writerConfig), nil
}

// Produce sends messages to Kafka
func (k *Kafka) Produce(
	writer *kafkago.Writer, messages []map[string]interface{},
	keySchema string, valueSchema string, autoCreateTopic bool) *Xk6KafkaError {
	writer.AllowAutoTopicCreation = autoCreateTopic

	return k.produceInternal(writer, messages, Configuration{}, keySchema, valueSchema)
}

// ProduceWithConfiguration sends messages to Kafka with the given configuration
func (k *Kafka) ProduceWithConfiguration(
	writer *kafkago.Writer, messages []map[string]interface{},
	configurationJson string, keySchema string, valueSchema string, autoCreateTopic bool) *Xk6KafkaError {
	writer.AllowAutoTopicCreation = autoCreateTopic

	configuration, err := UnmarshalConfiguration(configurationJson)
	if err != nil {
		if err.Unwrap() != nil {
			logger.WithField("error", err).Error(err)
		}
		return err
	}

	return k.produceInternal(writer, messages, configuration, keySchema, valueSchema)
}

// GetSerializer returns the serializer for the given schema
func (k *Kafka) GetSerializer(schema string) Serializer {
	if ser, ok := k.serializerRegistry.Registry[schema]; ok {
		return ser.GetSerializer()
	}
	return SerializeString
}

// produceInternal sends messages to Kafka with the given configuration
func (k *Kafka) produceInternal(
	writer *kafkago.Writer, messages []map[string]interface{},
	configuration Configuration, keySchema string, valueSchema string) *Xk6KafkaError {
	state := k.vu.State()
	if state == nil {
		logger.WithField("error", ErrorForbiddenInInitContext).Error(ErrorForbiddenInInitContext)
		return ErrorForbiddenInInitContext
	}

	ctx := k.vu.Context()
	if ctx == nil {
		err := NewXk6KafkaError(noContextError, "No context.", nil)
		logger.WithField("error", err).Info(err)
		return err
	}

	err := ValidateConfiguration(configuration)
	if err != nil {
		configuration.Producer.KeySerializer = DefaultSerializer
		configuration.Producer.ValueSerializer = DefaultSerializer
		logger.WithField("error", err).Warn("Using default string serializers")
	}

	keySerializer := k.GetSerializer(configuration.Producer.KeySerializer)
	valueSerializer := k.GetSerializer(configuration.Producer.ValueSerializer)

	kafkaMessages := make([]kafkago.Message, len(messages))
	for i, message := range messages {
		kafkaMessages[i] = kafkago.Message{}

		// Topic can be explicitly set on each individual message
		// Setting topic on the writer and the messages are mutually exclusive
		if _, has_topic := message["topic"]; has_topic {
			kafkaMessages[i].Topic = message["topic"].(string)
		}

		if _, has_offset := message["offset"]; has_offset {
			kafkaMessages[i].Offset = message["offset"].(int64)
		}

		// If time is set, use it to set the time on the message,
		// otherwise use the current time.
		if _, has_time := message["time"]; has_time {
			kafkaMessages[i].Time = time.UnixMilli(message["time"].(int64))
		}

		// If a key was provided, add it to the message. Keys are optional.
		if _, has_key := message["key"]; has_key {
			keyData, err := keySerializer(
				configuration, writer.Stats().Topic, message["key"], "key", keySchema, 0)
			if err != nil && err.Unwrap() != nil {
				logger.WithField("error", err).Error(err)
			}

			kafkaMessages[i].Key = keyData
		}

		// Then add the message
		valueData, err := valueSerializer(configuration, writer.Stats().Topic, message["value"], "value", valueSchema, 0)
		if err != nil && err.Unwrap() != nil {
			logger.WithField("error", err).Error(err)
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

	originalErr := writer.WriteMessages(k.vu.Context(), kafkaMessages...)

	err = k.reportWriterStats(writer.Stats())
	if err != nil {
		logger.WithField("error", err).Error(err)
	}

	if originalErr != nil {
		if originalErr == k.vu.Context().Err() {
			logger.WithField("error", k.vu.Context().Err()).Error(k.vu.Context().Err())
			return NewXk6KafkaError(contextCancelled, "Context cancelled.", originalErr)
		} else {
			// TODO: fix this
			// Ignore stats reporting errors here, because we can't return twice,
			// and there is no way to wrap the error in another one.
			logger.WithField("error", originalErr).Error(originalErr)
			return NewXk6KafkaError(failedWriteMessage, "Failed to write messages.", err)
		}
	}

	return nil
}

// reportWriterStats reports the writer stats to the state
func (k *Kafka) reportWriterStats(currentStats kafkago.WriterStats) *Xk6KafkaError {
	state := k.vu.State()
	if state == nil {
		logger.WithField("error", ErrorForbiddenInInitContext).Error(ErrorForbiddenInInitContext)
		return ErrorForbiddenInInitContext
	}

	ctx := k.vu.Context()
	if ctx == nil {
		err := NewXk6KafkaError(cannotReportStats, "Cannot report writer stats, no context.", nil)
		logger.WithField("error", err).Info(err)
		return err
	}

	tags := make(map[string]string)
	tags["topic"] = currentStats.Topic

	now := time.Now()

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
		Metric: k.metrics.WriterErrors,
		Tags:   metrics.IntoSampleTags(&tags),
		Value:  float64(currentStats.Errors),
	})

	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		Time:   now,
		Metric: k.metrics.WriterWriteTime,
		Tags:   metrics.IntoSampleTags(&tags),
		Value:  metrics.D(currentStats.WriteTime.Avg),
	})

	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		Time:   now,
		Metric: k.metrics.WriterWaitTime,
		Tags:   metrics.IntoSampleTags(&tags),
		Value:  metrics.D(currentStats.WaitTime.Avg),
	})

	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		Time:   now,
		Metric: k.metrics.WriterRetries,
		Tags:   metrics.IntoSampleTags(&tags),
		Value:  float64(currentStats.Retries.Avg),
	})

	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		Time:   now,
		Metric: k.metrics.WriterBatchSize,
		Tags:   metrics.IntoSampleTags(&tags),
		Value:  float64(currentStats.BatchSize.Avg),
	})

	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		Time:   now,
		Metric: k.metrics.WriterBatchBytes,
		Tags:   metrics.IntoSampleTags(&tags),
		Value:  float64(currentStats.BatchBytes.Avg),
	})

	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		Time:   now,
		Metric: k.metrics.WriterMaxAttempts,
		Tags:   metrics.IntoSampleTags(&tags),
		Value:  float64(currentStats.MaxAttempts),
	})

	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		Time:   now,
		Metric: k.metrics.WriterMaxBatchSize,
		Tags:   metrics.IntoSampleTags(&tags),
		Value:  float64(currentStats.MaxBatchSize),
	})

	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		Time:   now,
		Metric: k.metrics.WriterBatchTimeout,
		Tags:   metrics.IntoSampleTags(&tags),
		Value:  float64(currentStats.BatchTimeout),
	})

	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		Time:   now,
		Metric: k.metrics.WriterReadTimeout,
		Tags:   metrics.IntoSampleTags(&tags),
		Value:  float64(currentStats.ReadTimeout),
	})

	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		Time:   now,
		Metric: k.metrics.WriterWriteTimeout,
		Tags:   metrics.IntoSampleTags(&tags),
		Value:  float64(currentStats.WriteTimeout),
	})

	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		Time:   now,
		Metric: k.metrics.WriterRequiredAcks,
		Tags:   metrics.IntoSampleTags(&tags),
		Value:  float64(currentStats.RequiredAcks),
	})

	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		Time:   now,
		Metric: k.metrics.WriterAsync,
		Tags:   metrics.IntoSampleTags(&tags),
		Value:  metrics.B(currentStats.Async),
	})

	return nil
}
