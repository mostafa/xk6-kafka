package kafka

import (
	"encoding/json"
	"io"
	"time"

	"github.com/dop251/goja"
	kafkago "github.com/segmentio/kafka-go"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/metrics"
)

var DefaultDeserializer = StringDeserializer

type ReaderConfig struct {
	Brokers    []string   `json:"brokers"`
	Topic      string     `json:"topic"`
	Partition  int        `json:"partition"`
	GroupID    string     `json:"groupID"`
	Offset     int64      `json:"offset"`
	SaslConfig SASLConfig `json:"saslConfig"`
	TlsConfig  TLSConfig  `json:"tlsConfig"`
}

type ConsumeConfig struct {
	Limit             int64  `json:"limit"`
	ConfigurationJson string `json:"configurationJson"`
	KeySchema         string `json:"keySchema"`
	ValueSchema       string `json:"valueSchema"`
}

// XReader is a wrapper around kafkago.Reader and acts as a JS constructor
// for this extension, thus it must be called with new operator, e.g. new Reader(...).
func (k *Kafka) XReader(call goja.ConstructorCall) *goja.Object {
	rt := k.vu.Runtime()
	var rConfig *ReaderConfig
	if len(call.Arguments) > 0 {
		if params, ok := call.Argument(0).Export().(map[string]interface{}); ok {
			b, err := json.Marshal(params)
			if err != nil {
				common.Throw(rt, err)
			}
			err = json.Unmarshal(b, &rConfig)
			if err != nil {
				common.Throw(rt, err)
			}
		}
	}

	reader := k.Reader(
		rConfig.Brokers, rConfig.Topic, rConfig.Partition, rConfig.GroupID, rConfig.Offset,
		rConfig.SaslConfig, rConfig.TlsConfig)

	readerObject := rt.NewObject()
	// This is the reader object itself
	err := readerObject.Set("This", reader)
	if err != nil {
		common.Throw(rt, err)
	}

	err = readerObject.Set("consume", func(call goja.FunctionCall) goja.Value {
		cConfig := ConsumeConfig{}
		if len(call.Arguments) > 0 {
			cConfig.Limit = call.Arguments[0].Export().(int64)
		}

		if len(call.Arguments) > 1 {
			cConfig.KeySchema = call.Arguments[1].Export().(string)
		}

		if len(call.Arguments) > 2 {
			cConfig.ValueSchema = call.Arguments[2].Export().(string)
		}

		return rt.ToValue(k.Consume(
			reader, cConfig.Limit, cConfig.KeySchema, cConfig.ValueSchema))
	})
	if err != nil {
		common.Throw(rt, err)
	}

	err = readerObject.Set("consumeWithConfiguration", func(call goja.FunctionCall) goja.Value {
		cConfig := ConsumeConfig{}
		if len(call.Arguments) > 0 {
			cConfig.Limit = call.Arguments[0].Export().(int64)
		}

		if len(call.Arguments) > 1 {
			cConfig.ConfigurationJson = call.Arguments[1].Export().(string)
		}

		if len(call.Arguments) > 2 {
			cConfig.KeySchema = call.Arguments[2].Export().(string)
		}

		if len(call.Arguments) > 3 {
			cConfig.ValueSchema = call.Arguments[3].Export().(string)
		}

		return rt.ToValue(
			k.ConsumeWithConfiguration(
				reader, cConfig.Limit, cConfig.ConfigurationJson,
				cConfig.KeySchema, cConfig.ValueSchema))
	})
	if err != nil {
		common.Throw(rt, err)
	}

	// This is unnecessary, but it's here for reference purposes
	err = readerObject.Set("close", func(call goja.FunctionCall) goja.Value {
		err := reader.Close()
		if err != nil {
			common.Throw(rt, err)
		}

		return goja.Undefined()
	})
	if err != nil {
		common.Throw(rt, err)
	}

	freeze(readerObject)

	return rt.ToValue(readerObject).ToObject(rt)
}

// Reader creates a Kafka reader with the given configuration
// Deprecated: use XReader instead
func (k *Kafka) Reader(
	brokers []string, topic string, partition int,
	groupID string, offset int64, saslConfig SASLConfig, tlsConfig TLSConfig) *kafkago.Reader {
	if groupID != "" {
		partition = 0
	}

	dialer, err := GetDialer(saslConfig, tlsConfig)
	if err != nil {
		if err.Unwrap() != nil {
			logger.WithField("error", err).Error(err)
		}
		common.Throw(k.vu.Runtime(), err)
	}

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:          brokers,
		Topic:            topic,
		Partition:        partition,
		GroupID:          groupID,
		MaxWait:          time.Millisecond * 200,
		RebalanceTimeout: time.Second * 5,
		QueueCapacity:    1,
		Dialer:           dialer,
	})

	if offset > 0 {
		if groupID == "" {
			err := reader.SetOffset(offset)
			if err != nil {
				wrappedError := NewXk6KafkaError(
					failedSetOffset, "Unable to set offset, yet returning the reader.", err)
				logger.WithField("error", wrappedError).Warn(wrappedError)
				return reader
			}
		} else {
			err := NewXk6KafkaError(
				failedSetOffset, "Offset and groupID are mutually exclusive options, so offset is not set, yet returning the reader.", nil)
			logger.WithField("error", err).Warn(err)
			return reader
		}
	}

	return reader
}

// Consume consumes messages from the given reader
func (k *Kafka) Consume(reader *kafkago.Reader, limit int64,
	keySchema string, valueSchema string) []map[string]interface{} {
	return k.consumeInternal(reader, limit, Configuration{}, keySchema, valueSchema)
}

// ConsumeWithConfiguration consumes messages from the given reader with the given configuration
func (k *Kafka) ConsumeWithConfiguration(
	reader *kafkago.Reader, limit int64, configurationJson string,
	keySchema string, valueSchema string) []map[string]interface{} {
	configuration, err := UnmarshalConfiguration(configurationJson)
	if err != nil {
		if err.Unwrap() != nil {
			logger.WithField("error", err).Error(err)
		}
		common.Throw(k.vu.Runtime(), err)
	}
	return k.consumeInternal(reader, limit, configuration, keySchema, valueSchema)
}

// GetDeserializer returns the deserializer for the given schema
func (k *Kafka) GetDeserializer(schema string) Deserializer {
	if de, ok := k.deserializerRegistry.Registry[schema]; ok {
		return de.GetDeserializer()
	}
	return DeserializeString
}

// consumeInternal consumes messages from the given reader
func (k *Kafka) consumeInternal(
	reader *kafkago.Reader, limit int64,
	configuration Configuration, keySchema string, valueSchema string) []map[string]interface{} {
	state := k.vu.State()
	if state == nil {
		logger.WithField("error", ErrorForbiddenInInitContext).Error(ErrorForbiddenInInitContext)
		common.Throw(k.vu.Runtime(), ErrorForbiddenInInitContext)
	}

	ctx := k.vu.Context()
	if ctx == nil {
		err := NewXk6KafkaError(noContextError, "No context.", nil)
		logger.WithField("error", err).Info(err)
		common.Throw(k.vu.Runtime(), err)
	}

	if limit <= 0 {
		limit = 1
	}

	err := ValidateConfiguration(configuration)
	if err != nil {
		configuration.Consumer.KeyDeserializer = DefaultDeserializer
		configuration.Consumer.ValueDeserializer = DefaultDeserializer
		logger.WithField("error", err).Warn("Using default string serializers")
	}

	keyDeserializer := k.GetDeserializer(configuration.Consumer.KeyDeserializer)
	valueDeserializer := k.GetDeserializer(configuration.Consumer.ValueDeserializer)

	messages := make([]map[string]interface{}, 0)

	for i := int64(0); i < limit; i++ {
		msg, err := reader.ReadMessage(ctx)

		if err == io.EOF {
			k.reportReaderStats(reader.Stats())

			err = NewXk6KafkaError(noMoreMessages, "No more messages.", nil)
			logger.WithField("error", err).Info(err)
			return messages
		}

		if err != nil {
			k.reportReaderStats(reader.Stats())

			err = NewXk6KafkaError(failedReadMessage, "Unable to read messages.", nil)
			logger.WithField("error", err).Error(err)
			return messages
		}

		message := make(map[string]interface{})
		if len(msg.Key) > 0 {
			var wrappedError *Xk6KafkaError
			message["key"], wrappedError = keyDeserializer(
				configuration, reader.Config().Topic, msg.Key, Key, keySchema, 0)
			if wrappedError != nil && wrappedError.Unwrap() != nil {
				logger.WithField("error", wrappedError).Error(wrappedError)
			}
		}

		if len(msg.Value) > 0 {
			var wrappedError *Xk6KafkaError
			message["value"], wrappedError = valueDeserializer(
				configuration, reader.Config().Topic, msg.Value, "value", valueSchema, 0)
			if wrappedError != nil && wrappedError.Unwrap() != nil {
				logger.WithField("error", wrappedError).Error(wrappedError)
			}
		}

		// Rest of the fields of a given message
		message["topic"] = msg.Topic
		message["partition"] = msg.Partition
		message["offset"] = msg.Offset
		message["time"] = time.Unix(msg.Time.Unix(), 0).Format(time.RFC3339)
		message["highWaterMark"] = msg.HighWaterMark
		message["headers"] = msg.Headers

		messages = append(messages, message)
	}

	k.reportReaderStats(reader.Stats())
	return messages
}

// reportReaderStats reports the reader stats
func (k *Kafka) reportReaderStats(currentStats kafkago.ReaderStats) {
	state := k.vu.State()
	if state == nil {
		logger.WithField("error", ErrorForbiddenInInitContext).Error(ErrorForbiddenInInitContext)
		common.Throw(k.vu.Runtime(), ErrorForbiddenInInitContext)
	}

	ctx := k.vu.Context()
	if ctx == nil {
		err := NewXk6KafkaError(contextCancelled, "No context.", nil)
		logger.WithField("error", err).Info(err)
		common.Throw(k.vu.Runtime(), err)
	}

	tags := make(map[string]string)
	tags["clientid"] = currentStats.ClientID
	tags["topic"] = currentStats.Topic
	tags["partition"] = currentStats.Partition

	now := time.Now()

	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		Time:   now,
		Metric: k.metrics.ReaderDials,
		Tags:   metrics.IntoSampleTags(&tags),
		Value:  float64(currentStats.Dials),
	})

	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		Time:   now,
		Metric: k.metrics.ReaderFetches,
		Tags:   metrics.IntoSampleTags(&tags),
		Value:  float64(currentStats.Fetches),
	})

	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		Time:   now,
		Metric: k.metrics.ReaderMessages,
		Tags:   metrics.IntoSampleTags(&tags),
		Value:  float64(currentStats.Messages),
	})

	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		Time:   now,
		Metric: k.metrics.ReaderBytes,
		Tags:   metrics.IntoSampleTags(&tags),
		Value:  float64(currentStats.Bytes),
	})

	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		Time:   now,
		Metric: k.metrics.ReaderRebalances,
		Tags:   metrics.IntoSampleTags(&tags),
		Value:  float64(currentStats.Rebalances),
	})

	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		Time:   now,
		Metric: k.metrics.ReaderTimeouts,
		Tags:   metrics.IntoSampleTags(&tags),
		Value:  float64(currentStats.Timeouts),
	})

	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		Time:   now,
		Metric: k.metrics.ReaderErrors,
		Tags:   metrics.IntoSampleTags(&tags),
		Value:  float64(currentStats.Errors),
	})

	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		Time:   now,
		Metric: k.metrics.ReaderDialTime,
		Tags:   metrics.IntoSampleTags(&tags),
		Value:  metrics.D(currentStats.DialTime.Avg),
	})

	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		Time:   now,
		Metric: k.metrics.ReaderReadTime,
		Tags:   metrics.IntoSampleTags(&tags),
		Value:  metrics.D(currentStats.ReadTime.Avg),
	})

	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		Time:   now,
		Metric: k.metrics.ReaderWaitTime,
		Tags:   metrics.IntoSampleTags(&tags),
		Value:  metrics.D(currentStats.WaitTime.Avg),
	})

	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		Time:   now,
		Metric: k.metrics.ReaderFetchSize,
		Tags:   metrics.IntoSampleTags(&tags),
		Value:  float64(currentStats.FetchSize.Avg),
	})

	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		Time:   now,
		Metric: k.metrics.ReaderFetchBytes,
		Tags:   metrics.IntoSampleTags(&tags),
		Value:  float64(currentStats.FetchBytes.Min),
	})

	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		Time:   now,
		Metric: k.metrics.ReaderFetchBytes,
		Tags:   metrics.IntoSampleTags(&tags),
		Value:  float64(currentStats.FetchBytes.Max),
	})

	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		Time:   now,
		Metric: k.metrics.ReaderOffset,
		Tags:   metrics.IntoSampleTags(&tags),
		Value:  float64(currentStats.Offset),
	})

	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		Time:   now,
		Metric: k.metrics.ReaderLag,
		Tags:   metrics.IntoSampleTags(&tags),
		Value:  float64(currentStats.Lag),
	})

	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		Time:   now,
		Metric: k.metrics.ReaderMinBytes,
		Tags:   metrics.IntoSampleTags(&tags),
		Value:  float64(currentStats.MinBytes),
	})

	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		Time:   now,
		Metric: k.metrics.ReaderMaxBytes,
		Tags:   metrics.IntoSampleTags(&tags),
		Value:  float64(currentStats.MaxBytes),
	})

	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		Time:   now,
		Metric: k.metrics.ReaderMaxWait,
		Tags:   metrics.IntoSampleTags(&tags),
		Value:  metrics.D(currentStats.MaxWait),
	})

	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		Time:   now,
		Metric: k.metrics.ReaderQueueLength,
		Tags:   metrics.IntoSampleTags(&tags),
		Value:  float64(currentStats.QueueLength),
	})

	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		Time:   now,
		Metric: k.metrics.ReaderQueueCapacity,
		Tags:   metrics.IntoSampleTags(&tags),
		Value:  float64(currentStats.QueueCapacity),
	})
}
