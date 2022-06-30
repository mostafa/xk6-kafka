package kafka

import (
	"encoding/json"
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

	// Balancers
	BALANCER_ROUND_ROBIN = "balancer_roundrobin"
	BALANCER_LEAST_BYTES = "balancer_leastbytes"
	BALANCER_HASH        = "balancer_hash"
	BALANCER_CRC32       = "balancer_crc32"
	BALANCER_MURMUR2     = "balancer_murmur2"

	// BalancerRegistry is a map of balancer names to their respective balancers.
	BalancerRegistry map[string]kafkago.Balancer

	// DefaultSerializer is string serializer
	DefaultSerializer = StringSerializer
)

// freeze disallows resetting or changing the properties of the object
func freeze(o *goja.Object) {
	for _, key := range o.Keys() {
		err := o.DefineDataProperty(
			key, o.Get(key), goja.FLAG_FALSE, goja.FLAG_FALSE, goja.FLAG_TRUE)
		if err != nil {
			panic(err)
		}
	}
}

type WriterConfig struct {
	Brokers         []string      `json:"brokers"`
	Topic           string        `json:"topic"`
	AutoCreateTopic bool          `json:"autoCreateTopic"`
	Balancer        string        `json:"balancer"`
	MaxAttempts     int           `json:"maxAttempts"`
	BatchSize       int           `json:"batchSize"`
	BatchBytes      int           `json:"batchBytes"`
	BatchTimeout    time.Duration `json:"batchTimeout"`
	ReadTimeout     time.Duration `json:"readTimeout"`
	WriteTimeout    time.Duration `json:"writeTimeout"`
	RequiredAcks    int           `json:"requiredAcks"`
	Compression     string        `json:"compression"`
	SASL            SASLConfig    `json:"sasl"`
	TLS             TLSConfig     `json:"tls"`
	ConnectLogger   bool          `json:"connectLogger"`
}

type ProduceConfig struct {
	Messages          []map[string]interface{} `json:"messages"`
	ConfigurationJson string                   `json:"config"`
	KeySchema         string                   `json:"keySchema"`
	ValueSchema       string                   `json:"valueSchema"`
	AutoCreateTopic   bool                     `json:"autoCreateTopic"`
}

// XWriter is a wrapper around kafkago.Writer and acts as a JS constructor
// for this extension, thus it must be called with new operator, e.g. new Writer(...).
func (k *Kafka) XWriter(call goja.ConstructorCall) *goja.Object {
	rt := k.vu.Runtime()
	var wConfig *WriterConfig
	if len(call.Arguments) > 0 {
		if params, ok := call.Argument(0).Export().(map[string]interface{}); ok {
			b, err := json.Marshal(params)
			if err != nil {
				common.Throw(rt, err)
			}
			err = json.Unmarshal(b, &wConfig)
			if err != nil {
				common.Throw(rt, err)
			}
		}
	}

	writer := k.Writer(wConfig)

	writerObject := rt.NewObject()
	// This is the writer object itself
	err := writerObject.Set("This", writer)
	if err != nil {
		common.Throw(rt, err)
	}

	err = writerObject.Set("produce", func(call goja.FunctionCall) goja.Value {
		pConfig := ProduceConfig{}
		if len(call.Arguments) > 0 {
			m := call.Arguments[0].Export().([]interface{})
			pConfig.Messages = make([]map[string]interface{}, len(m))
			for i, v := range m {
				pConfig.Messages[i] = v.(map[string]interface{})
			}
		}

		if len(call.Arguments) > 1 {
			pConfig.KeySchema = call.Arguments[1].Export().(string)
		}

		if len(call.Arguments) > 2 {
			pConfig.ValueSchema = call.Arguments[2].Export().(string)
		}

		k.Produce(
			writer, pConfig.Messages, pConfig.KeySchema,
			pConfig.ValueSchema, pConfig.AutoCreateTopic)

		return goja.Undefined()
	})
	if err != nil {
		common.Throw(rt, err)
	}

	err = writerObject.Set("produceWithConfiguration", func(call goja.FunctionCall) goja.Value {
		pConfig := ProduceConfig{}
		if len(call.Arguments) > 0 {
			m := call.Arguments[0].Export().([]interface{})
			pConfig.Messages = make([]map[string]interface{}, len(m))
			for i, v := range m {
				pConfig.Messages[i] = v.(map[string]interface{})
			}
		}

		if len(call.Arguments) > 1 {
			pConfig.ConfigurationJson = call.Arguments[1].Export().(string)
		}

		if len(call.Arguments) > 2 {
			pConfig.KeySchema = call.Arguments[2].Export().(string)
		}

		if len(call.Arguments) > 3 {
			pConfig.ValueSchema = call.Arguments[3].Export().(string)
		}

		if len(call.Arguments) > 4 {
			pConfig.AutoCreateTopic = call.Arguments[4].Export().(bool)
		}

		k.ProduceWithConfiguration(
			writer, pConfig.Messages, pConfig.ConfigurationJson,
			pConfig.KeySchema, pConfig.ValueSchema, pConfig.AutoCreateTopic)

		return goja.Undefined()
	})
	if err != nil {
		common.Throw(rt, err)
	}

	// This is unnecessary, but it's here for reference purposes
	err = writerObject.Set("close", func(call goja.FunctionCall) goja.Value {
		err := writer.Close()
		if err != nil {
			common.Throw(rt, err)
		}

		return goja.Undefined()
	})
	if err != nil {
		common.Throw(rt, err)
	}

	freeze(writerObject)

	return rt.ToValue(writerObject).ToObject(rt)
}

// Writer creates a new Kafka writer
// TODO: accept a configuration
// Deprecated: use XWriter instead
func (k *Kafka) Writer(writerConfig *WriterConfig) *kafkago.Writer {
	dialer, err := GetDialer(writerConfig.SASL, writerConfig.TLS)
	if err != nil {
		if err.Unwrap() != nil {
			logger.WithField("error", err).Error(err)
		}
		common.Throw(k.vu.Runtime(), err)
		return nil
	}

	if writerConfig.BatchSize <= 0 {
		writerConfig.BatchSize = 1
	}

	balancerType := BalancerRegistry[BALANCER_LEAST_BYTES]
	if b, ok := BalancerRegistry[writerConfig.Balancer]; ok {
		balancerType = b
	}

	// TODO: add AllowAutoTopicCreation to writer configuration
	config := kafkago.WriterConfig{
		Brokers:      writerConfig.Brokers,
		Topic:        writerConfig.Topic,
		Balancer:     balancerType,
		MaxAttempts:  writerConfig.MaxAttempts,
		BatchSize:    writerConfig.BatchSize,
		BatchBytes:   writerConfig.BatchBytes,
		BatchTimeout: writerConfig.BatchTimeout,
		ReadTimeout:  writerConfig.ReadTimeout,
		WriteTimeout: writerConfig.WriteTimeout,
		RequiredAcks: writerConfig.RequiredAcks,
		Dialer:       dialer,
		Async:        false,
	}

	if writerConfig.ConnectLogger {
		config.Logger = logger
	}

	if codec, ok := CompressionCodecs[writerConfig.Compression]; ok {
		config.CompressionCodec = compress.Codecs[codec]
	}

	// TODO: instantiate Writer directly
	writer := kafkago.NewWriter(config)
	writer.AllowAutoTopicCreation = writerConfig.AutoCreateTopic

	return writer
}

// Produce sends messages to Kafka
func (k *Kafka) Produce(
	writer *kafkago.Writer, messages []map[string]interface{},
	keySchema string, valueSchema string, autoCreateTopic bool) {
	writer.AllowAutoTopicCreation = autoCreateTopic

	k.produceInternal(writer, messages, Configuration{}, keySchema, valueSchema)
}

// ProduceWithConfiguration sends messages to Kafka with the given configuration
func (k *Kafka) ProduceWithConfiguration(
	writer *kafkago.Writer, messages []map[string]interface{},
	configurationJson string, keySchema string, valueSchema string, autoCreateTopic bool) {
	writer.AllowAutoTopicCreation = autoCreateTopic

	configuration, err := UnmarshalConfiguration(configurationJson)
	if err != nil {
		if err.Unwrap() != nil {
			logger.WithField("error", err).Error(err)
		}
		common.Throw(k.vu.Runtime(), err)
	}

	k.produceInternal(writer, messages, configuration, keySchema, valueSchema)
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
	configuration Configuration, keySchema string, valueSchema string) {
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

	k.reportWriterStats(writer.Stats())

	if originalErr != nil {
		if originalErr == k.vu.Context().Err() {
			logger.WithField("error", k.vu.Context().Err()).Error(k.vu.Context().Err())
			common.Throw(k.vu.Runtime(),
				NewXk6KafkaError(contextCancelled, "Context cancelled.", originalErr))
		} else {
			// TODO: fix this
			// Ignore stats reporting errors here, because we can't return twice,
			// and there is no way to wrap the error in another one.
			logger.WithField("error", originalErr).Error(originalErr)
			common.Throw(k.vu.Runtime(),
				NewXk6KafkaError(failedWriteMessage, "Failed to write messages.", err))
		}
	}
}

// reportWriterStats reports the writer stats to the state
func (k *Kafka) reportWriterStats(currentStats kafkago.WriterStats) {
	state := k.vu.State()
	if state == nil {
		logger.WithField("error", ErrorForbiddenInInitContext).Error(ErrorForbiddenInInitContext)
		common.Throw(k.vu.Runtime(), ErrorForbiddenInInitContext)
	}

	ctx := k.vu.Context()
	if ctx == nil {
		err := NewXk6KafkaError(cannotReportStats, "Cannot report writer stats, no context.", nil)
		logger.WithField("error", err).Info(err)
		common.Throw(k.vu.Runtime(), err)
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
}
