package kafka

import (
	"context"
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

	// Balancers is a map of balancer names to their respective balancers.
	Balancers map[string]kafkago.Balancer

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

type Message struct {
	Topic string `json:"topic"`

	// Setting Partition has no effect when writing messages
	Partition     int                    `json:"partition"`
	Offset        int64                  `json:"offset"`
	HighWaterMark int64                  `json:"highWaterMark"`
	Key           string                 `json:"key"`
	Value         string                 `json:"value"`
	Headers       map[string]interface{} `json:"headers"`

	// If not set at the creation, Time will be automatically set when
	// writing the message.
	Time time.Time `json:"time"`
}

type ProduceConfig struct {
	Messages    []Message     `json:"messages"`
	Config      Configuration `json:"config"`
	KeySchema   string        `json:"keySchema"`
	ValueSchema string        `json:"valueSchema"`
}

// XWriter is a wrapper around kafkago.Writer and acts as a JS constructor
// for this extension, thus it must be called with new operator, e.g. new Writer(...).
func (k *Kafka) XWriter(call goja.ConstructorCall) *goja.Object {
	rt := k.vu.Runtime()
	var writerConfig *WriterConfig
	if len(call.Arguments) > 0 {
		if params, ok := call.Argument(0).Export().(map[string]interface{}); ok {
			if b, err := json.Marshal(params); err != nil {
				common.Throw(rt, err)
			} else {
				if err = json.Unmarshal(b, &writerConfig); err != nil {
					common.Throw(rt, err)
				}
			}
		}
	}

	writer := k.writer(writerConfig)

	writerObject := rt.NewObject()
	// This is the writer object itself
	if err := writerObject.Set("This", writer); err != nil {
		common.Throw(rt, err)
	}

	err := writerObject.Set("produce", func(call goja.FunctionCall) goja.Value {
		var producerConfig *ProduceConfig
		if len(call.Arguments) > 0 {
			if params, ok := call.Argument(0).Export().(map[string]interface{}); ok {
				b, err := json.Marshal(params)
				if err != nil {
					common.Throw(rt, err)
				}
				err = json.Unmarshal(b, &producerConfig)
				if err != nil {
					common.Throw(rt, err)
				}
			}
		}

		k.produce(writer, producerConfig)
		return goja.Undefined()
	})
	if err != nil {
		common.Throw(rt, err)
	}

	// This is unnecessary, but it's here for reference purposes
	err = writerObject.Set("close", func(call goja.FunctionCall) goja.Value {
		if err := writer.Close(); err != nil {
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

// writer creates a new Kafka writer
func (k *Kafka) writer(writerConfig *WriterConfig) *kafkago.Writer {
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

	writer := &kafkago.Writer{
		Addr:         kafkago.TCP(writerConfig.Brokers...),
		Topic:        writerConfig.Topic,
		Balancer:     Balancers[BALANCER_LEAST_BYTES],
		MaxAttempts:  writerConfig.MaxAttempts,
		BatchSize:    writerConfig.BatchSize,
		BatchBytes:   int64(writerConfig.BatchBytes),
		BatchTimeout: writerConfig.BatchTimeout,
		ReadTimeout:  writerConfig.ReadTimeout,
		WriteTimeout: writerConfig.WriteTimeout,
		RequiredAcks: kafkago.RequiredAcks(writerConfig.RequiredAcks),
		Async:        false,
		Transport: &kafkago.Transport{
			TLS:      dialer.TLS,
			SASL:     dialer.SASLMechanism,
			ClientID: dialer.ClientID,
		},
		AllowAutoTopicCreation: writerConfig.AutoCreateTopic,
	}

	if balancer, ok := Balancers[writerConfig.Balancer]; ok {
		writer.Balancer = balancer
	}

	if codec, ok := CompressionCodecs[writerConfig.Compression]; ok {
		writer.Compression = codec
	}

	if writerConfig.ConnectLogger {
		writer.Logger = logger
	}

	return writer
}

// GetSerializer returns the serializer for the given schema
func (k *Kafka) GetSerializer(schema string) Serializer {
	if ser, ok := k.serializerRegistry.Registry[schema]; ok {
		return ser.GetSerializer()
	}
	return SerializeString
}

// produce sends messages to Kafka with the given configuration
// nolint: funlen
func (k *Kafka) produce(writer *kafkago.Writer, produceConfig *ProduceConfig) {
	if state := k.vu.State(); state == nil {
		logger.WithField("error", ErrorForbiddenInInitContext).Error(ErrorForbiddenInInitContext)
		common.Throw(k.vu.Runtime(), ErrorForbiddenInInitContext)
	}

	var ctx context.Context
	if ctx = k.vu.Context(); ctx == nil {
		err := NewXk6KafkaError(noContextError, "No context.", nil)
		logger.WithField("error", err).Info(err)
		common.Throw(k.vu.Runtime(), err)
	}

	var err error
	if err = ValidateConfiguration(produceConfig.Config); err != nil {
		produceConfig.Config.Producer.KeySerializer = DefaultSerializer
		produceConfig.Config.Producer.ValueSerializer = DefaultSerializer
		logger.WithField("error", err).Warn("Using default string serializers")
	}

	keySerializer := k.GetSerializer(produceConfig.Config.Producer.KeySerializer)
	valueSerializer := k.GetSerializer(produceConfig.Config.Producer.ValueSerializer)

	kafkaMessages := make([]kafkago.Message, len(produceConfig.Messages))
	for i, message := range produceConfig.Messages {
		kafkaMessages[i] = kafkago.Message{
			Offset: message.Offset,
		}

		// Topic can be explicitly set on each individual message
		// Setting topic on the writer and the messages are mutually exclusive
		if message.Topic != "" {
			kafkaMessages[i].Topic = message.Topic
		}

		// If time is set, use it to set the time on the message,
		// otherwise use the current time.
		if !message.Time.IsZero() {
			kafkaMessages[i].Time = message.Time
		}

		// If a key was provided, add it to the message. Keys are optional.
		if message.Key != "" {
			keyData, err := keySerializer(
				produceConfig.Config, writer.Stats().Topic,
				message.Key, Key, produceConfig.KeySchema, 0)
			if err != nil && err.Unwrap() != nil {
				logger.WithField("error", err).Error(err)
			}

			kafkaMessages[i].Key = keyData
		}

		// Then add the value to the message.
		valueData, err := valueSerializer(
			produceConfig.Config, writer.Stats().Topic,
			message.Value, Value, produceConfig.ValueSchema, 0)
		if err != nil && err.Unwrap() != nil {
			logger.WithField("error", err).Error(err)
		}

		kafkaMessages[i].Value = valueData

		// If headers are provided, add them to the message.
		if len(message.Headers) > 0 {
			for key, value := range message.Headers {
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
// nolint: funlen
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

	sampleTags := metrics.IntoSampleTags(&map[string]string{
		"topic": currentStats.Topic,
	})

	now := time.Now()

	metrics.PushIfNotDone(ctx, state.Samples, metrics.ConnectedSamples{
		Samples: []metrics.Sample{
			{
				Time:   now,
				Metric: k.metrics.WriterWrites,
				Tags:   sampleTags,
				Value:  float64(currentStats.Writes),
			},
			{
				Time:   now,
				Metric: k.metrics.WriterMessages,
				Tags:   sampleTags,
				Value:  float64(currentStats.Messages),
			},
			{
				Time:   now,
				Metric: k.metrics.WriterBytes,
				Tags:   sampleTags,
				Value:  float64(currentStats.Bytes),
			},
			{
				Time:   now,
				Metric: k.metrics.WriterErrors,
				Tags:   sampleTags,
				Value:  float64(currentStats.Errors),
			},
			{
				Time:   now,
				Metric: k.metrics.WriterWriteTime,
				Tags:   sampleTags,
				Value:  metrics.D(currentStats.WriteTime.Avg),
			},
			{
				Time:   now,
				Metric: k.metrics.WriterWaitTime,
				Tags:   sampleTags,
				Value:  metrics.D(currentStats.WaitTime.Avg),
			},
			{
				Time:   now,
				Metric: k.metrics.WriterRetries,
				Tags:   sampleTags,
				Value:  float64(currentStats.Retries.Avg),
			},
			{
				Time:   now,
				Metric: k.metrics.WriterBatchSize,
				Tags:   sampleTags,
				Value:  float64(currentStats.BatchSize.Avg),
			},
			{
				Time:   now,
				Metric: k.metrics.WriterBatchBytes,
				Tags:   sampleTags,
				Value:  float64(currentStats.BatchBytes.Avg),
			},
			{
				Time:   now,
				Metric: k.metrics.WriterMaxAttempts,
				Tags:   sampleTags,
				Value:  float64(currentStats.MaxAttempts),
			},
			{
				Time:   now,
				Metric: k.metrics.WriterMaxBatchSize,
				Tags:   sampleTags,
				Value:  float64(currentStats.MaxBatchSize),
			},
			{
				Time:   now,
				Metric: k.metrics.WriterBatchTimeout,
				Tags:   sampleTags,
				Value:  float64(currentStats.BatchTimeout),
			},
			{
				Time:   now,
				Metric: k.metrics.WriterReadTimeout,
				Tags:   sampleTags,
				Value:  float64(currentStats.ReadTimeout),
			},
			{
				Time:   now,
				Metric: k.metrics.WriterWriteTimeout,
				Tags:   sampleTags,
				Value:  float64(currentStats.WriteTimeout),
			},
			{
				Time:   now,
				Metric: k.metrics.WriterRequiredAcks,
				Tags:   sampleTags,
				Value:  float64(currentStats.RequiredAcks),
			},
			{
				Time:   now,
				Metric: k.metrics.WriterAsync,
				Tags:   sampleTags,
				Value:  metrics.B(currentStats.Async),
			},
		},
		Tags: sampleTags,
		Time: now,
	})
}
