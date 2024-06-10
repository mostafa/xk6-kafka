package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/grafana/sobek"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/compress"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/metrics"
)

var (
	// Compression codecs.
	codecGzip   = "gzip"
	codecSnappy = "snappy"
	codecLz4    = "lz4"
	codecZstd   = "zstd"

	// CompressionCodecs is a map of compression codec names to their respective codecs.
	CompressionCodecs map[string]compress.Compression

	// Balancers.
	balancerRoundRobin = "balancer_roundrobin"
	balancerLeastBytes = "balancer_leastbytes"
	balancerHash       = "balancer_hash"
	balancerCrc32      = "balancer_crc32"
	balancerMurmur2    = "balancer_murmur2"

	// Balancers is a map of balancer names to their respective balancers.
	Balancers map[string]kafkago.Balancer
)

type WriterConfig struct {
	AutoCreateTopic bool          `json:"autoCreateTopic"`
	ConnectLogger   bool          `json:"connectLogger"`
	MaxAttempts     int           `json:"maxAttempts"`
	BatchSize       int           `json:"batchSize"`
	BatchBytes      int           `json:"batchBytes"`
	RequiredAcks    int           `json:"requiredAcks"`
	Topic           string        `json:"topic"`
	Balancer        string        `json:"balancer"`
	Compression     string        `json:"compression"`
	Brokers         []string      `json:"brokers"`
	BatchTimeout    time.Duration `json:"batchTimeout"`
	ReadTimeout     time.Duration `json:"readTimeout"`
	WriteTimeout    time.Duration `json:"writeTimeout"`
	SASL            SASLConfig    `json:"sasl"`
	TLS             TLSConfig     `json:"tls"`
}

type Message struct {
	Topic string `json:"topic"`

	// Setting Partition has no effect when writing messages.
	Partition     int                    `json:"partition"`
	Offset        int64                  `json:"offset"`
	HighWaterMark int64                  `json:"highWaterMark"`
	Key           []byte                 `json:"key"`
	Value         []byte                 `json:"value"`
	Headers       map[string]interface{} `json:"headers"`

	// If not set at the creation, Time will be automatically set when
	// writing the message.
	Time time.Time `json:"time"`
}

type ProduceConfig struct {
	Messages []Message `json:"messages"`
}

// writerClass is a wrapper around kafkago.writer and acts as a JS constructor
// for this extension, thus it must be called with new operator, e.g. new Writer(...).
// nolint: funlen
func (k *Kafka) writerClass(call sobek.ConstructorCall) *sobek.Object {
	runtime := k.vu.Runtime()
	var writerConfig *WriterConfig
	if len(call.Arguments) == 0 {
		common.Throw(runtime, ErrNotEnoughArguments)
	}

	if params, ok := call.Argument(0).Export().(map[string]interface{}); ok {
		if b, err := json.Marshal(params); err != nil {
			common.Throw(runtime, err)
		} else {
			if err = json.Unmarshal(b, &writerConfig); err != nil {
				common.Throw(runtime, err)
			}
		}
	}

	writer := k.writer(writerConfig)

	writerObject := runtime.NewObject()
	// This is the writer object itself.
	if err := writerObject.Set("This", writer); err != nil {
		common.Throw(runtime, err)
	}

	err := writerObject.Set("produce", func(call sobek.FunctionCall) sobek.Value {
		var producerConfig *ProduceConfig
		if len(call.Arguments) == 0 {
			common.Throw(runtime, ErrNotEnoughArguments)
		}

		if params, ok := call.Argument(0).Export().(map[string]interface{}); ok {
			b, err := json.Marshal(params)
			if err != nil {
				common.Throw(runtime, err)
			}
			err = json.Unmarshal(b, &producerConfig)
			if err != nil {
				common.Throw(runtime, err)
			}
		}

		k.produce(writer, producerConfig)
		return sobek.Undefined()
	})
	if err != nil {
		common.Throw(runtime, err)
	}

	// This is unnecessary, but it's here for reference purposes.
	err = writerObject.Set("close", func(call sobek.FunctionCall) sobek.Value {
		if err := writer.Close(); err != nil {
			common.Throw(runtime, err)
		}

		return sobek.Undefined()
	})
	if err != nil {
		common.Throw(runtime, err)
	}

	freeze(writerObject)

	return runtime.ToValue(writerObject).ToObject(runtime)
}

// writer creates a new Kafka writer.
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
		Balancer:     Balancers[balancerLeastBytes],
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

// produce sends messages to Kafka with the given configuration.
// nolint: funlen
func (k *Kafka) produce(writer *kafkago.Writer, produceConfig *ProduceConfig) {
	if state := k.vu.State(); state == nil {
		logger.WithField("error", ErrForbiddenInInitContext).Error(ErrForbiddenInInitContext)
		common.Throw(k.vu.Runtime(), ErrForbiddenInInitContext)
	}

	var ctx context.Context
	if ctx = k.vu.Context(); ctx == nil {
		err := NewXk6KafkaError(noContextError, "No context.", nil)
		logger.WithField("error", err).Info(err)
		common.Throw(k.vu.Runtime(), err)
	}

	kafkaMessages := make([]kafkago.Message, len(produceConfig.Messages))
	for index, message := range produceConfig.Messages {
		kafkaMessages[index] = kafkago.Message{
			Offset: message.Offset,
		}

		// Topic can be explicitly set on each individual message.
		// Setting topic on the writer and the messages are mutually exclusive.
		if message.Topic != "" {
			kafkaMessages[index].Topic = message.Topic
		}

		// If time is set, use it to set the time on the message,
		// otherwise use the current time.
		if !message.Time.IsZero() {
			kafkaMessages[index].Time = message.Time
		}

		// If a key was provided, add it to the message. Keys are optional.
		if message.Key != nil {
			kafkaMessages[index].Key = message.Key
		}

		// Then add the value to the message.
		if message.Value != nil {
			kafkaMessages[index].Value = message.Value
		}

		// If headers are provided, add them to the message.
		if len(message.Headers) > 0 {
			for key, value := range message.Headers {
				kafkaMessages[index].Headers = append(kafkaMessages[index].Headers, kafkago.Header{
					Key:   key,
					Value: []byte(fmt.Sprint(value)),
				})
			}
		}
	}

	originalErr := writer.WriteMessages(k.vu.Context(), kafkaMessages...)

	k.reportWriterStats(writer.Stats())

	if originalErr != nil {
		err := NewXk6KafkaError(writerError, "Error writing messages.", originalErr)
		logger.WithField("error", err).Error(err)
		common.Throw(k.vu.Runtime(), err)
	}
}

// reportWriterStats reports the writer stats to the state.
// nolint: funlen
func (k *Kafka) reportWriterStats(currentStats kafkago.WriterStats) {
	state := k.vu.State()
	if state == nil {
		logger.WithField("error", ErrForbiddenInInitContext).Error(ErrForbiddenInInitContext)
		common.Throw(k.vu.Runtime(), ErrForbiddenInInitContext)
	}

	ctx := k.vu.Context()
	if ctx == nil {
		err := NewXk6KafkaError(cannotReportStats, "Cannot report writer stats, no context.", nil)
		logger.WithField("error", err).Info(err)
		common.Throw(k.vu.Runtime(), err)
	}

	ctm := k.vu.State().Tags.GetCurrentValues()
	sampleTags := ctm.Tags.With("topic", currentStats.Topic)

	now := time.Now()

	metrics.PushIfNotDone(ctx, state.Samples, metrics.ConnectedSamples{
		Samples: []metrics.Sample{
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: k.metrics.WriterWrites,
					Tags:   sampleTags,
				},
				Value:    float64(currentStats.Writes),
				Metadata: ctm.Metadata,
			},
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: k.metrics.WriterMessages,
					Tags:   sampleTags,
				},
				Value:    float64(currentStats.Messages),
				Metadata: ctm.Metadata,
			},
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: k.metrics.WriterBytes,
					Tags:   sampleTags,
				},
				Value:    float64(currentStats.Bytes),
				Metadata: ctm.Metadata,
			},
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: k.metrics.WriterErrors,
					Tags:   sampleTags,
				},
				Value:    float64(currentStats.Errors),
				Metadata: ctm.Metadata,
			},
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: k.metrics.WriterBatchTime,
					Tags:   sampleTags,
				},
				Value:    metrics.D(currentStats.BatchTime.Avg),
				Metadata: ctm.Metadata,
			},
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: k.metrics.WriterBatchQueueTime,
					Tags:   sampleTags,
				},
				Value:    metrics.D(currentStats.BatchQueueTime.Avg),
				Metadata: ctm.Metadata,
			},
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: k.metrics.WriterWriteTime,
					Tags:   sampleTags,
				},
				Value:    metrics.D(currentStats.WriteTime.Avg),
				Metadata: ctm.Metadata,
			},
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: k.metrics.WriterWaitTime,
					Tags:   sampleTags,
				},
				Value:    metrics.D(currentStats.WaitTime.Avg),
				Metadata: ctm.Metadata,
			},
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: k.metrics.WriterRetries,
					Tags:   sampleTags,
				},
				Value:    float64(currentStats.Retries),
				Metadata: ctm.Metadata,
			},
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: k.metrics.WriterBatchSize,
					Tags:   sampleTags,
				},
				Value:    float64(currentStats.BatchSize.Avg),
				Metadata: ctm.Metadata,
			},
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: k.metrics.WriterBatchBytes,
					Tags:   sampleTags,
				},
				Value:    float64(currentStats.BatchBytes.Avg),
				Metadata: ctm.Metadata,
			},
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: k.metrics.WriterMaxAttempts,
					Tags:   sampleTags,
				},
				Value:    float64(currentStats.MaxAttempts),
				Metadata: ctm.Metadata,
			},
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: k.metrics.WriterMaxBatchSize,
					Tags:   sampleTags,
				},
				Value:    float64(currentStats.MaxBatchSize),
				Metadata: ctm.Metadata,
			},
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: k.metrics.WriterBatchTimeout,
					Tags:   sampleTags,
				},
				Value:    float64(currentStats.BatchTimeout),
				Metadata: ctm.Metadata,
			},
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: k.metrics.WriterReadTimeout,
					Tags:   sampleTags,
				},
				Value:    float64(currentStats.ReadTimeout),
				Metadata: ctm.Metadata,
			},
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: k.metrics.WriterWriteTimeout,
					Tags:   sampleTags,
				},
				Value:    float64(currentStats.WriteTimeout),
				Metadata: ctm.Metadata,
			},
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: k.metrics.WriterRequiredAcks,
					Tags:   sampleTags,
				},
				Value:    float64(currentStats.RequiredAcks),
				Metadata: ctm.Metadata,
			},
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: k.metrics.WriterAsync,
					Tags:   sampleTags,
				},
				Value:    metrics.B(currentStats.Async),
				Metadata: ctm.Metadata,
			},
		},
		Tags: sampleTags,
		Time: now,
	})
}
