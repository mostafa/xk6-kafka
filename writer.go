package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-viper/mapstructure/v2"
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
	defaultBalancer    = balancerLeastBytes

	// Balancers is a map of balancer names to their respective balancers.
	Balancers map[string]kafkago.Balancer
)

type WriterConfig struct {
	AutoCreateTopic bool            `mapstructure:"autoCreateTopic"`
	ConnectLogger   bool            `mapstructure:"connectLogger"`
	MaxAttempts     int             `mapstructure:"maxAttempts"`
	BatchSize       int             `mapstructure:"batchSize"`
	BatchBytes      int             `mapstructure:"batchBytes"`
	RequiredAcks    int             `mapstructure:"requiredAcks"`
	Topic           string          `mapstructure:"topic"`
	Balancer        string          `mapstructure:"-"`
	BalancerFunc    BalancerKeyFunc `mapstructure:"-"`
	Compression     string          `mapstructure:"compression"`
	Brokers         []string        `mapstructure:"brokers"`
	BatchTimeout    time.Duration   `mapstructure:"batchTimeout"`
	ReadTimeout     time.Duration   `mapstructure:"readTimeout"`
	WriteTimeout    time.Duration   `mapstructure:"writeTimeout"`
	SASL            SASLConfig      `mapstructure:"sasl"`
	TLS             TLSConfig       `mapstructure:"tls"`
}

func (c *WriterConfig) Parse(m map[string]any, runtime *sobek.Runtime) error {
	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{Result: &c})
	if err != nil {
		return err
	}
	if m["balancer"] != nil {
		if balancer, ok := m["balancer"].(string); ok {
			c.Balancer = balancer
		} else {
			err = runtime.ExportTo(runtime.ToValue(m["balancer"]), &c.Balancer)
			if err != nil {
				return fmt.Errorf("error parsing balancerFunc: %w", err)
			}
		}
	}
	return decoder.Decode(m)
}

func (c *WriterConfig) GetBalancer() kafkago.Balancer {
	if c.BalancerFunc != nil {
		return kafkago.BalancerFunc(func(message kafkago.Message, partitions ...int) int {
			if message.Key == nil {
				panic(fmt.Sprintf("Trying to use balancer function specified in Writer, but message key is nil: %#v", message))
			}
			return c.BalancerFunc(message.Key, partitions...)
		})
	}
	if c.Balancer != "" {
		return Balancers[c.Balancer]
	}
	return Balancers[defaultBalancer]
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
	if len(call.Arguments) == 0 {
		common.Throw(runtime, ErrNotEnoughArguments)
	}

	m := map[string]any{}
	err := runtime.ExportTo(call.Arguments[0], &m)
	if err != nil {
		common.Throw(runtime, fmt.Errorf("can't export: %w", err))
	}
	var writerConfig WriterConfig
	err = writerConfig.Parse(m, runtime)
	if err != nil {
		common.Throw(runtime, fmt.Errorf("can't parse writerConfig: %w", err))
	}
	writer := k.writer(&writerConfig)

	writerObject := runtime.NewObject()
	// This is the writer object itself.
	if err := writerObject.Set("This", writer); err != nil {
		common.Throw(runtime, err)
	}

	err = writerObject.Set("produce", func(call sobek.FunctionCall) sobek.Value {
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
		Balancer:     writerConfig.GetBalancer(),
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
				Value:    metrics.D(currentStats.BatchTimeout),
				Metadata: ctm.Metadata,
			},
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: k.metrics.WriterReadTimeout,
					Tags:   sampleTags,
				},
				Value:    metrics.D(currentStats.ReadTimeout),
				Metadata: ctm.Metadata,
			},
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: k.metrics.WriterWriteTimeout,
					Tags:   sampleTags,
				},
				Value:    metrics.D(currentStats.WriteTimeout),
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
