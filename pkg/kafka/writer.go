package kafka

import (
	"errors"
	"fmt"
	"time"

	"github.com/go-viper/mapstructure/v2"
	"github.com/grafana/sobek"
	"go.k6.io/k6/v2/js/common"
)

var (
	// Compression codecs.
	codecGzip   = "gzip"
	codecSnappy = "snappy"
	codecLz4    = "lz4"
	codecZstd   = "zstd"

	// Balancers.
	balancerRoundRobin = "balancer_roundrobin"
	balancerLeastBytes = "balancer_leastbytes"
	balancerHash       = "balancer_hash"
	balancerCrc32      = "balancer_crc32"
	balancerMurmur2    = "balancer_murmur2"

	errExpectedNumericByte = errors.New("expected numeric byte")
)

var supportedBalancers = map[string]struct{}{
	balancerRoundRobin: {},
	balancerLeastBytes: {},
	balancerHash:       {},
	balancerCrc32:      {},
	balancerMurmur2:    {},
}

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
	if c == nil {
		return newMissingConfigError("writer config")
	}

	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{Result: c})
	if err != nil {
		return err
	}
	if m["balancer"] != nil {
		if balancer, ok := m["balancer"].(string); ok {
			c.Balancer = balancer
		} else {
			err = runtime.ExportTo(runtime.ToValue(m["balancer"]), &c.BalancerFunc)
			if err != nil {
				return fmt.Errorf("error parsing balancerFunc: %w", err)
			}
		}
	}
	if err := decoder.Decode(m); err != nil {
		return fmt.Errorf("failed to decode writer config: %w", err)
	}
	if c.Balancer != "" {
		if _, ok := supportedBalancers[c.Balancer]; !ok {
			return fmt.Errorf("%w %q", errUnknownBalancer, c.Balancer)
		}
	}
	return nil
}

type Message struct {
	Topic string `json:"topic"`

	// Setting Partition has no effect when writing messages.
	Partition     int            `json:"partition"`
	Offset        int64          `json:"offset"`
	HighWaterMark int64          `json:"highWaterMark"`
	Key           []byte         `json:"key"`
	Value         []byte         `json:"value"`
	Headers       map[string]any `json:"headers"`

	// If not set at the creation, Time will be automatically set when
	// writing the message.
	Time time.Time `json:"time"`
}

type ProduceConfig struct {
	Messages []Message `json:"messages"`
}

func (k *Kafka) producerClass(call sobek.ConstructorCall) *sobek.Object {
	return k.compatProducerClass(call)
}

// writerClass is the deprecated Writer compatibility constructor exposed to JS.
// nolint: funlen
func (k *Kafka) writerClass(call sobek.ConstructorCall) *sobek.Object {
	return k.compatProducerClass(call)
}

func (k *Kafka) compatProducerClass(call sobek.ConstructorCall) *sobek.Object {
	runtime := k.vu.Runtime()
	if len(call.Arguments) == 0 {
		common.Throw(runtime, ErrNotEnoughArguments)
	}

	m := exportArgumentMap(runtime, call.Arguments[0], "writer config")
	var writerConfig WriterConfig
	err := writerConfig.Parse(m, runtime)
	if err != nil {
		throwConfigError(runtime, newInvalidConfigError("writer config", err))
	}
	if err := validateConfluentWriterCompatibility(&writerConfig); err != nil {
		common.Throw(runtime, err)
	}
	producer, err := NewProducerFromWriterConfig(&writerConfig)
	if err != nil {
		common.Throw(runtime, err)
	}

	producerObject := runtime.NewObject()
	if err := producerObject.Set("This", producer); err != nil {
		common.Throw(runtime, err)
	}

	err = producerObject.Set("produce", func(call sobek.FunctionCall) sobek.Value {
		var producerConfig *ProduceConfig
		if len(call.Arguments) == 0 {
			common.Throw(runtime, ErrNotEnoughArguments)
		}

		producerConfig = decodeProduceConfig(runtime, call.Argument(0))
		if producerConfig == nil {
			return sobek.Undefined()
		}

		k.produceWithProducer(producer, producerConfig)
		return sobek.Undefined()
	})
	if err != nil {
		common.Throw(runtime, err)
	}

	err = producerObject.Set("flush", func(_ sobek.FunctionCall) sobek.Value {
		if ctx := k.vu.Context(); ctx != nil {
			if err := producer.Flush(ctx); err != nil {
				common.Throw(runtime, err)
			}
		}

		return sobek.Undefined()
	})
	if err != nil {
		common.Throw(runtime, err)
	}

	err = producerObject.Set("stats", func(_ sobek.FunctionCall) sobek.Value {
		stats := producer.Stats()
		return runtime.ToValue(map[string]any{
			"pending": stats.Pending,
			// Backward-compatible alias.
			"Pending": stats.Pending,
		})
	})
	if err != nil {
		common.Throw(runtime, err)
	}

	err = producerObject.Set("close", func(_ sobek.FunctionCall) sobek.Value {
		if err := producer.Close(); err != nil {
			common.Throw(runtime, err)
		}

		return sobek.Undefined()
	})
	if err != nil {
		common.Throw(runtime, err)
	}

	if err := freeze(producerObject); err != nil {
		common.Throw(runtime, err)
	}

	return runtime.ToValue(producerObject).ToObject(runtime)
}

// decodeProduceConfig decodes a produce config from JS. It uses the fast
// JSON round-trip path (decodeArgumentMap) after normalizing message key/value
// payloads in the exported map: plain strings and number arrays are converted
// to []byte first, so they survive the base64 semantics encoding/json applies
// to []byte fields. A previous implementation used a mapstructure decoder with
// a per-field reflection hook, which roughly halved producer throughput.
func decodeProduceConfig(runtime *sobek.Runtime, value sobek.Value) *ProduceConfig {
	params := exportArgumentMap(runtime, value, "produce config")
	if params == nil {
		return nil
	}

	normalizeProduceMessagePayloads(runtime, params)

	var produceConfig ProduceConfig
	decodeArgumentMap(runtime, params, &produceConfig, "produce config")
	return &produceConfig
}

// normalizeProduceMessagePayloads converts JS string and number-array
// key/value payloads in an exported produce config map to []byte in place.
// The exported messages are []any for script-created values and
// []map[string]any for Go-created ones (for example tests using ToValue).
func normalizeProduceMessagePayloads(runtime *sobek.Runtime, params map[string]any) {
	switch messages := params["messages"].(type) {
	case []any:
		for _, message := range messages {
			if messageMap, ok := message.(map[string]any); ok {
				normalizeProduceMessagePayload(runtime, messageMap)
			}
		}
	case []map[string]any:
		for _, messageMap := range messages {
			normalizeProduceMessagePayload(runtime, messageMap)
		}
	}
}

// normalizeProduceMessagePayload normalizes the key and value of a single
// exported message map in place.
func normalizeProduceMessagePayload(runtime *sobek.Runtime, messageMap map[string]any) {
	for _, field := range []string{"key", "value"} {
		payload, exists := messageMap[field]
		if !exists {
			continue
		}
		converted, err := toRawBytes(payload)
		if err != nil {
			throwConfigError(runtime, newInvalidConfigError("produce config", err))
			return
		}
		messageMap[field] = converted
	}
}

// toRawBytes converts string and []any payloads to raw bytes; anything else
// (including []byte and nil) passes through unchanged.
func toRawBytes(payload any) (any, error) {
	switch v := payload.(type) {
	case string:
		return []byte(v), nil
	case []any:
		bytes := make([]byte, len(v))
		for i, value := range v {
			number, ok := value.(float64)
			if !ok {
				return nil, fmt.Errorf("%w at index %d, got %T", errExpectedNumericByte, i, value)
			}
			bytes[i] = byte(number)
		}
		return bytes, nil
	default:
		return payload, nil
	}
}

func validateConfluentWriterCompatibility(writerConfig *WriterConfig) *Xk6KafkaError {
	if writerConfig == nil {
		return newMissingConfigError("writer config")
	}
	if writerConfig.Balancer != "" || writerConfig.BalancerFunc != nil {
		return NewXk6KafkaError(
			unsupportedOperation,
			"Writer balancer configuration is not supported on the Confluent compatibility path.",
			nil,
		)
	}

	return nil
}

func (k *Kafka) produceWithProducer(producer *Producer, produceConfig *ProduceConfig) {
	if producer == nil {
		throwConfigError(k.vu.Runtime(), newMissingConfigError("producer"))
		return
	}
	if produceConfig == nil {
		throwConfigError(k.vu.Runtime(), newMissingConfigError("produce config"))
		return
	}

	if state := k.vu.State(); state == nil {
		logger.WithField("error", ErrForbiddenInInitContext).Error(ErrForbiddenInInitContext)
		common.Throw(k.vu.Runtime(), ErrForbiddenInInitContext)
	}

	ctx := k.vu.Context()
	if ctx == nil {
		err := NewXk6KafkaError(noContextError, "No context.", nil)
		logger.WithField("error", err).Info(err)
		common.Throw(k.vu.Runtime(), err)
	}

	startedAt := time.Now()
	if err := producer.Produce(ctx, produceConfig.Messages); err != nil {
		k.reportProducerCompatibilityMetrics(producer, produceConfig.Messages, time.Since(startedAt), err)
		logger.WithField("error", err).Error(err)
		common.Throw(k.vu.Runtime(), err)
	}

	k.reportProducerCompatibilityMetrics(producer, produceConfig.Messages, time.Since(startedAt), nil)
}
