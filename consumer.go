package kafka

import (
	"context"
	"encoding/json"
	"io"
	"time"

	"github.com/dop251/goja"
	kafkago "github.com/segmentio/kafka-go"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/metrics"
)

var (
	// Group balancers.
	groupBalancerRange        = "group_balancer_range"
	groupBalancerRoundRobin   = "group_balancer_round_robin"
	groupBalancerRackAffinity = "group_balancer_rack_affinity"

	GroupBalancers map[string]kafkago.GroupBalancer

	// Isolation levels.
	isolationLevelReadUncommitted = "isolation_level_read_uncommitted"
	isolationLevelReadCommitted   = "isolation_level_read_committed"

	IsolationLevels map[string]kafkago.IsolationLevel

	DefaultDeserializer = StringDeserializer

	// Max wait time.
	MaxWait = time.Millisecond * 200

	// Rebalance timeout.
	RebalanceTimeout = time.Second * 5
)

type ReaderConfig struct {
	WatchPartitionChanges  bool          `json:"watchPartitionChanges"`
	ConnectLogger          bool          `json:"connectLogger"`
	Partition              int           `json:"partition"`
	QueueCapacity          int           `json:"queueCapacity"`
	MinBytes               int           `json:"minBytes"`
	MaxBytes               int           `json:"maxBytes"`
	MaxAttempts            int           `json:"maxAttempts"`
	GroupID                string        `json:"groupId"`
	Topic                  string        `json:"topic"`
	IsolationLevel         string        `json:"isolationLevel"`
	StartOffset            int64         `json:"startOffset"`
	Offset                 int64         `json:"offset"`
	Brokers                []string      `json:"brokers"`
	GroupTopics            []string      `json:"groupTopics"`
	GroupBalancers         []string      `json:"groupBalancers"`
	MaxWait                time.Duration `json:"maxWait"`
	ReadLagInterval        time.Duration `json:"readLagInterval"`
	HeartbeatInterval      time.Duration `json:"heartbeatInterval"`
	CommitInterval         time.Duration `json:"commitInterval"`
	PartitionWatchInterval time.Duration `json:"partitionWatchInterval"`
	SessionTimeout         time.Duration `json:"sessionTimeout"`
	RebalanceTimeout       time.Duration `json:"rebalanceTimeout"`
	JoinGroupBackoff       time.Duration `json:"joinGroupBackoff"`
	RetentionTime          time.Duration `json:"retentionTime"`
	ReadBackoffMin         time.Duration `json:"readBackoffMin"`
	ReadBackoffMax         time.Duration `json:"readBackoffMax"`
	SASL                   SASLConfig    `json:"sasl"`
	TLS                    TLSConfig     `json:"tls"`
}

type ConsumeConfig struct {
	Limit       int64         `json:"limit"`
	Config      Configuration `json:"config"`
	KeySchema   string        `json:"keySchema"`
	ValueSchema string        `json:"valueSchema"`
}

// XReader is a wrapper around kafkago.Reader and acts as a JS constructor
// for this extension, thus it must be called with new operator, e.g. new Reader(...).
// nolint: funlen
func (k *Kafka) XReader(call goja.ConstructorCall) *goja.Object {
	runtime := k.vu.Runtime()
	var readerConfig *ReaderConfig
	if len(call.Arguments) <= 0 {
		common.Throw(runtime, ErrorNotEnoughArguments)
	}

	if params, ok := call.Argument(0).Export().(map[string]interface{}); ok {
		if b, err := json.Marshal(params); err != nil {
			common.Throw(runtime, err)
		} else {
			if err = json.Unmarshal(b, &readerConfig); err != nil {
				common.Throw(runtime, err)
			}
		}
	}

	reader := k.reader(readerConfig)

	readerObject := runtime.NewObject()
	// This is the reader object itself
	if err := readerObject.Set("This", reader); err != nil {
		common.Throw(runtime, err)
	}

	err := readerObject.Set("consume", func(call goja.FunctionCall) goja.Value {
		var consumeConfig *ConsumeConfig
		if len(call.Arguments) <= 0 {
			common.Throw(runtime, ErrorNotEnoughArguments)
		}

		if params, ok := call.Argument(0).Export().(map[string]interface{}); ok {
			if b, err := json.Marshal(params); err != nil {
				common.Throw(runtime, err)
			} else {
				if err = json.Unmarshal(b, &consumeConfig); err != nil {
					common.Throw(runtime, err)
				}
			}
		}

		return runtime.ToValue(k.consume(reader, consumeConfig))
	})
	if err != nil {
		common.Throw(runtime, err)
	}

	// This is unnecessary, but it's here for reference purposes
	err = readerObject.Set("close", func(call goja.FunctionCall) goja.Value {
		if err := reader.Close(); err != nil {
			common.Throw(runtime, err)
		}

		return goja.Undefined()
	})
	if err != nil {
		common.Throw(runtime, err)
	}

	freeze(readerObject)

	return runtime.ToValue(readerObject).ToObject(runtime)
}

// reader creates a Kafka reader with the given configuration
// nolint: funlen
func (k *Kafka) reader(readerConfig *ReaderConfig) *kafkago.Reader {
	if readerConfig.GroupID != "" {
		readerConfig.Partition = 0
	}

	dialer, err := GetDialer(readerConfig.SASL, readerConfig.TLS)
	if err != nil {
		if err.Unwrap() != nil {
			logger.WithField("error", err).Error(err)
		}
		common.Throw(k.vu.Runtime(), err)
	}

	if readerConfig.MaxWait == 0 {
		readerConfig.MaxWait = MaxWait
	}

	if readerConfig.RebalanceTimeout == 0 {
		readerConfig.RebalanceTimeout = RebalanceTimeout
	}

	if readerConfig.QueueCapacity == 0 {
		readerConfig.QueueCapacity = 1
	}

	groupBalancers := []kafkago.GroupBalancer{}
	for _, balancer := range readerConfig.GroupBalancers {
		if b, ok := GroupBalancers[balancer]; ok {
			groupBalancers = append(groupBalancers, b)
		}
	}
	if len(groupBalancers) == 0 {
		// Default to [Range, RoundRobin] if no balancer is specified
		groupBalancers = append(groupBalancers, GroupBalancers[groupBalancerRange])
		groupBalancers = append(groupBalancers, GroupBalancers[groupBalancerRoundRobin])
	}

	isolationLevel := IsolationLevels[isolationLevelReadUncommitted]
	if readerConfig.IsolationLevel == "" {
		isolationLevel = IsolationLevels[readerConfig.IsolationLevel]
	}

	consolidatedConfig := kafkago.ReaderConfig{
		Brokers:                readerConfig.Brokers,
		GroupID:                readerConfig.GroupID,
		GroupTopics:            readerConfig.GroupTopics,
		Topic:                  readerConfig.Topic,
		Partition:              readerConfig.Partition,
		QueueCapacity:          readerConfig.QueueCapacity,
		MinBytes:               readerConfig.MinBytes,
		MaxBytes:               readerConfig.MaxBytes,
		MaxWait:                readerConfig.MaxWait,
		ReadLagInterval:        readerConfig.ReadLagInterval,
		GroupBalancers:         groupBalancers,
		HeartbeatInterval:      readerConfig.HeartbeatInterval,
		CommitInterval:         readerConfig.CommitInterval,
		PartitionWatchInterval: readerConfig.PartitionWatchInterval,
		WatchPartitionChanges:  readerConfig.WatchPartitionChanges,
		SessionTimeout:         readerConfig.SessionTimeout,
		RebalanceTimeout:       readerConfig.RebalanceTimeout,
		JoinGroupBackoff:       readerConfig.JoinGroupBackoff,
		RetentionTime:          readerConfig.RetentionTime,
		StartOffset:            readerConfig.StartOffset,
		ReadBackoffMin:         readerConfig.ReadBackoffMin,
		ReadBackoffMax:         readerConfig.ReadBackoffMax,
		IsolationLevel:         isolationLevel,
		MaxAttempts:            readerConfig.MaxAttempts,
		Dialer:                 dialer,
	}

	if readerConfig.ConnectLogger {
		consolidatedConfig.Logger = logger
	}

	reader := kafkago.NewReader(consolidatedConfig)

	if readerConfig.Offset > 0 {
		if readerConfig.GroupID == "" {
			if err := reader.SetOffset(readerConfig.Offset); err != nil {
				wrappedError := NewXk6KafkaError(
					failedSetOffset, "Unable to set offset, yet returning the reader.", err)
				logger.WithField("error", wrappedError).Warn(wrappedError)
				return reader
			}
		} else {
			err := NewXk6KafkaError(
				failedSetOffset, "Offset and groupID are mutually exclusive options, "+
					"so offset is not set, yet returning the reader.", nil)
			logger.WithField("error", err).Warn(err)
			return reader
		}
	}

	return reader
}

// GetDeserializer returns the deserializer for the given schema.
func (k *Kafka) GetDeserializer(schema string) Deserializer {
	if de, ok := k.deserializerRegistry.Registry[schema]; ok {
		return de.GetDeserializer()
	}
	return DeserializeString
}

// consume consumes messages from the given reader.
// nolint: funlen
func (k *Kafka) consume(
	reader *kafkago.Reader, consumeConfig *ConsumeConfig,
) []map[string]interface{} {
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

	if consumeConfig.Limit <= 0 {
		consumeConfig.Limit = 1
	}

	if err := ValidateConfiguration(consumeConfig.Config); err != nil {
		consumeConfig.Config.Consumer.KeyDeserializer = DefaultDeserializer
		consumeConfig.Config.Consumer.ValueDeserializer = DefaultDeserializer
		logger.WithField("error", err).Warn("Using default string serializers")
	}

	keyDeserializer := k.GetDeserializer(consumeConfig.Config.Consumer.KeyDeserializer)
	valueDeserializer := k.GetDeserializer(consumeConfig.Config.Consumer.ValueDeserializer)

	messages := make([]map[string]interface{}, 0)

	for i := int64(0); i < consumeConfig.Limit; i++ {
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

		// Rest of the fields of a given message
		message := map[string]interface{}{
			"topic":         msg.Topic,
			"partition":     msg.Partition,
			"offset":        msg.Offset,
			"time":          time.Unix(msg.Time.Unix(), 0).Format(time.RFC3339),
			"highWaterMark": msg.HighWaterMark,
			"headers":       make(map[string]interface{}),
		}

		if headers, ok := message["headers"].(map[string]interface{}); ok {
			for _, header := range msg.Headers {
				headers[header.Key] = header.Value
			}
		} else {
			err = NewXk6KafkaError(failedTypeCast, "Failed to cast to map.", nil)
			logger.WithField("error", err).Error(err)
		}

		if len(msg.Key) > 0 {
			var wrappedError *Xk6KafkaError
			message["key"], wrappedError = keyDeserializer(
				consumeConfig.Config, reader.Config().Topic, msg.Key,
				Key, consumeConfig.KeySchema, 0)
			if wrappedError != nil && wrappedError.Unwrap() != nil {
				logger.WithField("error", wrappedError).Error(wrappedError)
			}
		}

		if len(msg.Value) > 0 {
			var wrappedError *Xk6KafkaError
			message["value"], wrappedError = valueDeserializer(
				consumeConfig.Config, reader.Config().Topic, msg.Value,
				Value, consumeConfig.ValueSchema, 0)
			if wrappedError != nil && wrappedError.Unwrap() != nil {
				logger.WithField("error", wrappedError).Error(wrappedError)
			}
		}

		messages = append(messages, message)
	}

	k.reportReaderStats(reader.Stats())
	return messages
}

// reportReaderStats reports the reader stats
// nolint:funlen
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

	sampleTags := metrics.IntoSampleTags(&map[string]string{
		"clientid":  currentStats.ClientID,
		"topic":     currentStats.Topic,
		"partition": currentStats.Partition,
	})

	now := time.Now()
	metrics.PushIfNotDone(ctx, state.Samples, metrics.ConnectedSamples{
		Samples: []metrics.Sample{
			{
				Time:   now,
				Metric: k.metrics.ReaderDials,
				Tags:   sampleTags,
				Value:  float64(currentStats.Dials),
			},
			{
				Time:   now,
				Metric: k.metrics.ReaderFetches,
				Tags:   sampleTags,
				Value:  float64(currentStats.Fetches),
			},
			{
				Time:   now,
				Metric: k.metrics.ReaderMessages,
				Tags:   sampleTags,
				Value:  float64(currentStats.Messages),
			},
			{
				Time:   now,
				Metric: k.metrics.ReaderBytes,
				Tags:   sampleTags,
				Value:  float64(currentStats.Bytes),
			},
			{
				Time:   now,
				Metric: k.metrics.ReaderRebalances,
				Tags:   sampleTags,
				Value:  float64(currentStats.Rebalances),
			},
			{
				Time:   now,
				Metric: k.metrics.ReaderTimeouts,
				Tags:   sampleTags,
				Value:  float64(currentStats.Timeouts),
			},
			{
				Time:   now,
				Metric: k.metrics.ReaderErrors,
				Tags:   sampleTags,
				Value:  float64(currentStats.Errors),
			},
			{
				Time:   now,
				Metric: k.metrics.ReaderDialTime,
				Tags:   sampleTags,
				Value:  metrics.D(currentStats.DialTime.Avg),
			},
			{
				Time:   now,
				Metric: k.metrics.ReaderReadTime,
				Tags:   sampleTags,
				Value:  metrics.D(currentStats.ReadTime.Avg),
			},
			{
				Time:   now,
				Metric: k.metrics.ReaderWaitTime,
				Tags:   sampleTags,
				Value:  metrics.D(currentStats.WaitTime.Avg),
			},
			{
				Time:   now,
				Metric: k.metrics.ReaderFetchSize,
				Tags:   sampleTags,
				Value:  float64(currentStats.FetchSize.Avg),
			},
			{
				Time:   now,
				Metric: k.metrics.ReaderFetchBytes,
				Tags:   sampleTags,
				Value:  float64(currentStats.FetchBytes.Min),
			},
			{
				Time:   now,
				Metric: k.metrics.ReaderFetchBytes,
				Tags:   sampleTags,
				Value:  float64(currentStats.FetchBytes.Max),
			},
			{
				Time:   now,
				Metric: k.metrics.ReaderOffset,
				Tags:   sampleTags,
				Value:  float64(currentStats.Offset),
			},
			{
				Time:   now,
				Metric: k.metrics.ReaderLag,
				Tags:   sampleTags,
				Value:  float64(currentStats.Lag),
			},
			{
				Time:   now,
				Metric: k.metrics.ReaderMinBytes,
				Tags:   sampleTags,
				Value:  float64(currentStats.MinBytes),
			},
			{
				Time:   now,
				Metric: k.metrics.ReaderMaxBytes,
				Tags:   sampleTags,
				Value:  float64(currentStats.MaxBytes),
			},
			{
				Time:   now,
				Metric: k.metrics.ReaderMaxWait,
				Tags:   sampleTags,
				Value:  metrics.D(currentStats.MaxWait),
			},
			{
				Time:   now,
				Metric: k.metrics.ReaderQueueLength,
				Tags:   sampleTags,
				Value:  float64(currentStats.QueueLength),
			},
			{
				Time:   now,
				Metric: k.metrics.ReaderQueueCapacity,
				Tags:   sampleTags,
				Value:  float64(currentStats.QueueCapacity),
			},
		},
		Tags: sampleTags,
		Time: now,
	})
}
