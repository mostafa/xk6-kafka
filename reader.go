package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/grafana/sobek"
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

	// Start offsets.
	lastOffset  = "start_offset_last_offset"
	firstOffset = "start_offset_first_offset"

	StartOffsets map[string]int64

	RebalanceTimeout       = time.Second * 5
	HeartbeatInterval      = time.Second * 3
	SessionTimeout         = time.Second * 30
	PartitionWatchInterval = time.Second * 5
	JoinGroupBackoff       = time.Second * 5
	RetentionTime          = time.Hour * 24
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
	StartOffset            string        `json:"startOffset"`
	Offset                 int64         `json:"offset"`
	Brokers                []string      `json:"brokers"`
	GroupTopics            []string      `json:"groupTopics"`
	GroupBalancers         []string      `json:"groupBalancers"`
	MaxWait                Duration      `json:"maxWait"`
	ReadBatchTimeout       time.Duration `json:"readBatchTimeout"`
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
	OffsetOutOfRangeError  bool          `json:"offsetOutOfRangeError"` // deprecated, do not use
	SASL                   SASLConfig    `json:"sasl"`
	TLS                    TLSConfig     `json:"tls"`
}

type ConsumeConfig struct {
	Limit         int64 `json:"limit"`
	NanoPrecision bool  `json:"nanoPrecision"`
}

type Duration struct {
	time.Duration
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}

	switch value := v.(type) {
	case string:
		var err error
		d.Duration, err = time.ParseDuration(value)
		if err != nil {
			return err
		}
		return nil
	default:
		return errors.New("invalid duration")
	}
}

// readerClass is a wrapper around kafkago.reader and acts as a JS constructor
// for this extension, thus it must be called with new operator, e.g. new Reader(...).
// nolint: funlen
func (k *Kafka) readerClass(call sobek.ConstructorCall) *sobek.Object {
	runtime := k.vu.Runtime()
	var readerConfig *ReaderConfig
	if len(call.Arguments) == 0 {
		common.Throw(runtime, ErrNotEnoughArguments)
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

	err := readerObject.Set("consume", func(call sobek.FunctionCall) sobek.Value {
		var consumeConfig *ConsumeConfig
		if len(call.Arguments) == 0 {
			common.Throw(runtime, ErrNotEnoughArguments)
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
	err = readerObject.Set("close", func(call sobek.FunctionCall) sobek.Value {
		if err := reader.Close(); err != nil {
			common.Throw(runtime, err)
		}

		return sobek.Undefined()
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
	dialer, err := GetDialer(readerConfig.SASL, readerConfig.TLS)
	if err != nil {
		if err.Unwrap() != nil {
			logger.WithField("error", err).Error(err)
		}
		common.Throw(k.vu.Runtime(), err)
	}

	if readerConfig.Partition != 0 && readerConfig.GroupID != "" {
		common.Throw(k.vu.Runtime(), ErrPartitionAndGroupID)
	}

	if readerConfig.Topic != "" && readerConfig.GroupID != "" {
		common.Throw(k.vu.Runtime(), ErrTopicAndGroupID)
	}

	if readerConfig.GroupID != "" &&
		len(readerConfig.GroupTopics) >= 0 &&
		readerConfig.HeartbeatInterval == 0 {
		readerConfig.HeartbeatInterval = HeartbeatInterval
	}

	if readerConfig.GroupID != "" && readerConfig.SessionTimeout == 0 {
		readerConfig.SessionTimeout = SessionTimeout
	}

	if readerConfig.GroupID != "" && readerConfig.RebalanceTimeout == 0 {
		readerConfig.RebalanceTimeout = RebalanceTimeout
	}

	if readerConfig.GroupID != "" && readerConfig.JoinGroupBackoff == 0 {
		readerConfig.JoinGroupBackoff = JoinGroupBackoff
	}

	if readerConfig.GroupID != "" && readerConfig.PartitionWatchInterval == 0 {
		readerConfig.PartitionWatchInterval = PartitionWatchInterval
	}

	if readerConfig.GroupID != "" && readerConfig.RetentionTime == 0 {
		readerConfig.RetentionTime = RetentionTime
	}

	var groupBalancers []kafkago.GroupBalancer
	if readerConfig.GroupID != "" {
		groupBalancers = make([]kafkago.GroupBalancer, 0, len(readerConfig.GroupBalancers))
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
	}

	isolationLevel := IsolationLevels[isolationLevelReadUncommitted]
	if readerConfig.IsolationLevel != "" {
		isolationLevel = IsolationLevels[readerConfig.IsolationLevel]
	}

	var startOffset int64
	if readerConfig.GroupID != "" && readerConfig.StartOffset != "" {
		startOffset = StartOffsets[firstOffset] // Default to FirstOffset
		if s, ok := StartOffsets[readerConfig.StartOffset]; ok {
			startOffset = s
		}
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
		MaxWait:                readerConfig.MaxWait.Duration,
		ReadBatchTimeout:       readerConfig.ReadBatchTimeout,
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
		StartOffset:            startOffset,
		ReadBackoffMin:         readerConfig.ReadBackoffMin,
		ReadBackoffMax:         readerConfig.ReadBackoffMax,
		IsolationLevel:         isolationLevel,
		MaxAttempts:            readerConfig.MaxAttempts,
		OffsetOutOfRangeError:  readerConfig.OffsetOutOfRangeError,
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

// consume consumes messages from the given reader.
// nolint: funlen
func (k *Kafka) consume(
	reader *kafkago.Reader, consumeConfig *ConsumeConfig,
) []map[string]interface{} {
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

	if consumeConfig.Limit <= 0 {
		consumeConfig.Limit = 1
	}

	messages := make([]map[string]interface{}, 0)

	maxWait := reader.Config().MaxWait

	for i := int64(0); i < consumeConfig.Limit; i++ {
		ctxWithTimeout, cancel := context.WithTimeout(ctx, maxWait)
		msg, err := reader.ReadMessage(ctxWithTimeout)
		cancel()
		if errors.Is(err, io.EOF) {
			k.reportReaderStats(reader.Stats())

			err = NewXk6KafkaError(noMoreMessages, "No more messages.", nil)
			logger.WithField("error", err).Info(err)
			common.Throw(k.vu.Runtime(), err)
		}

		if err != nil {
			k.reportReaderStats(reader.Stats())

			err = NewXk6KafkaError(failedReadMessage, "Unable to read messages.", err)
			logger.WithField("error", err).Error(err)
			common.Throw(k.vu.Runtime(), err)
		}

		var messageTime string
		if consumeConfig.NanoPrecision {
			messageTime = msg.Time.Format(time.RFC3339Nano)
		} else {
			messageTime = time.Unix(msg.Time.Unix(), 0).Format(time.RFC3339)
		}

		// Rest of the fields of a given message
		message := map[string]interface{}{
			"topic":         msg.Topic,
			"partition":     msg.Partition,
			"offset":        msg.Offset,
			"time":          messageTime,
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
			message["key"] = msg.Key
		}

		if len(msg.Value) > 0 {
			message["value"] = msg.Value
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
		logger.WithField("error", ErrForbiddenInInitContext).Error(ErrForbiddenInInitContext)
		common.Throw(k.vu.Runtime(), ErrForbiddenInInitContext)
	}

	ctx := k.vu.Context()
	if ctx == nil {
		err := NewXk6KafkaError(noContextError, "No context.", nil)
		logger.WithField("error", err).Info(err)
		common.Throw(k.vu.Runtime(), err)
	}

	ctm := k.vu.State().Tags.GetCurrentValues()
	sampleTags := ctm.Tags.With("topic", currentStats.Topic)
	sampleTags = sampleTags.With("clientid", currentStats.ClientID)
	sampleTags = sampleTags.With("partition", currentStats.Partition)

	now := time.Now()
	metrics.PushIfNotDone(ctx, state.Samples, metrics.ConnectedSamples{
		Samples: []metrics.Sample{
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: k.metrics.ReaderDials,
					Tags:   sampleTags,
				},
				Value:    float64(currentStats.Dials),
				Metadata: ctm.Metadata,
			},
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: k.metrics.ReaderFetches,
					Tags:   sampleTags,
				},
				Value:    float64(currentStats.Fetches),
				Metadata: ctm.Metadata,
			},
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: k.metrics.ReaderMessages,
					Tags:   sampleTags,
				},
				Value:    float64(currentStats.Messages),
				Metadata: ctm.Metadata,
			},
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: k.metrics.ReaderBytes,
					Tags:   sampleTags,
				},
				Value:    float64(currentStats.Bytes),
				Metadata: ctm.Metadata,
			},
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: k.metrics.ReaderRebalances,
					Tags:   sampleTags,
				},
				Value:    float64(currentStats.Rebalances),
				Metadata: ctm.Metadata,
			},
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: k.metrics.ReaderTimeouts,
					Tags:   sampleTags,
				},
				Value:    float64(currentStats.Timeouts),
				Metadata: ctm.Metadata,
			},
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: k.metrics.ReaderErrors,
					Tags:   sampleTags,
				},
				Value:    float64(currentStats.Errors),
				Metadata: ctm.Metadata,
			},
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: k.metrics.ReaderDialTime,
					Tags:   sampleTags,
				},
				Value:    metrics.D(currentStats.DialTime.Avg),
				Metadata: ctm.Metadata,
			},
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: k.metrics.ReaderReadTime,
					Tags:   sampleTags,
				},
				Value:    metrics.D(currentStats.ReadTime.Avg),
				Metadata: ctm.Metadata,
			},
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: k.metrics.ReaderWaitTime,
					Tags:   sampleTags,
				},
				Value:    metrics.D(currentStats.WaitTime.Avg),
				Metadata: ctm.Metadata,
			},
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: k.metrics.ReaderFetchSize,
					Tags:   sampleTags,
				},
				Value:    float64(currentStats.FetchSize.Avg),
				Metadata: ctm.Metadata,
			},
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: k.metrics.ReaderFetchBytes,
					Tags:   sampleTags,
				},
				Value:    float64(currentStats.FetchBytes.Min),
				Metadata: ctm.Metadata,
			},
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: k.metrics.ReaderFetchBytes,
					Tags:   sampleTags,
				},
				Value:    float64(currentStats.FetchBytes.Max),
				Metadata: ctm.Metadata,
			},
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: k.metrics.ReaderOffset,
					Tags:   sampleTags,
				},
				Value:    float64(currentStats.Offset),
				Metadata: ctm.Metadata,
			},
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: k.metrics.ReaderLag,
					Tags:   sampleTags,
				},
				Value:    float64(currentStats.Lag),
				Metadata: ctm.Metadata,
			},
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: k.metrics.ReaderMinBytes,
					Tags:   sampleTags,
				},
				Value:    float64(currentStats.MinBytes),
				Metadata: ctm.Metadata,
			},
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: k.metrics.ReaderMaxBytes,
					Tags:   sampleTags,
				},
				Value:    float64(currentStats.MaxBytes),
				Metadata: ctm.Metadata,
			},
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: k.metrics.ReaderMaxWait,
					Tags:   sampleTags,
				},
				Value:    metrics.D(currentStats.MaxWait),
				Metadata: ctm.Metadata,
			},
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: k.metrics.ReaderQueueLength,
					Tags:   sampleTags,
				},
				Value:    float64(currentStats.QueueLength),
				Metadata: ctm.Metadata,
			},
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: k.metrics.ReaderQueueCapacity,
					Tags:   sampleTags,
				},
				Value:    float64(currentStats.QueueCapacity),
				Metadata: ctm.Metadata,
			},
		},
		Tags: sampleTags,
		Time: now,
	})
}
