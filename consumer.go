package kafka

import (
	"context"
	"errors"
	"io"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"go.k6.io/k6/js/modules"
	"go.k6.io/k6/lib"
	"go.k6.io/k6/stats"
)

func init() {
	modules.Register("k6/x/kafka", new(Kafka))
}

type Kafka struct{}

var (
	GroupBalancers = map[string]kafkago.GroupBalancer{
		"Range":        &kafkago.RangeGroupBalancer{},
		"RoundRobin":   &kafkago.RoundRobinGroupBalancer{},
		"RackAffinity": &kafkago.RackAffinityGroupBalancer{},
	}
)

type ReaderConfig struct {
	Brokers                []string               `json:"brokers"`
	Topic                  string                 `json:"topic"`
	Auth                   Credentials            `json:"auth"`
	Partition              int                    `json:"partition"`
	GroupID                string                 `json:"groupId"`
	GroupTopics            []string               `json:"groupTopics"`
	GroupBalancers         []string               `json:"groupBalancers"`
	Offset                 int64                  `json:"offset"`
	QueueCapacity          int                    `json:"queueCapacity"`
	MinBytes               int                    `json:"minBytes"`
	MaxBytes               int                    `json:"maxBytes"`
	MaxWait                time.Duration          `json:"maxWait"`
	ReadLagInterval        time.Duration          `json:"readLagInterval"`
	HeartbeatInterval      time.Duration          `json:"heartbeatInterval"`
	CommitInterval         time.Duration          `json:"commitInterval"`
	PartitionWatchInterval time.Duration          `json:"partitionWatchInterval"`
	WatchPartitionChanges  bool                   `json:"watchPartitionChanges"`
	SessionTimeout         time.Duration          `json:"sessionTimeout"`
	RebalanceTimeout       time.Duration          `json:"rebalanceTimeout"`
	JoinGroupBackoff       time.Duration          `json:"joinGroupBackoff"`
	RetentionTime          time.Duration          `json:"retentionTime"`
	StartOffset            int64                  `json:"startOffset"`
	ReadBackoffMin         time.Duration          `json:"readBackoffMin"`
	ReadBackoffMax         time.Duration          `json:"readBackoffMax"`
	IsolationLevel         kafkago.IsolationLevel `json:"isolationLevel"`
	MaxAttempts            int                    `json:"maxAttempts"`
}

func (*Kafka) Reader(rc *ReaderConfig) *kafkago.Reader {
	var dialer *kafkago.Dialer

	if rc.Auth.Username != "" && rc.Auth.Password != "" {
		dialer = getDialer(rc.Auth)
		if dialer == nil {
			ReportError(nil, "Dialer cannot authenticate")
			return nil
		}
	}

	if rc.GroupID != "" {
		rc.Partition = 0
	}

	if rc.MaxWait == 0 {
		rc.MaxWait = time.Millisecond * 200
	}

	if rc.RebalanceTimeout == 0 {
		rc.RebalanceTimeout = time.Second * 5
	}

	if rc.QueueCapacity == 0 {
		rc.QueueCapacity = 1
	}

	groupBalancers := make([]kafkago.GroupBalancer, len(rc.GroupBalancers))
	for _, groupBalancer := range rc.GroupBalancers {
		groupBalancers = append(groupBalancers, GroupBalancers[groupBalancer])
	}

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:                rc.Brokers,
		Topic:                  rc.Topic,
		Partition:              rc.Partition,
		GroupID:                rc.GroupID,
		GroupTopics:            rc.GroupTopics,
		GroupBalancers:         groupBalancers,
		MinBytes:               rc.MinBytes,
		MaxBytes:               rc.MaxBytes,
		ReadLagInterval:        rc.ReadLagInterval,
		HeartbeatInterval:      rc.HeartbeatInterval,
		CommitInterval:         rc.CommitInterval,
		PartitionWatchInterval: rc.PartitionWatchInterval,
		WatchPartitionChanges:  rc.WatchPartitionChanges,
		SessionTimeout:         rc.SessionTimeout,
		RebalanceTimeout:       rc.RebalanceTimeout,
		JoinGroupBackoff:       rc.JoinGroupBackoff,
		RetentionTime:          rc.RetentionTime,
		StartOffset:            rc.StartOffset,
		ReadBackoffMin:         rc.ReadBackoffMin,
		ReadBackoffMax:         rc.ReadBackoffMax,
		IsolationLevel:         rc.IsolationLevel,
		MaxAttempts:            rc.MaxAttempts,
		Dialer:                 dialer,
	})

	if rc.Offset > 0 {
		reader.SetOffset(rc.Offset)
	}

	return reader
}

func (*Kafka) Consume(
	ctx context.Context, reader *kafkago.Reader, limit int64,
	keySchema string, valueSchema string) []map[string]interface{} {
	return ConsumeInternal(ctx, reader, limit, Configuration{}, keySchema, valueSchema)
}

func (*Kafka) ConsumeWithConfiguration(
	ctx context.Context, reader *kafkago.Reader, limit int64, configurationJson string,
	keySchema string, valueSchema string) []map[string]interface{} {
	configuration, err := unmarshalConfiguration(configurationJson)
	if err != nil {
		ReportError(err, "Cannot unmarshal configuration "+configurationJson)
		ReportReaderStats(ctx, reader.Stats())
		return nil
	}
	return ConsumeInternal(ctx, reader, limit, configuration, keySchema, valueSchema)
}

func ConsumeInternal(
	ctx context.Context, reader *kafkago.Reader, limit int64,
	configuration Configuration, keySchema string, valueSchema string) []map[string]interface{} {
	state := lib.GetState(ctx)

	if state == nil {
		ReportError(nil, "Cannot determine state")
		ReportReaderStats(ctx, reader.Stats())
		return nil
	}

	if limit <= 0 {
		limit = 1
	}

	messages := make([]map[string]interface{}, 0)

	for i := int64(0); i < limit; i++ {
		msg, err := reader.ReadMessage(ctx)

		if err == io.EOF {
			ReportError(err, "Reached the end of queue")
			// context is cancelled, so break
			ReportReaderStats(ctx, reader.Stats())
			return messages
		}

		if err != nil {
			ReportError(err, "There was an error fetching messages")
			ReportReaderStats(ctx, reader.Stats())
			return messages
		}

		message := make(map[string]interface{})
		if len(msg.Key) > 0 {
			keyWithoutPrefix := removeMagicByteAndSchemaIdPrefix(configuration, msg.Key, "key")
			message["key"] = string(keyWithoutPrefix)
			if keySchema != "" {
				message["key"] = FromAvro(keyWithoutPrefix, keySchema)
			}
		}

		if len(msg.Value) > 0 {
			valueWithoutPrefix := removeMagicByteAndSchemaIdPrefix(configuration, msg.Value, "value")
			message["value"] = string(valueWithoutPrefix)
			if valueSchema != "" {
				message["value"] = FromAvro(valueWithoutPrefix, valueSchema)
			}
		}

		messages = append(messages, message)
	}

	ReportReaderStats(ctx, reader.Stats())

	return messages
}

func ReportReaderStats(ctx context.Context, currentStats kafkago.ReaderStats) error {
	state := lib.GetState(ctx)
	err := errors.New("state is nil")

	if state == nil {
		ReportError(err, "Cannot determine state")
		return err
	}

	tags := make(map[string]string)
	tags["clientid"] = currentStats.ClientID
	tags["topic"] = currentStats.Topic
	tags["partition"] = currentStats.Partition

	now := time.Now()

	stats.PushIfNotDone(ctx, state.Samples, stats.Sample{
		Time:   now,
		Metric: ReaderDials,
		Tags:   stats.IntoSampleTags(&tags),
		Value:  float64(currentStats.Dials),
	})

	stats.PushIfNotDone(ctx, state.Samples, stats.Sample{
		Time:   now,
		Metric: ReaderFetches,
		Tags:   stats.IntoSampleTags(&tags),
		Value:  float64(currentStats.Fetches),
	})

	stats.PushIfNotDone(ctx, state.Samples, stats.Sample{
		Time:   now,
		Metric: ReaderMessages,
		Tags:   stats.IntoSampleTags(&tags),
		Value:  float64(currentStats.Messages),
	})

	stats.PushIfNotDone(ctx, state.Samples, stats.Sample{
		Time:   now,
		Metric: ReaderBytes,
		Tags:   stats.IntoSampleTags(&tags),
		Value:  float64(currentStats.Bytes),
	})

	stats.PushIfNotDone(ctx, state.Samples, stats.Sample{
		Time:   now,
		Metric: ReaderRebalances,
		Tags:   stats.IntoSampleTags(&tags),
		Value:  float64(currentStats.Rebalances),
	})

	stats.PushIfNotDone(ctx, state.Samples, stats.Sample{
		Time:   now,
		Metric: ReaderTimeouts,
		Tags:   stats.IntoSampleTags(&tags),
		Value:  float64(currentStats.Timeouts),
	})

	stats.PushIfNotDone(ctx, state.Samples, stats.Sample{
		Time:   now,
		Metric: ReaderErrors,
		Tags:   stats.IntoSampleTags(&tags),
		Value:  float64(currentStats.Errors),
	})

	return nil
}
