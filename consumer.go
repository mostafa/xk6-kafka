package kafka

import (
	"errors"
	"io"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"go.k6.io/k6/metrics"
)

var DefaultDeserializer = "org.apache.kafka.common.serialization.StringDeserializer"

func (*Kafka) Reader(
	brokers []string, topic string, partition int,
	groupID string, offset int64, auth string) *kafkago.Reader {
	if groupID != "" {
		partition = 0
	}

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:          brokers,
		Topic:            topic,
		Partition:        partition,
		GroupID:          groupID,
		MaxWait:          time.Millisecond * 200,
		RebalanceTimeout: time.Second * 5,
		QueueCapacity:    1,
		Dialer:           getDialerFromAuth(auth),
	})

	if offset > 0 {
		err := reader.SetOffset(offset)
		if err != nil {
			ReportError(err, "Unable to set offset")
			return nil
		}
	}

	return reader
}

func (k *Kafka) Consume(reader *kafkago.Reader, limit int64,
	keySchema string, valueSchema string) []map[string]interface{} {
	return k.consumeInternal(reader, limit, Configuration{}, keySchema, valueSchema)
}

func (k *Kafka) ConsumeWithConfiguration(
	reader *kafkago.Reader, limit int64, configurationJson string,
	keySchema string, valueSchema string) []map[string]interface{} {
	configuration, err := UnmarshalConfiguration(configurationJson)
	if err != nil {
		ReportError(err, "Cannot unmarshal configuration "+configurationJson)
		err = k.reportReaderStats(reader.Stats())
		if err != nil {
			ReportError(err, "Cannot report reader stats")
		}
		return nil
	}
	return k.consumeInternal(reader, limit, configuration, keySchema, valueSchema)
}

func (k *Kafka) consumeInternal(
	reader *kafkago.Reader, limit int64,
	configuration Configuration, keySchema string, valueSchema string) []map[string]interface{} {
	state := k.vu.State()
	err := errors.New("state is nil")

	if state == nil {
		ReportError(err, "Cannot determine state")
		err = k.reportReaderStats(reader.Stats())
		if err != nil {
			ReportError(err, "Cannot report reader stats")
		}
		return nil
	}

	ctx := k.vu.Context()
	err = errors.New("context is nil")

	if ctx == nil {
		ReportError(err, "Cannot determine context")
		return nil
	}

	if limit <= 0 {
		limit = 1
	}

	err = ValidateConfiguration(configuration)
	if err != nil {
		ReportError(err, "Validation of configuration failed. Falling back to defaults")
		configuration.Consumer.KeyDeserializer = DefaultDeserializer
		configuration.Consumer.ValueDeserializer = DefaultDeserializer
	}

	keyDeserializer := GetDeserializer(configuration.Consumer.KeyDeserializer, keySchema)
	valueDeserializer := GetDeserializer(configuration.Consumer.ValueDeserializer, valueSchema)

	messages := make([]map[string]interface{}, 0)

	for i := int64(0); i < limit; i++ {
		msg, err := reader.ReadMessage(ctx)

		if err == io.EOF {
			ReportError(err, "Reached the end of queue")
			// context is cancelled, so break
			err = k.reportReaderStats(reader.Stats())
			if err != nil {
				ReportError(err, "Cannot report reader stats")
			}
			return messages
		}

		if err != nil {
			ReportError(err, "There was an error fetching messages")
			err = k.reportReaderStats(reader.Stats())
			if err != nil {
				ReportError(err, "Cannot report reader stats")
			}
			return messages
		}

		message := make(map[string]interface{})
		if len(msg.Key) > 0 {
			message["key"] = keyDeserializer(configuration, msg.Key, Key, keySchema, 0)
		}

		if len(msg.Value) > 0 {
			message["value"] = valueDeserializer(configuration, msg.Value, "value", valueSchema, 0)
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

	err = k.reportReaderStats(reader.Stats())
	if err != nil {
		ReportError(err, "Cannot report reader stats")
	}

	return messages
}

func (k *Kafka) reportReaderStats(currentStats kafkago.ReaderStats) error {
	state := k.vu.State()
	err := errors.New("state is nil")

	if state == nil {
		ReportError(err, "Cannot determine state")
		return err
	}

	ctx := k.vu.Context()
	err = errors.New("context is nil")
	if ctx == nil {
		ReportError(err, "Cannot determine context")
		return err
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

	return nil
}
