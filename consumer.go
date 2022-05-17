package kafka

import (
	"io"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"go.k6.io/k6/metrics"
)

var DefaultDeserializer = "org.apache.kafka.common.serialization.StringDeserializer"

func (k *Kafka) Reader(
	brokers []string, topic string, partition int,
	groupID string, offset int64, auth string) (*kafkago.Reader, *Xk6KafkaError) {
	if groupID != "" {
		partition = 0
	}

	dialer, err := GetDialerFromAuth(auth)
	if err != nil {
		if err.Unwrap() != nil {
			k.logger.WithField("error", err).Error(err)
		}
		return nil, err
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
				k.logger.WithField("error", wrappedError).Warn(wrappedError)
				return reader, wrappedError
			}
		} else {
			return reader, NewXk6KafkaError(
				failedSetOffset, "Offset and groupID are mutually exclusive options, so offset is not set, yet returning the reader.", nil)
		}
	}

	return reader, nil
}

func (k *Kafka) Consume(reader *kafkago.Reader, limit int64,
	keySchema string, valueSchema string) ([]map[string]interface{}, *Xk6KafkaError) {
	return k.consumeInternal(reader, limit, Configuration{}, keySchema, valueSchema)
}

func (k *Kafka) ConsumeWithConfiguration(
	reader *kafkago.Reader, limit int64, configurationJson string,
	keySchema string, valueSchema string) ([]map[string]interface{}, *Xk6KafkaError) {
	configuration, err := UnmarshalConfiguration(configurationJson)
	if err != nil {
		if err.Unwrap() != nil {
			k.logger.WithField("error", err).Error(err)
		}
		return nil, err
	}
	return k.consumeInternal(reader, limit, configuration, keySchema, valueSchema)
}

func (k *Kafka) GetDeserializer(schema string) Deserializer {
	if de, ok := k.deserializerRegistry.Registry[schema]; ok {
		return de.GetDeserializer()
	}
	return DeserializeString
}

func (k *Kafka) consumeInternal(
	reader *kafkago.Reader, limit int64,
	configuration Configuration, keySchema string, valueSchema string) ([]map[string]interface{}, *Xk6KafkaError) {
	state := k.vu.State()
	if state == nil {
		k.logger.WithField("error", ErrorForbiddenInInitContext).Error(ErrorForbiddenInInitContext)
		return nil, ErrorForbiddenInInitContext
	}

	ctx := k.vu.Context()
	if ctx == nil {
		err := NewXk6KafkaError(noContextError, "No context.", nil)
		k.logger.WithField("error", err).Info(err)
		return nil, err
	}

	if limit <= 0 {
		limit = 1
	}

	err := ValidateConfiguration(configuration)
	if err != nil {
		configuration.Consumer.KeyDeserializer = DefaultDeserializer
		configuration.Consumer.ValueDeserializer = DefaultDeserializer
		state.Logger.WithField("error", err).Warn("Using default string serializers")
	}

	keyDeserializer := k.GetDeserializer(configuration.Consumer.KeyDeserializer)
	valueDeserializer := k.GetDeserializer(configuration.Consumer.ValueDeserializer)

	messages := make([]map[string]interface{}, 0)

	for i := int64(0); i < limit; i++ {
		msg, err := reader.ReadMessage(ctx)

		if err == io.EOF {
			err := k.reportReaderStats(reader.Stats())
			if err != nil {
				if err.Unwrap() != nil {
					k.logger.WithField("error", err).Error(err)
				}
				return nil, err
			}
			err = NewXk6KafkaError(noMoreMessages, "No more messages.", nil)
			k.logger.WithField("error", err).Info(err)
			return messages, err
		}

		if err != nil {
			err := k.reportReaderStats(reader.Stats())
			if err != nil {
				if err.Unwrap() != nil {
					k.logger.WithField("error", err).Error(err)
				}
				return messages, err
			}
			err = NewXk6KafkaError(failedReadMessage, "Unable to read messages.", nil)
			k.logger.WithField("error", err).Error(err)
			return messages, err
		}

		message := make(map[string]interface{})
		if len(msg.Key) > 0 {
			var wrappedError *Xk6KafkaError
			message["key"], wrappedError = keyDeserializer(
				configuration, reader.Config().Topic, msg.Key, Key, keySchema, 0)
			if wrappedError != nil && wrappedError.Unwrap() != nil {
				k.logger.WithField("error", wrappedError).Error(wrappedError)
			}
		}

		if len(msg.Value) > 0 {
			var wrappedError *Xk6KafkaError
			message["value"], wrappedError = valueDeserializer(
				configuration, reader.Config().Topic, msg.Value, "value", valueSchema, 0)
			if wrappedError != nil && wrappedError.Unwrap() != nil {
				k.logger.WithField("error", wrappedError).Error(wrappedError)
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

	err = k.reportReaderStats(reader.Stats())
	if err != nil {
		if err.Unwrap() != nil {
			k.logger.WithField("error", err).Error(err)
		}
		return messages, err
	}

	return messages, nil
}

func (k *Kafka) reportReaderStats(currentStats kafkago.ReaderStats) *Xk6KafkaError {
	state := k.vu.State()
	if state == nil {
		k.logger.WithField("error", ErrorForbiddenInInitContext).Error(ErrorForbiddenInInitContext)
		return ErrorForbiddenInInitContext
	}

	ctx := k.vu.Context()
	if ctx == nil {
		err := NewXk6KafkaError(contextCancelled, "No context.", nil)
		k.logger.WithField("error", err).Info(err)
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
