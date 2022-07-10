package kafka

import (
	"encoding/json"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProduce tests the produce function.
// nolint: funlen
func TestProduce(t *testing.T) {
	test := GetTestModuleInstance(t)

	assert.NotPanics(t, func() {
		writer := test.module.Kafka.writer(&WriterConfig{
			Brokers:         []string{"localhost:9092"},
			Topic:           "test-topic",
			AutoCreateTopic: true,
		})
		assert.NotNil(t, writer)
		defer writer.Close()

		// Produce a message in the init context.
		assert.Panics(t, func() {
			test.module.Kafka.produce(writer, &ProduceConfig{
				Messages: []Message{
					{
						Key:   "key1",
						Value: "value1",
					},
					{
						Key:   "key2",
						Value: "value2",
					},
				},
			})
		})

		require.NoError(t, test.moveToVUCode())

		// Produce two messages in the VU function.
		assert.NotPanics(t, func() {
			test.module.Kafka.produce(writer, &ProduceConfig{
				Messages: []Message{
					{
						Key:   "key1",
						Value: "value1",
					},
					{
						Key:   "key2",
						Value: "value2",
					},
				},
			})
		})
	})

	// Check if two message were produced.
	metricsValues := test.GetCounterMetricsValues()
	assert.Equal(t, 2.0, metricsValues[test.module.metrics.WriterWrites.Name])
	assert.Equal(t, 2.0, metricsValues[test.module.metrics.WriterMessages.Name])
	assert.Equal(t, 64.0, metricsValues[test.module.metrics.WriterBytes.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterErrors.Name])
	assert.GreaterOrEqual(t, 1.0, metricsValues[test.module.metrics.WriterWriteTime.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterWaitTime.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterRetries.Name])
	assert.Equal(t, 1.0, metricsValues[test.module.metrics.WriterBatchSize.Name])
	assert.Equal(t, 32.0, metricsValues[test.module.metrics.WriterBatchBytes.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterMaxAttempts.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterMaxBatchSize.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterBatchTimeout.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterReadTimeout.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterWriteTimeout.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterRequiredAcks.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterAsync.Name])
}

// TestProduceWithoutKey tests the produce function without a key.
func TestProduceWithoutKey(t *testing.T) {
	test := GetTestModuleInstance(t)

	assert.NotPanics(t, func() {
		writer := test.module.Kafka.writer(&WriterConfig{
			Brokers: []string{"localhost:9092"},
		})
		assert.NotNil(t, writer)
		defer writer.Close()

		// Create a topic before producing messages, otherwise tests will fail.
		assert.NotPanics(t, func() {
			connection := test.module.GetKafkaControllerConnection(&ConnectionConfig{
				Address: "localhost:9092",
			})
			test.module.CreateTopic(connection, &kafkago.TopicConfig{
				Topic:             "test-topic",
				NumPartitions:     1,
				ReplicationFactor: 1,
			})
			connection.Close()
		})

		require.NoError(t, test.moveToVUCode())

		// Produce two messages in the VU function.
		assert.NotPanics(t, func() {
			test.module.Kafka.produce(writer, &ProduceConfig{
				Messages: []Message{
					{
						Value:  "value1",
						Topic:  "test-topic",
						Offset: 0,
						Time:   time.Now(),
					},
					{
						Value: "value2",
						Topic: "test-topic",
					},
				},
			})
		})
	})

	// Check if two message were produced.
	metricsValues := test.GetCounterMetricsValues()
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterErrors.Name])
	// Notice the smaller size because the key is not present (64 -> 56).
	assert.Equal(t, 56.0, metricsValues[test.module.metrics.WriterBytes.Name])
	assert.Equal(t, 2.0, metricsValues[test.module.metrics.WriterMessages.Name])
	assert.Equal(t, 2.0, metricsValues[test.module.metrics.WriterWrites.Name])
}

// TestProducerContextCancelled tests the produce function with a cancelled context.
func TestProducerContextCancelled(t *testing.T) {
	test := GetTestModuleInstance(t)

	assert.NotPanics(t, func() {
		writer := test.module.Kafka.writer(&WriterConfig{
			Brokers:         []string{"localhost:9092"},
			Topic:           "test-topic",
			AutoCreateTopic: true,
		})
		assert.NotNil(t, writer)
		defer writer.Close()

		require.NoError(t, test.moveToVUCode())

		// This will cancel the context, so the produce will fail.
		test.cancelContext()

		// Produce two messages in the VU function.
		assert.Panics(t, func() {
			test.module.Kafka.produce(writer, &ProduceConfig{
				Messages: []Message{
					{
						Key:   "key1",
						Value: "value1",
					},
					{
						Key:   "key2",
						Value: "value2",
					},
				},
			})
		})
	})

	// Cancelled context is immediately reflected in metrics, because
	// we need the context object to update the metrics.
	metricsValues := test.GetCounterMetricsValues()
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterErrors.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterBytes.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterMessages.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterWrites.Name])
}

// TestProduceJSON tests the produce function with a JSON value.
func TestProduceJSON(t *testing.T) {
	// TODO: change this once the interfaces accept JSON

	test := GetTestModuleInstance(t)

	assert.NotPanics(t, func() {
		writer := test.module.Kafka.writer(&WriterConfig{
			Brokers:         []string{"localhost:9092"},
			Topic:           "test-topic",
			AutoCreateTopic: true,
		})
		assert.NotNil(t, writer)
		defer writer.Close()

		require.NoError(t, test.moveToVUCode())

		serialized, jsonErr := json.Marshal(map[string]interface{}{"field": "value"})
		assert.Nil(t, jsonErr)

		// Produce a message in the VU function.
		assert.NotPanics(t, func() {
			test.module.Kafka.produce(writer, &ProduceConfig{
				Messages: []Message{
					{
						Value: string(serialized),
					},
				},
			})
		})
	})

	// Check if one message was produced.
	metricsValues := test.GetCounterMetricsValues()
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterErrors.Name])
	assert.Equal(t, 39, int(metricsValues[test.module.metrics.WriterBytes.Name]))
	assert.Equal(t, 1.0, metricsValues[test.module.metrics.WriterMessages.Name])
	assert.Equal(t, 1.0, metricsValues[test.module.metrics.WriterWrites.Name])
}
