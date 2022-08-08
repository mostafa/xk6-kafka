package kafka

import (
	"encoding/json"
	"testing"

	"github.com/riferrei/srclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConsume tests the consume function.
// nolint: funlen
func TestConsume(t *testing.T) {
	test := getTestModuleInstance(t)
	test.createTopic("test-topic")
	writer := test.newWriter("test-topic")
	defer writer.Close()

	assert.True(t, test.topicExists("test-topic"))

	// Create a reader to consume messages.
	assert.NotPanics(t, func() {
		reader := test.module.Kafka.reader(&ReaderConfig{
			Brokers: []string{"localhost:9092"},
			Topic:   "test-topic",
		})
		assert.NotNil(t, reader)
		defer reader.Close()

		// Switch to VU code.
		require.NoError(t, test.moveToVUCode())

		// Produce a message in the VU function.
		assert.NotPanics(t, func() {
			test.module.Kafka.produce(writer, &ProduceConfig{
				Messages: []Message{
					{
						Key: test.module.Kafka.serialize(&Container{
							Data:       "key1",
							SchemaType: String.String(),
						}),
						Value: test.module.Kafka.serialize(&Container{
							Data:       "value1",
							SchemaType: String.String(),
						}),
						Offset: 0,
					},
				},
			})
		})

		// Consume a message in the VU function.
		assert.NotPanics(t, func() {
			messages := test.module.Kafka.consume(reader, &ConsumeConfig{Limit: 1})
			assert.Equal(t, 1, len(messages))

			result := test.module.Kafka.deserialize(&Container{
				Data:       messages[0]["key"],
				SchemaType: String.String(),
			})

			if key, ok := result.([]byte); ok {
				assert.Equal(t, "key1", string(key))
			}

			result = test.module.Kafka.deserialize(&Container{
				Data:       messages[0]["value"],
				SchemaType: String.String(),
			})
			if value, ok := result.([]byte); ok {
				assert.Equal(t, "value1", string(value))
			}
		})
	})

	// Check if one message was consumed.
	metricsValues := test.getCounterMetricsValues()
	assert.Equal(t, 1.0, metricsValues[test.module.metrics.ReaderDials.Name])
	assert.Equal(t, 2.0, metricsValues[test.module.metrics.ReaderFetches.Name])
	assert.Equal(t, 1.0, metricsValues[test.module.metrics.ReaderMessages.Name])
	assert.Equal(t, 10.0, metricsValues[test.module.metrics.ReaderBytes.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.ReaderRebalances.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.ReaderTimeouts.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.ReaderErrors.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.ReaderDialTime.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.ReaderReadTime.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.ReaderWaitTime.Name])
	assert.Equal(t, 1.0, metricsValues[test.module.metrics.ReaderFetchSize.Name])
	assert.Equal(t, 10.0, metricsValues[test.module.metrics.ReaderFetchBytes.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.ReaderOffset.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.ReaderLag.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.ReaderMinBytes.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.ReaderMaxBytes.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.ReaderMaxWait.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.ReaderQueueLength.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.ReaderQueueCapacity.Name])
}

// TestConsumeWithoutKey tests the consume function without a key.
func TestConsumeWithoutKey(t *testing.T) {
	test := getTestModuleInstance(t)
	writer := test.newWriter("test-topic")
	defer writer.Close()

	// Create a reader to consume messages.
	assert.NotPanics(t, func() {
		reader := test.module.Kafka.reader(&ReaderConfig{
			Brokers: []string{"localhost:9092"},
			Topic:   "test-topic",
			Offset:  1,
		})
		assert.NotNil(t, reader)
		defer reader.Close()

		// Switch to VU code.
		require.NoError(t, test.moveToVUCode())

		// Produce a message in the VU function.
		assert.NotPanics(t, func() {
			test.module.Kafka.produce(writer, &ProduceConfig{
				Messages: []Message{
					{
						Value: test.module.Kafka.serialize(&Container{
							Data:       "value1",
							SchemaType: String.String(),
						}),
						Offset: 1,
					},
				},
			})
		})

		// Consume a message in the VU function.
		assert.NotPanics(t, func() {
			messages := test.module.Kafka.consume(reader, &ConsumeConfig{Limit: 1})
			assert.Equal(t, 1, len(messages))
			assert.NotContains(t, messages[0], "key")

			result := test.module.Kafka.deserialize(&Container{
				Data:       messages[0]["value"],
				SchemaType: String.String(),
			})
			if value, ok := result.([]byte); ok {
				assert.Equal(t, "value1", string(value))
			}
		})
	})

	// Check if one message was consumed.
	metricsValues := test.getCounterMetricsValues()
	assert.Equal(t, 1.0, metricsValues[test.module.metrics.ReaderDials.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.ReaderErrors.Name])
	assert.Equal(t, 6.0, metricsValues[test.module.metrics.ReaderBytes.Name])
	assert.Equal(t, 1.0, metricsValues[test.module.metrics.ReaderMessages.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.ReaderRebalances.Name])
}

// TestConsumerContextCancelled tests the consume function and fails on a cancelled context.
func TestConsumerContextCancelled(t *testing.T) {
	test := getTestModuleInstance(t)
	writer := test.newWriter("test-topic")
	defer writer.Close()

	// Create a reader to consume messages.
	assert.NotPanics(t, func() {
		reader := test.module.Kafka.reader(&ReaderConfig{
			Brokers: []string{"localhost:9092"},
			Topic:   "test-topic",
		})
		assert.NotNil(t, reader)
		defer reader.Close()

		// Switch to VU code.
		require.NoError(t, test.moveToVUCode())

		// Produce a message in the VU function.
		assert.NotPanics(t, func() {
			test.module.Kafka.produce(writer, &ProduceConfig{
				Messages: []Message{
					{
						Value: test.module.Kafka.serialize(&Container{
							Data:       "value1",
							SchemaType: String.String(),
						}),
						Offset: 2,
					},
				},
			})
		})

		test.cancelContext()

		// Consume a message in the VU function.
		assert.NotPanics(t, func() {
			messages := test.module.Kafka.consume(reader, &ConsumeConfig{Limit: 1})
			assert.Empty(t, messages)
		})
	})

	// Check if one message was consumed.
	metricsValues := test.getCounterMetricsValues()
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.ReaderDials.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.ReaderErrors.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.ReaderBytes.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.ReaderMessages.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.ReaderRebalances.Name])
}

// TestConsumeJSON tests the consume function with a JSON value.
func TestConsumeJSON(t *testing.T) {
	test := getTestModuleInstance(t)
	writer := test.newWriter("test-topic")
	defer writer.Close()

	// Create a reader to consume messages.
	assert.NotPanics(t, func() {
		reader := test.module.Kafka.reader(&ReaderConfig{
			Brokers: []string{"localhost:9092"},
			Topic:   "test-topic",
			Offset:  3,
		})
		assert.NotNil(t, reader)
		defer reader.Close()

		// Switch to VU code.
		require.NoError(t, test.moveToVUCode())

		serialized, jsonErr := json.Marshal(map[string]interface{}{"field": "value"})
		assert.Nil(t, jsonErr)

		// Produce a message in the VU function.
		assert.NotPanics(t, func() {
			test.module.Kafka.produce(writer, &ProduceConfig{
				Messages: []Message{
					{
						Value:  serialized,
						Offset: 3,
					},
				},
			})
		})

		// Consume the message.
		assert.NotPanics(t, func() {
			messages := test.module.Kafka.consume(reader, &ConsumeConfig{Limit: 1})
			assert.Equal(t, 1, len(messages))

			result := test.module.Kafka.deserialize(&Container{
				Data:       messages[0]["value"],
				SchemaType: srclient.Json.String(),
			})
			if data, ok := result.(map[string]interface{}); ok {
				assert.Equal(t, "value", data["field"])
			}
		})
	})
}
