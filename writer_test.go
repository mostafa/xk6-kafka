package kafka

import (
	"testing"
	"time"

	"github.com/riferrei/srclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProduce tests the produce function.
// nolint: funlen
func TestProduce(t *testing.T) {
	test := getTestModuleInstance(t)
	assert.True(t, test.topicExists("test-topic"))

	assert.NotPanics(t, func() {
		writer := test.module.Kafka.writer(&WriterConfig{
			Brokers: []string{"localhost:9092"},
			Topic:   "test-topic",
		})
		assert.NotNil(t, writer)
		defer writer.Close()

		// Produce a message in the init context.
		assert.Panics(t, func() {
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
					},
					{
						Key: test.module.Kafka.serialize(&Container{
							Data:       "key2",
							SchemaType: String.String(),
						}),
						Value: test.module.Kafka.serialize(&Container{
							Data:       "value2",
							SchemaType: String.String(),
						}),
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
						Key: test.module.Kafka.serialize(&Container{
							Data:       "key1",
							SchemaType: String.String(),
						}),
						Value: test.module.Kafka.serialize(&Container{
							Data:       "value1",
							SchemaType: String.String(),
						}),
					},
					{
						Key: test.module.Kafka.serialize(&Container{
							Data:       "key2",
							SchemaType: String.String(),
						}),
						Value: test.module.Kafka.serialize(&Container{
							Data:       "value2",
							SchemaType: String.String(),
						}),
					},
				},
			})
		})
	})

	// Check if two message were produced.
	metricsValues := test.getCounterMetricsValues()
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
	test := getTestModuleInstance(t)

	assert.NotPanics(t, func() {
		writer := test.module.Kafka.writer(&WriterConfig{
			Brokers: []string{"localhost:9092"},
		})
		assert.NotNil(t, writer)
		defer writer.Close()

		require.NoError(t, test.moveToVUCode())

		// Produce two messages in the VU function.
		assert.NotPanics(t, func() {
			test.module.Kafka.produce(writer, &ProduceConfig{
				Messages: []Message{
					{
						Value: test.module.Kafka.serialize(&Container{
							Data:       "value1",
							SchemaType: String.String(),
						}),
						Topic:  "test-topic",
						Offset: 0,
						Time:   time.Now(),
					},
					{
						Value: test.module.Kafka.serialize(&Container{
							Data:       "value2",
							SchemaType: String.String(),
						}),
						Topic: "test-topic",
					},
				},
			})
		})
	})

	// Check if two message were produced.
	metricsValues := test.getCounterMetricsValues()
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterErrors.Name])
	// Notice the smaller size because the key is not present (64 -> 56).
	assert.Equal(t, 56.0, metricsValues[test.module.metrics.WriterBytes.Name])
	assert.Equal(t, 2.0, metricsValues[test.module.metrics.WriterMessages.Name])
	assert.Equal(t, 2.0, metricsValues[test.module.metrics.WriterWrites.Name])
}

// TestProducerContextCancelled tests the produce function with a cancelled context.
func TestProducerContextCancelled(t *testing.T) {
	test := getTestModuleInstance(t)

	assert.NotPanics(t, func() {
		writer := test.module.Kafka.writer(&WriterConfig{
			Brokers: []string{"localhost:9092"},
			Topic:   "test-topic",
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
						Key: test.module.Kafka.serialize(&Container{
							Data:       "key1",
							SchemaType: String.String(),
						}),
						Value: test.module.Kafka.serialize(&Container{
							Data:       "value1",
							SchemaType: String.String(),
						}),
					},
					{
						Key: test.module.Kafka.serialize(&Container{
							Data:       "key2",
							SchemaType: String.String(),
						}),
						Value: test.module.Kafka.serialize(&Container{
							Data:       "value2",
							SchemaType: String.String(),
						}),
					},
				},
			})
		})
	})

	// Cancelled context is immediately reflected in metrics, because
	// we need the context object to update the metrics.
	metricsValues := test.getCounterMetricsValues()
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterErrors.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterBytes.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterMessages.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterWrites.Name])
}

// TestProduceJSON tests the produce function with a JSON value.
func TestProduceJSON(t *testing.T) {
	test := getTestModuleInstance(t)

	assert.NotPanics(t, func() {
		writer := test.module.Kafka.writer(&WriterConfig{
			Brokers: []string{"localhost:9092"},
			Topic:   "test-topic",
		})
		assert.NotNil(t, writer)
		defer writer.Close()

		require.NoError(t, test.moveToVUCode())

		// Produce a message in the VU function.
		assert.NotPanics(t, func() {
			test.module.Kafka.produce(writer, &ProduceConfig{
				Messages: []Message{
					{
						Value: test.module.Kafka.serialize(&Container{
							Data:       map[string]interface{}{"field": "value"},
							SchemaType: srclient.Json.String(),
						}),
					},
				},
			})
		})
	})

	// Check if one message was produced.
	metricsValues := test.getCounterMetricsValues()
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterErrors.Name])
	assert.Equal(t, 39, int(metricsValues[test.module.metrics.WriterBytes.Name]))
	assert.Equal(t, 1.0, metricsValues[test.module.metrics.WriterMessages.Name])
	assert.Equal(t, 1.0, metricsValues[test.module.metrics.WriterWrites.Name])
}
