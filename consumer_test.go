package kafka

import (
	"encoding/json"
	"testing"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// initializeConsumerTest creates a k6 instance with the xk6-kafka extension
// and then it creates a Kafka topic and a Kafka writer.
func initializeConsumerTest(t *testing.T) (*kafkaTest, *kafkago.Writer) {
	t.Helper()
	test := GetTestModuleInstance(t)

	// Create a Kafka topic
	connection := test.module.Kafka.GetKafkaControllerConnection(&ConnectionConfig{
		Address: "localhost:9092",
	})
	defer connection.Close()

	test.module.Kafka.CreateTopic(connection, &kafkago.TopicConfig{
		Topic: "test-topic",
	})

	// Create a writer to produce messages.
	writer := test.module.Kafka.writer(&WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "test-topic",
	})
	assert.NotNil(t, writer)

	return test, writer
}

// TestConsume tests the consume function.
// nolint: funlen
func TestConsume(t *testing.T) {
	test, writer := initializeConsumerTest(t)
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
						Key:    "key1",
						Value:  "value1",
						Offset: 0,
					},
				},
			})
		})

		// Consume a message in the VU function.
		assert.NotPanics(t, func() {
			messages := test.module.Kafka.consume(reader, &ConsumeConfig{Limit: 1})
			assert.Equal(t, 1, len(messages))
			if key, ok := messages[0]["key"].(string); ok {
				assert.Equal(t, "key1", key)
			} else {
				assert.Fail(t, "key is not a string")
			}
			if value, ok := messages[0]["value"].(string); ok {
				assert.Equal(t, "value1", value)
			} else {
				assert.Fail(t, "value is not a string")
			}
		})
	})

	// Check if one message was consumed.
	metricsValues := test.GetCounterMetricsValues()
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

	// _ = test.module.Kafka.DeleteTopic("localhost:9092", "test-topic", "")
}

// TestConsumeWithoutKey tests the consume function without a key.
func TestConsumeWithoutKey(t *testing.T) {
	test, writer := initializeConsumerTest(t)
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
						Value:  "value1",
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
			if value, ok := messages[0]["value"].(string); ok {
				assert.Equal(t, "value1", value)
			} else {
				assert.Fail(t, "value is not a string")
			}
		})
	})

	// Check if one message was consumed.
	metricsValues := test.GetCounterMetricsValues()
	assert.Equal(t, 1.0, metricsValues[test.module.metrics.ReaderDials.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.ReaderErrors.Name])
	assert.Equal(t, 6.0, metricsValues[test.module.metrics.ReaderBytes.Name])
	assert.Equal(t, 1.0, metricsValues[test.module.metrics.ReaderMessages.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.ReaderRebalances.Name])
}

// TestConsumerContextCancelled tests the consume function and fails on a cancelled context.
func TestConsumerContextCancelled(t *testing.T) {
	test, writer := initializeConsumerTest(t)
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
						Value:  "value1",
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
	metricsValues := test.GetCounterMetricsValues()
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.ReaderDials.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.ReaderErrors.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.ReaderBytes.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.ReaderMessages.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.ReaderRebalances.Name])
}

// TestConsumeJSON tests the consume function with a JSON value.
func TestConsumeJSON(t *testing.T) {
	test, writer := initializeConsumerTest(t)
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
						Value:  string(serialized),
						Offset: 3,
					},
				},
			})
		})

		// Consume the message.
		assert.NotPanics(t, func() {
			messages := test.module.Kafka.consume(reader, &ConsumeConfig{Limit: 1})
			assert.Equal(t, 1, len(messages))

			type F struct {
				Field string `json:"field"`
			}
			var f *F
			if data, ok := messages[0]["value"].(string); ok {
				jsonErr = json.Unmarshal([]byte(data), &f)
				assert.Nil(t, jsonErr)
				assert.Equal(t, "value", f.Field)
			} else {
				assert.Fail(t, "value is not a string")
			}
		})
	})
}
