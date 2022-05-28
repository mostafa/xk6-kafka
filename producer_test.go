package kafka

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProduce(t *testing.T) {
	test := GetTestModuleInstance(t)

	writer, err := test.module.Kafka.Writer([]string{"localhost:9092"}, "test-topic", "", "")
	assert.Nil(t, err)
	assert.NotNil(t, writer)
	defer writer.Close()

	// Produce a message in the init context
	err = test.module.Kafka.Produce(writer, []map[string]interface{}{
		{
			"key":   "key1",
			"value": "value1",
		},
		{
			"key":   "key2",
			"value": "value2",
		},
	}, "", "")
	assert.NotNil(t, err)
	assert.Equal(t, ErrorForbiddenInInitContext, err)

	// Create a topic before producing messages, otherwise tests will fail.
	err = test.module.CreateTopic("localhost:9092", "test-topic", 1, 1, "", "")
	assert.Nil(t, err)

	require.NoError(t, test.moveToVUCode())
	// Produce two messages in the VU function
	err = test.module.Kafka.Produce(writer, []map[string]interface{}{
		{
			"key":   "key1",
			"value": "value1",
		},
		{
			"key":   "key2",
			"value": "value2",
		},
	}, "", "")
	assert.Nil(t, err)

	// Check if two message were produced
	metricsValues := test.GetCounterMetricsValues()
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterErrors.Name])
	assert.Equal(t, 64.0, metricsValues[test.module.metrics.WriterBytes.Name])
	assert.Equal(t, 2.0, metricsValues[test.module.metrics.WriterMessages.Name])
	assert.Equal(t, 2.0, metricsValues[test.module.metrics.WriterWrites.Name])
}

func TestProduceWithoutKey(t *testing.T) {
	test := GetTestModuleInstance(t)

	writer, err := test.module.Kafka.Writer([]string{"localhost:9092"}, "", "", "")
	assert.Nil(t, err)
	assert.NotNil(t, writer)
	defer writer.Close()

	// Create a topic before producing messages, otherwise tests will fail.
	err = test.module.CreateTopic("localhost:9092", "test-topic", 1, 1, "", "")
	assert.Nil(t, err)

	require.NoError(t, test.moveToVUCode())
	// Produce two messages in the VU function
	err = test.module.Kafka.Produce(writer, []map[string]interface{}{
		{
			"value": "value1",
			// The topic should be set either on the writer or on individual messages
			"topic":  "test-topic",
			"offset": int64(0),
			"time":   time.Now().UnixMilli(),
		},
		{
			"value": "value2",
			"topic": "test-topic",
		},
	}, "", "")
	assert.Nil(t, err)

	// Check if two message were produced
	metricsValues := test.GetCounterMetricsValues()
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterErrors.Name])
	// Notice the smaller size because the key is not present (64 -> 56)
	assert.Equal(t, 56.0, metricsValues[test.module.metrics.WriterBytes.Name])
	assert.Equal(t, 2.0, metricsValues[test.module.metrics.WriterMessages.Name])
	assert.Equal(t, 2.0, metricsValues[test.module.metrics.WriterWrites.Name])
}

func TestProducerContextCancelled(t *testing.T) {
	test := GetTestModuleInstance(t)

	writer, err := test.module.Kafka.Writer([]string{"localhost:9092"}, "test-topic", "", "")
	assert.Nil(t, err)
	assert.NotNil(t, writer)
	defer writer.Close()

	// Create a topic before producing messages, otherwise tests will fail.
	err = test.module.CreateTopic("localhost:9092", "test-topic", 1, 1, "", "")
	assert.Nil(t, err)

	require.NoError(t, test.moveToVUCode())
	// This will cancel the context, so the produce will fail

	test.cancelContext()

	// Produce two messages in the VU function
	err = test.module.Kafka.Produce(writer, []map[string]interface{}{
		{
			"key":   "key1",
			"value": "value1",
		},
		{
			"key":   "key2",
			"value": "value2",
		},
	}, "", "")
	assert.NotNil(t, err)
	assert.Equal(t, "Context cancelled.", err.Message)
	assert.Equal(t, context.Canceled, err.Unwrap())

	// Cancelled context is immediately reflected in metrics, because
	// we need the context object to update the metrics.
	metricsValues := test.GetCounterMetricsValues()
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterErrors.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterBytes.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterMessages.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterWrites.Name])
}

func TestProduceJSON(t *testing.T) {
	// TODO: change this once the interfaces accept JSON

	test := GetTestModuleInstance(t)

	writer, err := test.module.Kafka.Writer([]string{"localhost:9092"}, "test-topic", "", "")
	assert.Nil(t, err)

	// Create a topic before producing messages, otherwise tests will fail.
	err = test.module.CreateTopic("localhost:9092", "test-topic", 1, 1, "", "")
	assert.Nil(t, err)

	require.NoError(t, test.moveToVUCode())

	serialized, jsonErr := json.Marshal(map[string]interface{}{"field": "value"})
	assert.Nil(t, jsonErr)

	// Produce a message in the VU function
	err = test.module.Kafka.Produce(writer, []map[string]interface{}{
		{
			"value": string(serialized),
		},
	}, "", "")
	assert.Nil(t, err)

	// Check if one message was produced
	metricsValues := test.GetCounterMetricsValues()
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterErrors.Name])
	assert.Equal(t, 39, int(metricsValues[test.module.metrics.WriterBytes.Name]))
	assert.Equal(t, 1.0, metricsValues[test.module.metrics.WriterMessages.Name])
	assert.Equal(t, 1.0, metricsValues[test.module.metrics.WriterWrites.Name])
}
