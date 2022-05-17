package kafka

import (
	"testing"

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

	// Create a topic before producing messages, other tests will fail.
	test.module.CreateTopic("localhost:9092", "test-topic", 1, 1, "", "")

	require.NoError(t, test.moveToVUCode())
	// Produce a message in the VU function
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
	assert.Equal(t, 2.0, metricsValues[test.module.metrics.WriterDials.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterErrors.Name])
	assert.Equal(t, 64.0, metricsValues[test.module.metrics.WriterBytes.Name])
	assert.Equal(t, 2.0, metricsValues[test.module.metrics.WriterMessages.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterRebalances.Name])
	assert.Equal(t, 2.0, metricsValues[test.module.metrics.WriterWrites.Name])
}
