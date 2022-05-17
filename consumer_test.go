package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConsume(t *testing.T) {
	test := GetTestModuleInstance(t)

	// Create a topic before consuming messages, other tests will fail.
	err := test.module.CreateTopic("localhost:9092", "test-topic", 1, 1, "", "")
	assert.Nil(t, err)

	// Create a writer to produce messages
	writer, err := test.module.Kafka.Writer([]string{"localhost:9092"}, "test-topic", "", "")
	assert.Nil(t, err)
	assert.NotNil(t, writer)
	defer writer.Close()

	reader, err := test.module.Kafka.Reader([]string{"localhost:9092"}, "test-topic", 1, "", 0, "")
	assert.Nil(t, err)
	assert.NotNil(t, reader)
	defer reader.Close()

	require.NoError(t, test.moveToVUCode())
	// Produce a message in the VU function
	err = test.module.Kafka.Produce(writer, []map[string]interface{}{
		{
			"key":   "key1",
			"value": "value1",
		},
	}, "", "")
	assert.Nil(t, err)

	// Consume a message in the VU function
	messages, err := test.module.Kafka.Consume(reader, 2, "", "")
	assert.Nil(t, err)
	assert.Equal(t, 1, len(messages))
	assert.Equal(t, "key1", messages[0]["key"].(string))
	assert.Equal(t, "value1", messages[0]["value"].(string))

	// Check if one message was consumed
	metricsValues := test.GetCounterMetricsValues()
	assert.Equal(t, 1.0, metricsValues[test.module.metrics.ReaderDials.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.ReaderErrors.Name])
	assert.Equal(t, 64.0, metricsValues[test.module.metrics.ReaderBytes.Name])
	assert.Equal(t, 1.0, metricsValues[test.module.metrics.ReaderMessages.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.ReaderRebalances.Name])
}
