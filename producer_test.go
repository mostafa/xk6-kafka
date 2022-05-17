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
	require.NoError(t, test.moveToVUCode())
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
	// assert.True(t, mi.Kafka.metrics.WriterMessages.Observed)
	// assert.Equal(t, 2, mi.Kafka.metrics.WriterMessages.Sink.(*metrics.CounterSink).Value)
}
