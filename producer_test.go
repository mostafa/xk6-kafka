package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProduce(t *testing.T) {
	rt, mi := GetTestModuleInstance(t)
	assert.NotNil(t, rt)
	assert.NotNil(t, mi)

	writer, err := mi.Kafka.Writer([]string{"localhost:9092"}, "test-topic", "", "")
	assert.Nil(t, err)
	assert.NotNil(t, writer)
	defer writer.Close()

	err = mi.Kafka.Produce(writer, []map[string]interface{}{
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
