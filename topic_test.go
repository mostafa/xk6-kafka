package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetKafkaConnection(t *testing.T) {
	test := GetTestModuleInstance(t)
	connection, xk6KafkaError := test.module.Kafka.GetKafkaConnection("localhost:9092", "")
	assert.Nil(t, xk6KafkaError)
	assert.NotNil(t, connection)
}

func TestGetKafkaConnectionFails(t *testing.T) {
	test := GetTestModuleInstance(t)

	connection, xk6KafkaError := test.module.Kafka.GetKafkaConnection("localhost:9094", "")
	assert.Nil(t, connection)
	assert.NotNil(t, xk6KafkaError)
	assert.Contains(t, xk6KafkaError.Unwrap().Error(), "failed to dial: failed to open connection to localhost:9094")
	assert.Equal(t, xk6KafkaError.Code, dialerError)
}

func TestTopics(t *testing.T) {
	test := GetTestModuleInstance(t)

	require.NoError(t, test.moveToVUCode())
	err := test.module.Kafka.CreateTopic("localhost:9092", "test-topic", 1, 1, "", "")
	assert.NoError(t, err)

	topics, err := test.module.Kafka.ListTopics("localhost:9092", "")
	assert.NoError(t, err)
	assert.Contains(t, topics, "test-topic")

	err = test.module.Kafka.DeleteTopic("localhost:9092", "test-topic", "")
	assert.NoError(t, err)

	topics, err = test.module.Kafka.ListTopics("localhost:9092", "")
	assert.NoError(t, err)
	assert.NotContains(t, topics, "test-topic")
}
