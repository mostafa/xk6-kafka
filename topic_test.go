package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetKafkaConnection(t *testing.T) {
	rt, mi := GetTestModuleInstance(t)
	assert.NotNil(t, rt)
	assert.NotNil(t, mi)

	connection, xk6KafkaError := mi.Kafka.GetKafkaConnection("localhost:9092", "")
	assert.Nil(t, xk6KafkaError)
	assert.NotNil(t, connection)
}

func TestGetKafkaConnectionFails(t *testing.T) {
	rt, mi := GetTestModuleInstance(t)
	assert.NotNil(t, rt)
	assert.NotNil(t, mi)

	connection, xk6KafkaError := mi.Kafka.GetKafkaConnection("localhost:9094", "")
	assert.Nil(t, connection)
	assert.NotNil(t, xk6KafkaError)
	assert.Contains(t, xk6KafkaError.Unwrap().Error(), "failed to dial: failed to open connection to localhost:9094")
	assert.Equal(t, xk6KafkaError.Code, dialerError)
}

func TestTopics(t *testing.T) {
	rt, mi := GetTestModuleInstance(t)
	assert.NotNil(t, rt)
	assert.NotNil(t, mi)

	err := mi.Kafka.CreateTopic("localhost:9092", "test-topic", 1, 1, "", "")
	assert.Nil(t, err)

	topics, err := mi.Kafka.ListTopics("localhost:9092", "")
	assert.Nil(t, err)
	assert.Contains(t, topics, "test-topic")

	err = mi.Kafka.DeleteTopic("localhost:9092", "test-topic", "")
	assert.Nil(t, err)

	topics, err = mi.Kafka.ListTopics("localhost:9092", "")
	assert.Nil(t, err)
	assert.NotContains(t, topics, "test-topic")
}
