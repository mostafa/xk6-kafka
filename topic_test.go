package kafka

import (
	"testing"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetKafkaControllerConnection tests whether a connection can be established to a kafka broker.
func TestGetKafkaControllerConnection(t *testing.T) {
	test := GetTestModuleInstance(t)
	assert.NotPanics(t, func() {
		connection := test.module.Kafka.GetKafkaControllerConnection(&ConnectionConfig{
			Address: "localhost:9092",
		})
		assert.NotNil(t, connection)
		connection.Close()
	})
}

// TestGetKafkaControllerConnectionFails tests whether a connection can be established to a kafka broker and fails if the given broker is not reachable.
func TestGetKafkaControllerConnectionFails(t *testing.T) {
	test := GetTestModuleInstance(t)

	assert.Panics(t, func() {
		connection := test.module.Kafka.GetKafkaControllerConnection(&ConnectionConfig{
			Address: "localhost:9094",
		})
		assert.Nil(t, connection)
	})
}

// TestTopics tests various functions to create, delete and list topics.
func TestTopics(t *testing.T) {
	test := GetTestModuleInstance(t)

	require.NoError(t, test.moveToVUCode())
	assert.NotPanics(t, func() {
		connection := test.module.Kafka.GetKafkaControllerConnection(&ConnectionConfig{
			Address: "localhost:9092",
		})

		test.module.Kafka.CreateTopic(connection, &kafkago.TopicConfig{
			Topic: "test-topic",
		})

		topics := test.module.Kafka.ListTopics(connection)
		assert.Contains(t, topics, "test-topic")

		test.module.Kafka.DeleteTopic(connection, "test-topic")

		topics = test.module.Kafka.ListTopics(connection)
		assert.NotContains(t, topics, "test-topic")

		connection.Close()
	})
}
