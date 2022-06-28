package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetKafkaControllerConnection tests whether a connection can be established to a kafka broker
func TestGetKafkaControllerConnection(t *testing.T) {
	test := GetTestModuleInstance(t)
	assert.NotPanics(t, func() {
		connection := test.module.Kafka.GetKafkaControllerConnection(
			"localhost:9092", SASLConfig{}, TLSConfig{})
		defer connection.Close()
		assert.NotNil(t, connection)
	})
}

// TestGetKafkaControllerConnectionFails tests whether a connection can be established to a kafka broker
// and fails if the given broker is not reachable.
func TestGetKafkaControllerConnectionFails(t *testing.T) {
	test := GetTestModuleInstance(t)

	assert.Panics(t, func() {
		connection := test.module.Kafka.GetKafkaControllerConnection(
			"localhost:9094", SASLConfig{}, TLSConfig{})
		assert.Nil(t, connection)
	})
}

// TestTopics tests various functions to create, delete and list topics.
func TestTopics(t *testing.T) {
	test := GetTestModuleInstance(t)

	require.NoError(t, test.moveToVUCode())
	assert.NotPanics(t, func() {
		test.module.Kafka.CreateTopic(
			"localhost:9092", "test-topic", 1, 1, "", SASLConfig{}, TLSConfig{})

		topics := test.module.Kafka.ListTopics("localhost:9092", SASLConfig{}, TLSConfig{})
		assert.Contains(t, topics, "test-topic")

		test.module.Kafka.DeleteTopic("localhost:9092", "test-topic", SASLConfig{}, TLSConfig{})

		topics = test.module.Kafka.ListTopics("localhost:9092", SASLConfig{}, TLSConfig{})
		assert.NotContains(t, topics, "test-topic")
	})
}
