package kafka

import (
	"testing"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

// TestGetKafkaControllerConnection tests whether a connection can be established to a kafka broker.
func TestGetKafkaControllerConnection(t *testing.T) {
	test := GetTestModuleInstance(t)
	assert.NotPanics(t, func() {
		connection := test.module.Kafka.getKafkaControllerConnection(&ConnectionConfig{
			Address: "localhost:9092",
		})
		assert.NotNil(t, connection)
		connection.Close()
	})
}

// TestGetKafkaControllerConnectionFails tests whether a connection can be established
// to a kafka broker and fails if the given broker is not reachable.
func TestGetKafkaControllerConnectionFails(t *testing.T) {
	test := GetTestModuleInstance(t)

	assert.Panics(t, func() {
		connection := test.module.Kafka.getKafkaControllerConnection(&ConnectionConfig{
			Address: "localhost:9094",
		})
		assert.Nil(t, connection)
	})
}

// TestTopics tests various functions to create, delete and list topics.
func TestTopics(t *testing.T) {
	test := GetTestModuleInstance(t)

	test.rt.MoveToVUContext(test.rt.VU.StateField)
	assert.NotPanics(t, func() {
		connection := test.module.Kafka.getKafkaControllerConnection(&ConnectionConfig{
			Address: "localhost:9092",
		})

		test.module.Kafka.createTopic(connection, &kafkago.TopicConfig{
			Topic: "test-topic",
		})

		topics := test.module.Kafka.listTopics(connection)
		assert.Contains(t, topics, "test-topic")

		test.module.Kafka.deleteTopic(connection, "test-topic")

		topics = test.module.Kafka.listTopics(connection)
		assert.NotContains(t, topics, "test-topic")

		connection.Close()
	})
}
