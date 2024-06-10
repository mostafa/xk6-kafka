package kafka

import (
	"testing"

	"github.com/grafana/sobek"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetKafkaControllerConnection tests whether a connection can be established to a kafka broker.
func TestGetKafkaControllerConnection(t *testing.T) {
	test := getTestModuleInstance(t)
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
	test := getTestModuleInstance(t)

	assert.Panics(t, func() {
		connection := test.module.Kafka.getKafkaControllerConnection(&ConnectionConfig{
			Address: "localhost:9094",
		})
		assert.Nil(t, connection)
	})
}

// TestTopics tests various functions to create, delete and list topics.
func TestTopics(t *testing.T) {
	test := getTestModuleInstance(t)

	require.NoError(t, test.moveToVUCode())
	assert.NotPanics(t, func() {
		topic := "test-topics"
		connection := test.module.Kafka.getKafkaControllerConnection(&ConnectionConfig{
			Address: "localhost:9092",
		})

		test.module.Kafka.createTopic(connection, &kafkago.TopicConfig{
			Topic: topic,
		})

		topics := test.module.Kafka.listTopics(connection)
		assert.Contains(t, topics, topic)

		test.module.Kafka.deleteTopic(connection, topic)

		topics = test.module.Kafka.listTopics(connection)
		assert.NotContains(t, topics, topic)

		connection.Close()
	})
}

// TestConnectionClass tests the connection class that is exported to JS.
func TestConnectionClass(t *testing.T) {
	test := getTestModuleInstance(t)

	require.NoError(t, test.moveToVUCode())
	assert.NotPanics(t, func() {
		// Create a connection
		connection := test.module.Kafka.connectionClass(sobek.ConstructorCall{
			Arguments: []sobek.Value{
				test.module.vu.Runtime().ToValue(
					map[string]interface{}{
						"url": "localhost:9092",
					},
				),
			},
		})
		assert.NotNil(t, connection)

		// Create a topic
		createTopic := connection.Get("createTopic").Export().(func(sobek.FunctionCall) sobek.Value)
		assert.NotNil(t, createTopic)
		result := createTopic(sobek.FunctionCall{
			Arguments: []sobek.Value{
				test.module.vu.Runtime().ToValue(
					map[string]interface{}{
						"topic": "test-connection-class",
					},
				),
			},
		}).Export()
		assert.Nil(t, result)

		// List all topics
		listTopics := connection.Get("listTopics").Export().(func(sobek.FunctionCall) sobek.Value)
		assert.NotNil(t, listTopics)
		allTopics := listTopics(sobek.FunctionCall{}).Export().([]string)
		assert.Contains(t, allTopics, "test-connection-class")

		// Delete the topic
		deleteTopic := connection.Get("deleteTopic").Export().(func(sobek.FunctionCall) sobek.Value)
		assert.NotNil(t, deleteTopic)
		result = deleteTopic(sobek.FunctionCall{
			Arguments: []sobek.Value{
				test.module.vu.Runtime().ToValue("test-connection-class"),
			},
		}).Export()
		assert.Nil(t, result)
		allTopics = listTopics(sobek.FunctionCall{}).Export().([]string)
		assert.NotContains(t, allTopics, "test-connection-class")

		// Close the connection
		close := connection.Get("close").Export().(func(sobek.FunctionCall) sobek.Value)
		assert.NotNil(t, close)
		result = close(sobek.FunctionCall{}).Export()
		assert.Nil(t, result)
	})
}
