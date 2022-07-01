package kafka

import (
	"encoding/json"
	"net"
	"strconv"

	"github.com/dop251/goja"
	kafkago "github.com/segmentio/kafka-go"
	"go.k6.io/k6/js/common"
)

type ConnectionConfig struct {
	Address string     `json:"address"`
	SASL    SASLConfig `json:"sasl"`
	TLS     TLSConfig  `json:"tls"`
}

func (k *Kafka) XConnection(call goja.ConstructorCall) *goja.Object {
	rt := k.vu.Runtime()
	var connectionConfig *ConnectionConfig
	if len(call.Arguments) > 0 {
		if params, ok := call.Argument(0).Export().(map[string]interface{}); ok {
			b, err := json.Marshal(params)
			if err != nil {
				common.Throw(rt, err)
			}
			err = json.Unmarshal(b, &connectionConfig)
			if err != nil {
				common.Throw(rt, err)
			}
		}
	}

	connection := k.GetKafkaControllerConnection(connectionConfig)

	connectionObject := rt.NewObject()
	// This is the connection object itself
	err := connectionObject.Set("This", connection)
	if err != nil {
		common.Throw(rt, err)
	}

	err = connectionObject.Set("createTopic", func(call goja.FunctionCall) goja.Value {
		var topicConfig *kafkago.TopicConfig
		if len(call.Arguments) > 0 {
			if params, ok := call.Argument(0).Export().(map[string]interface{}); ok {
				b, err := json.Marshal(params)
				if err != nil {
					common.Throw(rt, err)
				}
				err = json.Unmarshal(b, &topicConfig)
				if err != nil {
					common.Throw(rt, err)
				}
			}
		}

		k.CreateTopic(connection, topicConfig)
		return goja.Undefined()
	})
	if err != nil {
		common.Throw(rt, err)
	}

	err = connectionObject.Set("deleteTopic", func(call goja.FunctionCall) goja.Value {
		var topic string
		if len(call.Arguments) > 0 {
			topic = call.Argument(0).Export().(string)
		}

		k.DeleteTopic(connection, topic)
		return goja.Undefined()
	})
	if err != nil {
		common.Throw(rt, err)
	}

	err = connectionObject.Set("listTopics", func(call goja.FunctionCall) goja.Value {
		topics := k.ListTopics(connection)
		return rt.ToValue(topics)
	})
	if err != nil {
		common.Throw(rt, err)
	}

	err = connectionObject.Set("close", func(call goja.FunctionCall) goja.Value {
		connection.Close()
		return goja.Undefined()
	})
	if err != nil {
		common.Throw(rt, err)
	}

	return connectionObject
}

// GetKafkaControllerConnection returns a kafka controller connection with a given node address.
// It will also try to use the auth and TLS settings to create a secure connection. The connection
// should be closed after use.
func (k *Kafka) GetKafkaControllerConnection(connectionConfig *ConnectionConfig) *kafkago.Conn {
	dialer, wrappedError := GetDialer(connectionConfig.SASL, connectionConfig.TLS)
	if wrappedError != nil {
		logger.WithField("error", wrappedError).Error(wrappedError)
		if dialer == nil {
			common.Throw(k.vu.Runtime(), wrappedError)
			return nil
		}
	}

	ctx := k.vu.Context()
	if ctx == nil {
		err := NewXk6KafkaError(noContextError, "No context.", nil)
		logger.WithField("error", err).Info(err)
		common.Throw(k.vu.Runtime(), err)
		return nil
	}

	conn, err := dialer.DialContext(ctx, "tcp", connectionConfig.Address)
	if err != nil {
		wrappedError := NewXk6KafkaError(dialerError, "Failed to create dialer.", err)
		logger.WithField("error", wrappedError).Error(wrappedError)
		common.Throw(k.vu.Runtime(), wrappedError)
		return nil
	}

	controller, err := conn.Controller()
	if err != nil {
		wrappedError := NewXk6KafkaError(failedGetController, "Failed to get controller.", err)
		logger.WithField("error", wrappedError).Error(wrappedError)
		common.Throw(k.vu.Runtime(), wrappedError)
		return nil
	}

	controllerConn, err := dialer.DialContext(
		ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		wrappedError := NewXk6KafkaError(failedGetController, "Failed to get controller.", err)
		logger.WithField("error", wrappedError).Error(wrappedError)
		common.Throw(k.vu.Runtime(), wrappedError)
		return nil
	}

	return controllerConn
}

// CreateTopic creates a topic with the given name, partitions, replication factor and compression.
// It will also try to use the auth and TLS settings to create a secure connection. If the topic
// already exists, it will do no-op.
func (k *Kafka) CreateTopic(conn *kafkago.Conn, topicConfig *kafkago.TopicConfig) {
	if topicConfig.NumPartitions <= 0 {
		topicConfig.NumPartitions = 1
	}

	if topicConfig.ReplicationFactor <= 0 {
		topicConfig.ReplicationFactor = 1
	}

	err := conn.CreateTopics(*topicConfig)
	if err != nil {
		wrappedError := NewXk6KafkaError(failedCreateTopic, "Failed to create topic.", err)
		logger.WithField("error", wrappedError).Error(wrappedError)
		common.Throw(k.vu.Runtime(), wrappedError)
	}
}

// DeleteTopic deletes the given topic from the given address. It will also try to
// use the auth and TLS settings to create a secure connection. If the topic
// does not exist, it will raise an error.
func (k *Kafka) DeleteTopic(conn *kafkago.Conn, topic string) {
	err := conn.DeleteTopics([]string{topic}...)
	if err != nil {
		wrappedError := NewXk6KafkaError(failedDeleteTopic, "Failed to delete topic.", err)
		logger.WithField("error", wrappedError).Error(wrappedError)
		common.Throw(k.vu.Runtime(), wrappedError)
	}
}

// ListTopics lists the topics from the given address. It will also try to
// use the auth and TLS settings to create a secure connection. If the topic
// does not exist, it will raise an error.
func (k *Kafka) ListTopics(conn *kafkago.Conn) []string {
	partitions, err := conn.ReadPartitions()
	if err != nil {
		wrappedError := NewXk6KafkaError(failedReadPartitions, "Failed to read partitions.", err)
		logger.WithField("error", wrappedError).Error(wrappedError)
		common.Throw(k.vu.Runtime(), wrappedError)
		return nil
	}

	// There should be a better way to return unique set of
	// topics instead of looping over them twice
	topicSet := map[string]struct{}{}

	for _, partition := range partitions {
		topicSet[partition.Topic] = struct{}{}
	}

	topics := make([]string, 0, len(topicSet))
	for topic := range topicSet {
		topics = append(topics, topic)
	}

	return topics
}
