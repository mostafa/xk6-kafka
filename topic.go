package kafka

import (
	"context"
	"errors"
	"net"
	"strconv"

	"github.com/grafana/sobek"
	kafkago "github.com/segmentio/kafka-go"
	"go.k6.io/k6/js/common"
)

type ConnectionConfig struct {
	Address string     `json:"address"`
	Brokers []string   `json:"brokers"`
	SASL    SASLConfig `json:"sasl"`
	TLS     TLSConfig  `json:"tls"`
}

func (k *Kafka) adminClientClass(call sobek.ConstructorCall) *sobek.Object {
	return k.compatAdminClientClass(call, false)
}

// connectionClass is a constructor for the Connection object in JS
// that creates a new connection for creating, listing and deleting topics,
// e.g. new Connection(...).
// nolint: funlen
func (k *Kafka) connectionClass(call sobek.ConstructorCall) *sobek.Object {
	return k.compatAdminClientClass(call, true)
}

func (k *Kafka) compatAdminClientClass(
	call sobek.ConstructorCall,
	legacy bool,
) *sobek.Object {
	runtime := k.vu.Runtime()
	var connectionConfig ConnectionConfig
	if len(call.Arguments) == 0 {
		common.Throw(runtime, ErrNotEnoughArguments)
	}

	decodeArgument(runtime, call.Argument(0), &connectionConfig, "connection config")

	adminClient, err := NewAdminClientFromConnectionConfig(&connectionConfig)
	if err != nil {
		common.Throw(runtime, err)
	}

	adminObject := runtime.NewObject()
	if err := adminObject.Set("This", adminClient); err != nil {
		common.Throw(runtime, err)
	}

	err = adminObject.Set("createTopic", func(call sobek.FunctionCall) sobek.Value {
		var topicConfig *kafkago.TopicConfig
		if len(call.Arguments) == 0 {
			common.Throw(runtime, ErrNotEnoughArguments)
		}

		decodeArgument(runtime, call.Argument(0), &topicConfig, "topic config")

		if err := adminClient.CreateTopic(k.adminContext(), legacyTopicConfigToTopicConfig(topicConfig)); err != nil {
			common.Throw(runtime, err)
		}
		return sobek.Undefined()
	})
	if err != nil {
		common.Throw(runtime, err)
	}

	err = adminObject.Set("deleteTopic", func(call sobek.FunctionCall) sobek.Value {
		if len(call.Arguments) > 0 {
			if topic, ok := call.Argument(0).Export().(string); !ok {
				common.Throw(runtime, ErrNotEnoughArguments)
			} else {
				if err := adminClient.DeleteTopic(k.adminContext(), topic); err != nil {
					common.Throw(runtime, err)
				}
			}
		}

		return sobek.Undefined()
	})
	if err != nil {
		common.Throw(runtime, err)
	}

	err = adminObject.Set("listTopics", func(_ sobek.FunctionCall) sobek.Value {
		topics, err := adminClient.ListTopics(k.adminContext())
		if err != nil {
			common.Throw(runtime, err)
		}
		if legacy {
			topicNames := make([]string, 0, len(topics))
			for _, topic := range topics {
				topicNames = append(topicNames, topic.Topic)
			}
			return runtime.ToValue(topicNames)
		}
		return runtime.ToValue(topics)
	})
	if err != nil {
		common.Throw(runtime, err)
	}

	err = adminObject.Set("getMetadata", func(call sobek.FunctionCall) sobek.Value {
		if len(call.Arguments) == 0 {
			common.Throw(runtime, ErrNotEnoughArguments)
		}

		topic, ok := call.Argument(0).Export().(string)
		if !ok {
			common.Throw(runtime, newInvalidConfigError("topic config", errors.New("topic must not be empty")))
		}

		metadata, err := adminClient.GetMetadata(k.adminContext(), topic)
		if err != nil {
			common.Throw(runtime, err)
		}
		return runtime.ToValue(metadata)
	})
	if err != nil {
		common.Throw(runtime, err)
	}

	err = adminObject.Set("close", func(_ sobek.FunctionCall) sobek.Value {
		if err := adminClient.Close(); err != nil {
			common.Throw(runtime, err)
		}

		return sobek.Undefined()
	})
	if err != nil {
		common.Throw(runtime, err)
	}

	if err := freeze(adminObject); err != nil {
		common.Throw(runtime, err)
	}

	return adminObject
}

func (k *Kafka) adminContext() context.Context {
	return ensureContext(k.vu.Context())
}

func legacyTopicConfigToTopicConfig(topicConfig *kafkago.TopicConfig) TopicConfig {
	if topicConfig == nil {
		return TopicConfig{}
	}

	assignments := make([][]int32, 0, len(topicConfig.ReplicaAssignments))
	for _, assignment := range topicConfig.ReplicaAssignments {
		replicas := make([]int32, 0, len(assignment.Replicas))
		for _, replica := range assignment.Replicas {
			replicas = append(replicas, int32(replica))
		}
		assignments = append(assignments, replicas)
	}

	config := make(map[string]string, len(topicConfig.ConfigEntries))
	for _, entry := range topicConfig.ConfigEntries {
		config[entry.ConfigName] = entry.ConfigValue
	}

	return TopicConfig{
		Topic:             topicConfig.Topic,
		NumPartitions:     topicConfig.NumPartitions,
		ReplicationFactor: topicConfig.ReplicationFactor,
		ReplicaAssignment: assignments,
		Config:            config,
	}
}

// getKafkaControllerConnection returns a kafka controller connection with a given node address.
// It will also try to use the auth and TLS settings to create a secure connection. The connection
// should be closed after use.
func (k *Kafka) getKafkaControllerConnection(connectionConfig *ConnectionConfig) *kafkago.Conn {
	if connectionConfig == nil {
		throwConfigError(k.vu.Runtime(), newMissingConfigError("connection config"))
		return nil
	}
	if connectionConfig.Address == "" {
		throwConfigError(
			k.vu.Runtime(),
			newInvalidConfigError("connection config", errors.New("address must not be empty")),
		)
		return nil
	}

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
		_ = conn.Close()
		wrappedError := NewXk6KafkaError(failedGetController, "Failed to get controller.", err)
		logger.WithField("error", wrappedError).Error(wrappedError)
		common.Throw(k.vu.Runtime(), wrappedError)
		return nil
	}
	_ = conn.Close()

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

// createTopic creates a topic with the given name, partitions, replication factor and compression.
// It will also try to use the auth and TLS settings to create a secure connection. If the topic
// already exists, it will do no-op.
func (k *Kafka) createTopic(conn *kafkago.Conn, topicConfig *kafkago.TopicConfig) {
	if conn == nil {
		throwConfigError(k.vu.Runtime(), newMissingConfigError("connection"))
		return
	}
	if topicConfig == nil {
		throwConfigError(k.vu.Runtime(), newMissingConfigError("topic config"))
		return
	}
	if topicConfig.Topic == "" {
		throwConfigError(
			k.vu.Runtime(),
			newInvalidConfigError("topic config", errors.New("topic must not be empty")),
		)
		return
	}

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

// deleteTopic deletes the given topic from the given address. It will also try to
// use the auth and TLS settings to create a secure connection. If the topic
// does not exist, it will raise an error.
func (k *Kafka) deleteTopic(conn *kafkago.Conn, topic string) {
	if conn == nil {
		throwConfigError(k.vu.Runtime(), newMissingConfigError("connection"))
		return
	}

	err := conn.DeleteTopics([]string{topic}...)
	if err != nil {
		wrappedError := NewXk6KafkaError(failedDeleteTopic, "Failed to delete topic.", err)
		logger.WithField("error", wrappedError).Error(wrappedError)
		common.Throw(k.vu.Runtime(), wrappedError)
	}
}

// listTopics lists the topics from the given address. It will also try to
// use the auth and TLS settings to create a secure connection. If the topic
// does not exist, it will raise an error.
func (k *Kafka) listTopics(conn *kafkago.Conn) []string {
	if conn == nil {
		throwConfigError(k.vu.Runtime(), newMissingConfigError("connection"))
		return nil
	}

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
