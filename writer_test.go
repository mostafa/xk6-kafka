package kafka

import (
	"testing"
	"time"

	"github.com/grafana/sobek"
	"github.com/riferrei/srclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProduce tests the produce function.
// nolint: funlen
func TestProduce(t *testing.T) {
	test := getTestModuleInstance(t)
	test.createTopic()

	assert.NotPanics(t, func() {
		writer := test.module.Kafka.writer(&WriterConfig{
			Brokers: []string{"localhost:9092"},
			Topic:   test.topicName,
		})
		assert.NotNil(t, writer)
		defer writer.Close()

		// Produce a message in the init context.
		assert.Panics(t, func() {
			test.module.Kafka.produce(writer, &ProduceConfig{
				Messages: []Message{
					{
						Key: test.module.Kafka.serialize(&Container{
							Data:       "key1",
							SchemaType: String,
						}),
						Value: test.module.Kafka.serialize(&Container{
							Data:       "value1",
							SchemaType: String,
						}),
					},
					{
						Key: test.module.Kafka.serialize(&Container{
							Data:       "key2",
							SchemaType: String,
						}),
						Value: test.module.Kafka.serialize(&Container{
							Data:       "value2",
							SchemaType: String,
						}),
					},
				},
			})
		})

		require.NoError(t, test.moveToVUCode())

		// Produce two messages in the VU function.
		assert.NotPanics(t, func() {
			test.module.Kafka.produce(writer, &ProduceConfig{
				Messages: []Message{
					{
						Key: test.module.Kafka.serialize(&Container{
							Data:       "key1",
							SchemaType: String,
						}),
						Value: test.module.Kafka.serialize(&Container{
							Data:       "value1",
							SchemaType: String,
						}),
					},
					{
						Key: test.module.Kafka.serialize(&Container{
							Data:       "key2",
							SchemaType: String,
						}),
						Value: test.module.Kafka.serialize(&Container{
							Data:       "value2",
							SchemaType: String,
						}),
					},
				},
			})
		})
	})

	// Check if two message were produced.
	metricsValues := test.getCounterMetricsValues()
	assert.Equal(t, 2.0, metricsValues[test.module.metrics.WriterWrites.Name])
	assert.Equal(t, 2.0, metricsValues[test.module.metrics.WriterMessages.Name])
	assert.Equal(t, 66.0, metricsValues[test.module.metrics.WriterBytes.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterErrors.Name])
	assert.GreaterOrEqual(t, 1.0, metricsValues[test.module.metrics.WriterWriteTime.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterWaitTime.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterRetries.Name])
	assert.Equal(t, 1.0, metricsValues[test.module.metrics.WriterBatchSize.Name])
	assert.Equal(t, 33.0, metricsValues[test.module.metrics.WriterBatchBytes.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterMaxAttempts.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterMaxBatchSize.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterBatchTimeout.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterReadTimeout.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterWriteTimeout.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterRequiredAcks.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterAsync.Name])
}

// TestProduceWithoutKey tests the produce function without a key.
func TestProduceWithoutKey(t *testing.T) {
	test := getTestModuleInstance(t)
	test.createTopic()

	assert.NotPanics(t, func() {
		writer := test.module.Kafka.writer(&WriterConfig{
			Brokers: []string{"localhost:9092"},
		})
		assert.NotNil(t, writer)
		defer writer.Close()

		require.NoError(t, test.moveToVUCode())

		// Produce two messages in the VU function.
		assert.NotPanics(t, func() {
			test.module.Kafka.produce(writer, &ProduceConfig{
				Messages: []Message{
					{
						Value: test.module.Kafka.serialize(&Container{
							Data:       "value1",
							SchemaType: String,
						}),
						Topic:  test.topicName,
						Offset: 0,
						Time:   time.Now(),
					},
					{
						Value: test.module.Kafka.serialize(&Container{
							Data:       "value2",
							SchemaType: String,
						}),
						Topic: test.topicName,
					},
				},
			})
		})
	})

	// Check if two message were produced.
	metricsValues := test.getCounterMetricsValues()
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterErrors.Name])
	// Notice the smaller size because the key is not present (64 -> 56).
	assert.Equal(t, 58.0, metricsValues[test.module.metrics.WriterBytes.Name])
	assert.Equal(t, 2.0, metricsValues[test.module.metrics.WriterMessages.Name])
	assert.Equal(t, 2.0, metricsValues[test.module.metrics.WriterWrites.Name])
}

// TestProducerContextCancelled tests the produce function with a cancelled context.
func TestProducerContextCancelled(t *testing.T) {
	test := getTestModuleInstance(t)
	test.createTopic()

	assert.NotPanics(t, func() {
		writer := test.module.Kafka.writer(&WriterConfig{
			Brokers: []string{"localhost:9092"},
			Topic:   test.topicName,
		})
		assert.NotNil(t, writer)
		defer writer.Close()

		require.NoError(t, test.moveToVUCode())

		// This will cancel the context, so the produce will fail.
		test.cancelContext()

		// Produce two messages in the VU function.
		assert.Panics(t, func() {
			test.module.Kafka.produce(writer, &ProduceConfig{
				Messages: []Message{
					{
						Key: test.module.Kafka.serialize(&Container{
							Data:       "key1",
							SchemaType: String,
						}),
						Value: test.module.Kafka.serialize(&Container{
							Data:       "value1",
							SchemaType: String,
						}),
					},
					{
						Key: test.module.Kafka.serialize(&Container{
							Data:       "key2",
							SchemaType: String,
						}),
						Value: test.module.Kafka.serialize(&Container{
							Data:       "value2",
							SchemaType: String,
						}),
					},
				},
			})
		})
	})

	// Cancelled context is immediately reflected in metrics, because
	// we need the context object to update the metrics.
	metricsValues := test.getCounterMetricsValues()
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterErrors.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterBytes.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterMessages.Name])
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterWrites.Name])
}

// TestProduceJSON tests the produce function with a JSON value.
func TestProduceJSON(t *testing.T) {
	test := getTestModuleInstance(t)
	test.createTopic()

	assert.NotPanics(t, func() {
		writer := test.module.Kafka.writer(&WriterConfig{
			Brokers: []string{"localhost:9092"},
			Topic:   test.topicName,
		})
		assert.NotNil(t, writer)
		defer writer.Close()

		require.NoError(t, test.moveToVUCode())

		// Produce a message in the VU function.
		assert.NotPanics(t, func() {
			test.module.Kafka.produce(writer, &ProduceConfig{
				Messages: []Message{
					{
						Value: test.module.Kafka.serialize(&Container{
							Data:       map[string]interface{}{"field": "value"},
							SchemaType: srclient.Json,
						}),
					},
				},
			})
		})
	})

	// Check if one message was produced.
	metricsValues := test.getCounterMetricsValues()
	assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterErrors.Name])
	assert.Equal(t, 40, int(metricsValues[test.module.metrics.WriterBytes.Name]))
	assert.Equal(t, 1.0, metricsValues[test.module.metrics.WriterMessages.Name])
	assert.Equal(t, 1.0, metricsValues[test.module.metrics.WriterWrites.Name])
}

// TestWriterClass tests the writer class.
func TestWriterClass(t *testing.T) {
	test := getTestModuleInstance(t)

	require.NoError(t, test.moveToVUCode())
	test.createTopic()

	assert.NotPanics(t, func() {
		writer := test.module.writerClass(sobek.ConstructorCall{
			Arguments: []sobek.Value{
				test.module.vu.Runtime().ToValue(
					map[string]interface{}{
						"brokers": []string{"localhost:9092"},
						"topic":   test.topicName,
					},
				),
			},
		})
		assert.NotNil(t, writer)

		// Produce a message.
		produce := writer.Get("produce").Export().(func(sobek.FunctionCall) sobek.Value)
		result := produce(sobek.FunctionCall{
			Arguments: []sobek.Value{
				test.module.vu.Runtime().ToValue(
					map[string]interface{}{
						"messages": []map[string]interface{}{
							{
								"key": test.module.Kafka.serialize(&Container{
									Data:       "key",
									SchemaType: String,
								}),
								"value": test.module.Kafka.serialize(&Container{
									Data:       "value",
									SchemaType: String,
								}),
							},
						},
					},
				),
			},
		}).Export()
		assert.Nil(t, result)

		// Close the writer.
		close := writer.Get("close").Export().(func(sobek.FunctionCall) sobek.Value)
		assert.NotNil(t, close)
		result = close(sobek.FunctionCall{}).Export()
		assert.Nil(t, result)

		// Check if one message was produced.
		metricsValues := test.getCounterMetricsValues()
		assert.Equal(t, 0.0, metricsValues[test.module.metrics.WriterErrors.Name])
		assert.Equal(t, 31, int(metricsValues[test.module.metrics.WriterBytes.Name]))
		assert.Equal(t, 1.0, metricsValues[test.module.metrics.WriterMessages.Name])
		assert.Equal(t, 1.0, metricsValues[test.module.metrics.WriterWrites.Name])
	})
}

func TestWriterConfig_Parse(t *testing.T) {
	var writerConfig WriterConfig
	m := map[string]any{
		"AutoCreateTopic": true,
		"ConnectLogger":   true,
		"MaxAttempts":     879,
		"BatchSize":       258,
		"BatchBytes":      652,
		"RequiredAcks":    5,
		"Topic":           "Juana Martinez",
		"Compression":     "7R8cX5PlXEN",
		"Brokers":         []string{"localhost:9092"},
		"BatchTimeout":    747,
		"ReadTimeout":     642,
		"WriteTimeout":    264,
		"SASL": map[string]any{
			"Username":   "GfiaNXfyR",
			"Password":   "Mtia8icljU",
			"Algorithm":  "XsL85F7Er",
			"AWSProfile": "MJerPEC",
		},
		"TLS": map[string]any{
			"EnableTLS":             true,
			"InsecureSkipTLSVerify": false,
			"MinVersion":            "nDZKIp5l",
			"ClientCertPem":         "kAVlkqupirI",
			"ClientKeyPem":          "MrKCYpVW",
			"ServerCaPem":           "3zbbZEoc1",
		},
	}
	require.NoError(t, writerConfig.Parse(m, sobek.New()))
	assert.Equal(t, WriterConfig{
		AutoCreateTopic: true,
		ConnectLogger:   true,
		MaxAttempts:     879,
		BatchSize:       258,
		BatchBytes:      652,
		RequiredAcks:    5,
		Topic:           "Juana Martinez",
		Compression:     "7R8cX5PlXEN",
		Brokers:         []string{"localhost:9092"},
		BatchTimeout:    747,
		ReadTimeout:     642,
		WriteTimeout:    264,
		SASL: SASLConfig{
			Username:   "GfiaNXfyR",
			Password:   "Mtia8icljU",
			Algorithm:  "XsL85F7Er",
			AWSProfile: "MJerPEC",
		},
		TLS: TLSConfig{
			EnableTLS:             true,
			InsecureSkipTLSVerify: false,
			MinVersion:            "nDZKIp5l",
			ClientCertPem:         "kAVlkqupirI",
			ClientKeyPem:          "MrKCYpVW",
			ServerCaPem:           "3zbbZEoc1",
		},
	}, writerConfig)
}
