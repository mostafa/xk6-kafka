package kafka

import (
	"crypto/tls"

	"github.com/dop251/goja"
	"github.com/segmentio/kafka-go/compress"
	"github.com/sirupsen/logrus"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"
	"go.k6.io/k6/lib/netext"
)

var (
	// logger is globally used by the Kafka module.
	logger *logrus.Logger
)

// init registers the xk6-kafka module as 'k6/x/kafka'
func init() {
	// Initialize the global logger
	logger = logrus.New()

	// Initialize the TLS versions map
	TLSVersions = map[string]uint16{
		netext.TLS_1_0: tls.VersionTLS10,
		netext.TLS_1_1: tls.VersionTLS11,
		netext.TLS_1_2: tls.VersionTLS12,
		netext.TLS_1_3: tls.VersionTLS13,
	}

	CompressionCodecs = map[string]compress.Compression{
		CODEC_GZIP:   compress.Gzip,
		CODEC_SNAPPY: compress.Snappy,
		CODEC_LZ4:    compress.Lz4,
		CODEC_ZSTD:   compress.Zstd,
	}

	// Register the module namespace (aka. JS import path)
	modules.Register("k6/x/kafka", New())
}

type (
	Kafka struct {
		vu                   modules.VU
		metrics              kafkaMetrics
		serializerRegistry   *Serde[Serializer]
		deserializerRegistry *Serde[Deserializer]
		exports              *goja.Object
	}
	RootModule  struct{}
	KafkaModule struct {
		*Kafka
	}
)

var (
	_ modules.Instance = &KafkaModule{}
	_ modules.Module   = &RootModule{}
)

// New creates a new instance of the root module
func New() *RootModule {
	return &RootModule{}
}

// NewModuleInstance creates a new instance of the Kafka module
func (*RootModule) NewModuleInstance(vu modules.VU) modules.Instance {
	rt := vu.Runtime()

	m, err := registerMetrics(vu)
	if err != nil {
		common.Throw(vu.Runtime(), err)
	}

	// Create a new Kafka module
	kafkaModuleInstance := &KafkaModule{
		Kafka: &Kafka{
			vu:                   vu,
			metrics:              m,
			serializerRegistry:   NewSerializersRegistry(),
			deserializerRegistry: NewDeserializersRegistry(),
			exports:              rt.NewObject(),
		},
	}

	// Export constants to the JS code
	kafkaModuleInstance.defineConstants()

	mustExport := func(name string, value interface{}) {
		if err := kafkaModuleInstance.exports.Set(name, value); err != nil {
			common.Throw(rt, err)
		}
	}

	// Export the functions from the Kafka module to the JS code
	mustExport("writer", kafkaModuleInstance.Writer)
	mustExport("produce", kafkaModuleInstance.Produce)
	mustExport("produceWithConfiguration", kafkaModuleInstance.ProduceWithConfiguration)
	mustExport("reader", kafkaModuleInstance.Reader)
	mustExport("consume", kafkaModuleInstance.Consume)
	mustExport("consumeWithConfiguration", kafkaModuleInstance.ConsumeWithConfiguration)
	mustExport("createTopic", kafkaModuleInstance.CreateTopic)
	mustExport("deleteTopic", kafkaModuleInstance.DeleteTopic)
	mustExport("listTopics", kafkaModuleInstance.ListTopics)

	// This causes the struct fields to be exported to the native (camelCases) JS code.
	vu.Runtime().SetFieldNameMapper(goja.TagFieldNameMapper("json", true))

	return kafkaModuleInstance
}

// Exports returns the exports of the Kafka module, which are the functions
// that can be called from the JS code.
func (c *KafkaModule) Exports() modules.Exports {
	return modules.Exports{
		Default: c.Kafka.exports,
	}
}

func (c *KafkaModule) defineConstants() {
	rt := c.vu.Runtime()
	mustAddProp := func(name, val string) {
		err := c.exports.DefineDataProperty(
			name, rt.ToValue(val), goja.FLAG_FALSE, goja.FLAG_FALSE, goja.FLAG_TRUE,
		)
		if err != nil {
			common.Throw(rt, err)
		}
	}

	// TLS versions
	mustAddProp("TLS_1_0", netext.TLS_1_0)
	mustAddProp("TLS_1_1", netext.TLS_1_1)
	mustAddProp("TLS_1_2", netext.TLS_1_2)
	mustAddProp("TLS_1_3", netext.TLS_1_3)

	// SASL mechanisms
	mustAddProp("NONE", NONE)
	mustAddProp("SASL_PLAIN", SASL_PLAIN)
	mustAddProp("SASL_SCRAM_SHA256", SASL_SCRAM_SHA256)
	mustAddProp("SASL_SCRAM_SHA512", SASL_SCRAM_SHA512)
	mustAddProp("SASL_SSL", SASL_SSL)

	// Compression codecs
	mustAddProp("CODEC_GZIP", CODEC_GZIP)
	mustAddProp("CODEC_SNAPPY", CODEC_SNAPPY)
	mustAddProp("CODEC_LZ4", CODEC_LZ4)
	mustAddProp("CODEC_ZSTD", CODEC_ZSTD)

	// Serde types
	mustAddProp("STRING_SERIALIZER", StringSerializer)
	mustAddProp("STRING_DESERIALIZER", StringDeserializer)
	mustAddProp("BYTE_ARRAY_SERIALIZER", ByteArraySerializer)
	mustAddProp("BYTE_ARRAY_DESERIALIZER", ByteArrayDeserializer)
	mustAddProp("JSON_SCHEMA_SERIALIZER", JsonSchemaSerializer)
	mustAddProp("JSON_SCHEMA_DESERIALIZER", JsonSchemaDeserializer)
	mustAddProp("AVRO_SERIALIZER", AvroSerializer)
	mustAddProp("AVRO_DESERIALIZER", AvroDeserializer)
	mustAddProp("PROTOBUF_SERIALIZER", ProtobufSerializer)
	mustAddProp("PROTOBUF_DESERIALIZER", ProtobufDeserializer)

	// TopicNameStrategy types
	mustAddProp("TOPIC_NAME_STRATEGY", TopicNameStrategy)
	mustAddProp("RECORD_NAME_STRATEGY", RecordNameStrategy)
	mustAddProp("TOPIC_RECORD_NAME_STRATEGY", TopicRecordNameStrategy)
}
