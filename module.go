package kafka

import (
	"crypto/tls"

	"github.com/dop251/goja"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/compress"
	"github.com/sirupsen/logrus"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"
	"go.k6.io/k6/lib/netext"
)

// logger is globally used by the Kafka module.
var logger *logrus.Logger

// init registers the xk6-kafka module as 'k6/x/kafka'.
func init() {
	// Initialize the global logger.
	logger = logrus.New()

	// Initialize the TLS versions map.
	TLSVersions = map[string]uint16{
		netext.TLS_1_0: tls.VersionTLS10,
		netext.TLS_1_1: tls.VersionTLS11,
		netext.TLS_1_2: tls.VersionTLS12,
		netext.TLS_1_3: tls.VersionTLS13,
	}

	// Initialize the compression types map.
	CompressionCodecs = map[string]compress.Compression{
		codecGzip:   compress.Gzip,
		codecSnappy: compress.Snappy,
		codecLz4:    compress.Lz4,
		codecZstd:   compress.Zstd,
	}

	// Initialize the balancer types map.
	Balancers = map[string]kafkago.Balancer{
		balancerRoundRobin: &kafkago.RoundRobin{},
		balancerLeastBytes: &kafkago.LeastBytes{},
		balancerHash:       &kafkago.Hash{},
		balancerCrc32:      &kafkago.CRC32Balancer{},
		balancerMurmur2:    &kafkago.Murmur2Balancer{},
	}

	// Initialize the group balancer types map.
	GroupBalancers = map[string]kafkago.GroupBalancer{
		groupBalancerRange:        &kafkago.RangeGroupBalancer{},
		groupBalancerRoundRobin:   &kafkago.RoundRobinGroupBalancer{},
		groupBalancerRackAffinity: &kafkago.RackAffinityGroupBalancer{},
	}

	// Initialize the isolation levels map.
	IsolationLevels = map[string]kafkago.IsolationLevel{
		isolationLevelReadUncommitted: kafkago.ReadUncommitted,
		isolationLevelReadCommitted:   kafkago.ReadCommitted,
	}

	// Register the module namespace (aka. JS import path).
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

// New creates a new instance of the root module.
func New() *RootModule {
	return &RootModule{}
}

// NewModuleInstance creates a new instance of the Kafka module.
func (*RootModule) NewModuleInstance(vu modules.VU) modules.Instance {
	rt := vu.Runtime()

	m, err := registerMetrics(vu)
	if err != nil {
		common.Throw(vu.Runtime(), err)
	}

	// Create a new Kafka module.
	kafkaModuleInstance := &KafkaModule{
		Kafka: &Kafka{
			vu:                   vu,
			metrics:              m,
			serializerRegistry:   NewSerializersRegistry(),
			deserializerRegistry: NewDeserializersRegistry(),
			exports:              rt.NewObject(),
		},
	}

	// Export constants to the JS code.
	kafkaModuleInstance.defineConstants()

	mustExport := func(name string, value interface{}) {
		if err := kafkaModuleInstance.exports.Set(name, value); err != nil {
			common.Throw(rt, err)
		}
	}

	// Export the functions from the Kafka module to the JS code.
	// The Writer is a constructor and must be called with new, e.g. new Writer(...).
	mustExport("Writer", kafkaModuleInstance.XWriter)
	// The Reader is a constructor and must be called with new, e.g. new Reader(...).
	mustExport("Reader", kafkaModuleInstance.XReader)
	// The Connection is a constructor and must be called with new, e.g. new Connection(...).
	mustExport("Connection", kafkaModuleInstance.XConnection)

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

// defineConstants defines the constants that can be used in the JS code.
// nolint: funlen
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
	mustAddProp("NONE", none)
	mustAddProp("SASL_PLAIN", saslPlain)
	mustAddProp("SASL_SCRAM_SHA256", saslScramSha256)
	mustAddProp("SASL_SCRAM_SHA512", saslScramSha512)
	mustAddProp("SASL_SSL", saslSsl)

	// Compression codecs
	mustAddProp("CODEC_GZIP", codecGzip)
	mustAddProp("CODEC_SNAPPY", codecSnappy)
	mustAddProp("CODEC_LZ4", codecLz4)
	mustAddProp("CODEC_ZSTD", codecZstd)

	// Balancer types
	mustAddProp("BALANCER_ROUND_ROBIN", balancerRoundRobin)
	mustAddProp("BALANCER_LEAST_BYTES", balancerLeastBytes)
	mustAddProp("BALANCER_HASH", balancerHash)
	mustAddProp("BALANCER_CRC32", balancerCrc32)
	mustAddProp("BALANCER_MURMUR2", balancerMurmur2)

	// Group balancer types
	mustAddProp("GROUP_BALANCER_RANGE", groupBalancerRange)
	mustAddProp("GROUP_BALANCER_ROUND_ROBIN", groupBalancerRoundRobin)
	mustAddProp("GROUP_BALANCER_RACK_AFFINITY", groupBalancerRackAffinity)

	// Isolation levels
	mustAddProp("ISOLATION_LEVEL_READ_UNCOMMITTED", isolationLevelReadUncommitted)
	mustAddProp("ISOLATION_LEVEL_READ_COMMITTED", isolationLevelReadCommitted)

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
