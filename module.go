package kafka

import (
	"crypto/tls"
	"time"

	"github.com/grafana/sobek"
	"github.com/riferrei/srclient"
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

	// Initialize the start offsets map.
	StartOffsets = map[string]int64{
		lastOffset:  kafkago.LastOffset,  // The most recent offset available for a partition.
		firstOffset: kafkago.FirstOffset, // The least recent offset available for a partition.
	}

	// Register the module namespace (aka. JS import path).
	modules.Register("k6/x/kafka", New())
}

type (
	Kafka struct {
		vu          modules.VU
		metrics     kafkaMetrics
		exports     *sobek.Object
		schemaCache map[string]*Schema
	}
	RootModule struct{}
	Module     struct {
		*Kafka
	}
)

var (
	_ modules.Instance = &Module{}
	_ modules.Module   = &RootModule{}
)

// New creates a new instance of the root module.
func New() *RootModule {
	return &RootModule{}
}

// NewModuleInstance creates a new instance of the Kafka module.
func (*RootModule) NewModuleInstance(virtualUser modules.VU) modules.Instance {
	runtime := virtualUser.Runtime()

	metrics, err := registerMetrics(virtualUser)
	if err != nil {
		common.Throw(virtualUser.Runtime(), err)
	}

	// Create a new Kafka module.
	moduleInstance := &Module{
		Kafka: &Kafka{
			vu:          virtualUser,
			metrics:     metrics,
			exports:     runtime.NewObject(),
			schemaCache: make(map[string]*Schema),
		},
	}

	// Export constants to the JS code.
	moduleInstance.defineConstants()

	mustExport := func(name string, value interface{}) {
		if err := moduleInstance.exports.Set(name, value); err != nil {
			common.Throw(runtime, err)
		}
	}

	// Export the constructors and functions from the Kafka module to the JS code.
	// The Writer is a constructor and must be called with new, e.g. new Writer(...).
	mustExport("Writer", moduleInstance.writerClass)
	// The Reader is a constructor and must be called with new, e.g. new Reader(...).
	mustExport("Reader", moduleInstance.readerClass)
	// The Connection is a constructor and must be called with new, e.g. new Connection(...).
	mustExport("Connection", moduleInstance.connectionClass)
	// The SchemaRegistry is a constructor and must be called with new, e.g. new SchemaRegistry(...).
	mustExport("SchemaRegistry", moduleInstance.schemaRegistryClientClass)

	// The LoadJKS is a function and must be called without new, e.g. LoadJKS(...).
	mustExport("LoadJKS", moduleInstance.loadJKSFunction)

	return moduleInstance
}

// Exports returns the exports of the Kafka module, which are the functions
// that can be called from the JS code.
func (m *Module) Exports() modules.Exports {
	return modules.Exports{
		Default: m.Kafka.exports,
	}
}

// defineConstants defines the constants that can be used in the JS code.
// nolint: funlen
func (m *Module) defineConstants() {
	runtime := m.vu.Runtime()
	mustAddProp := func(name string, val interface{}) {
		err := m.exports.DefineDataProperty(
			name, runtime.ToValue(val), sobek.FLAG_FALSE, sobek.FLAG_FALSE, sobek.FLAG_TRUE,
		)
		if err != nil {
			common.Throw(runtime, err)
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
	mustAddProp("SASL_AWS_IAM", saslAwsIam)

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

	// Start offsets
	mustAddProp("FIRST_OFFSET", firstOffset)
	mustAddProp("LAST_OFFSET", lastOffset)

	// TopicNameStrategy types
	mustAddProp("TOPIC_NAME_STRATEGY", TopicNameStrategy)
	mustAddProp("RECORD_NAME_STRATEGY", RecordNameStrategy)
	mustAddProp("TOPIC_RECORD_NAME_STRATEGY", TopicRecordNameStrategy)

	// Element types
	mustAddProp("KEY", string(Key))
	mustAddProp("VALUE", string(Value))

	// Schema types
	mustAddProp("SCHEMA_TYPE_STRING", String)
	mustAddProp("SCHEMA_TYPE_BYTES", Bytes)
	mustAddProp("SCHEMA_TYPE_AVRO", srclient.Avro)
	mustAddProp("SCHEMA_TYPE_JSON", srclient.Json)
	mustAddProp("SCHEMA_TYPE_PROTOBUF", srclient.Protobuf)

	// Time constants
	mustAddProp("NANOSECOND", int64(time.Nanosecond))
	mustAddProp("MICROSECOND", int64(time.Microsecond))
	mustAddProp("MILLISECOND", int64(time.Millisecond))
	mustAddProp("SECOND", int64(time.Second))
	mustAddProp("MINUTE", int64(time.Minute))
	mustAddProp("HOUR", int64(time.Hour))
}
