package kafka

import (
	"github.com/dop251/goja"
	"github.com/sirupsen/logrus"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"
)

// init registers the xk6-kafka module as 'k6/x/kafka'
func init() {
	modules.Register("k6/x/kafka", New())
}

type (
	Kafka struct {
		vu                   modules.VU
		metrics              kafkaMetrics
		logger               *logrus.Logger
		serializerRegistry   *Serde[Serializer]
		deserializerRegistry *Serde[Deserializer]
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
	m, err := registerMetrics(vu)
	if err != nil {
		common.Throw(vu.Runtime(), err)
	}

	// This causes the struct fields to be exported to the native (camelCases) JS code.
	vu.Runtime().SetFieldNameMapper(goja.TagFieldNameMapper("json", true))

	return &KafkaModule{Kafka: &Kafka{
		vu:                   vu,
		metrics:              m,
		logger:               logrus.New(),
		serializerRegistry:   NewSerializersRegistry(),
		deserializerRegistry: NewDeserializersRegistry()},
	}
}

// Exports returns the exports of the Kafka module, which are the functions
// that can be called from the JS code.
func (c *KafkaModule) Exports() modules.Exports {
	return modules.Exports{Default: c.Kafka}
}
