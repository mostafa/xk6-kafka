package kafka

import "go.k6.io/k6/js/modules"

func init() {
	modules.Register("k6/x/kafka", New())
}

type (
	Kafka struct {
		vu modules.VU
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

func New() *RootModule {
	return &RootModule{}
}

func (*RootModule) NewModuleInstance(vu modules.VU) modules.Instance {
	return &KafkaModule{Kafka: &Kafka{vu: vu}}
}

func (c *KafkaModule) Exports() modules.Exports {
	return modules.Exports{Default: c.Kafka}
}
