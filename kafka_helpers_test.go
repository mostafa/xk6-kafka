package kafka

import (
	"context"
	"testing"

	"github.com/dop251/goja"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modulestest"
	"go.k6.io/k6/lib"
	"go.k6.io/k6/metrics"
	"gopkg.in/guregu/null.v3"
)

// struct to keep all the things test need in one place.
type kafkaTest struct {
	rt            *goja.Runtime
	module        *KafkaModule
	vu            *modulestest.VU
	samples       chan metrics.SampleContainer
	cancelContext context.CancelFunc
}

// GetTestModuleInstance returns a new instance of the Kafka module for testing.
func GetTestModuleInstance(t testing.TB) *kafkaTest {
	rt := goja.New()
	rt.SetFieldNameMapper(common.FieldNameMapper{})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	root := New()
	mockVU := &modulestest.VU{
		RuntimeField: rt,
		InitEnvField: &common.InitEnvironment{
			Registry: metrics.NewRegistry(),
		},
		CtxField: ctx,
	}
	mi, ok := root.NewModuleInstance(mockVU).(*KafkaModule)
	require.True(t, ok)

	require.NoError(t, rt.Set("kafka", mi.Exports().Default))

	return &kafkaTest{
		rt:            rt,
		module:        mi,
		vu:            mockVU,
		cancelContext: cancel,
	}
}

// moveToVUCode moves to the VU code from the init code (to test certain functions).
func (k *kafkaTest) moveToVUCode() error {
	rootGroup, err := lib.NewGroup("", nil)
	if err != nil {
		return err
	}
	samples := make(chan metrics.SampleContainer, 1000)
	// Save it, so we can reuse it in other tests
	k.samples = samples
	state := &lib.State{
		Group: rootGroup,
		Options: lib.Options{
			UserAgent: null.StringFrom("TestUserAgent"),
			Paused:    null.BoolFrom(false),
		},
		Samples:        k.samples,
		BuiltinMetrics: metrics.RegisterBuiltinMetrics(metrics.NewRegistry()),
	}
	k.vu.StateField = state
	k.vu.InitEnvField = nil
	return nil
}

// GetCounterMetricsValues returns the samples of the collected metrics in the VU.
func (k *kafkaTest) GetCounterMetricsValues() map[string]float64 {
	metricsValues := make(map[string]float64)

	for _, sampleContainer := range metrics.GetBufferedSamples(k.samples) {
		for _, sample := range sampleContainer.GetSamples() {
			if sample.Metric.Type == metrics.Counter {
				metricsValues[sample.Metric.Name] = sample.Value
			}
		}
	}
	return metricsValues
}
