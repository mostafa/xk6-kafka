package kafka

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.k6.io/k6/js/modulestest"
	"go.k6.io/k6/lib"
	"go.k6.io/k6/metrics"
	"gopkg.in/guregu/null.v3"
)

// struct to keep all the things test need in one place.
type kafkaTest struct {
	rt      *modulestest.Runtime
	module  *Module
	samples chan metrics.SampleContainer
}

// GetTestModuleInstance returns a new instance of the Kafka module for testing.
// nolint: golint,revive
func GetTestModuleInstance(tb testing.TB) *kafkaTest {
	tb.Helper()
	// runtime := goja.New()
	// runtime.SetFieldNameMapper(common.FieldNameMapper{})

	// ctx, cancel := context.WithCancel(context.Background())
	// tb.Cleanup(cancel)

	runtime := modulestest.NewRuntime(tb)

	root := New()
	// mockVU := &modulestest.VU{
	// 	RuntimeField: runtime,
	// 	InitEnvField: &common.InitEnvironment{
	// 		Registry: metrics.NewRegistry(),
	// 	},
	// 	CtxField: ctx,
	// }
	moduleInstance, ok := root.NewModuleInstance(runtime.VU).(*Module)
	require.True(tb, ok)

	require.NoError(tb, runtime.VU.RuntimeField.Set("kafka", moduleInstance.Exports().Default))

	rootGroup, err := lib.NewGroup("", nil)
	if err != nil {
		return nil
	}
	samples := make(chan metrics.SampleContainer, 1000)
	// Save it, so we can reuse it in other tests
	state := &lib.State{
		Group: rootGroup,
		Options: lib.Options{
			UserAgent: null.StringFrom("TestUserAgent"),
			Paused:    null.BoolFrom(false),
		},
		Samples:        samples,
		BuiltinMetrics: metrics.RegisterBuiltinMetrics(metrics.NewRegistry()),
	}
	runtime.VU.StateField = state

	return &kafkaTest{
		rt:      runtime,
		module:  moduleInstance,
		samples: samples,
	}
}

// moveToVUCode moves to the VU code from the init code (to test certain functions).
// func (k *kafkaTest) moveToVUCode() error {

// 	k.rt.VU.StateField = state
// 	k.rt.VU.InitEnvField = nil
// 	return nil
// }

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
