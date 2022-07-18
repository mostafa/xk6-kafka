package kafka

import (
	"net/url"
	"os"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/js/common"
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
	cwd, err := os.Getwd()
	require.NoError(tb, err)
	fs := afero.NewOsFs()
	initEnv := &common.InitEnvironment{
		Logger: logrus.New(),
		CWD:    &url.URL{Path: cwd},
		FileSystems: map[string]afero.Fs{
			"file": fs,
		},
		Registry: metrics.NewRegistry(),
	}
	runtime.InitContext(initEnv)

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

	samples := make(chan metrics.SampleContainer, 1000)

	return &kafkaTest{
		rt:      runtime,
		module:  moduleInstance,
		samples: samples,
	}
}

// moveToVUCode moves to the VU code from the init code (to test certain functions).
func (k *kafkaTest) moveToVUCode() error {
	rootGroup, err := lib.NewGroup("", nil)
	if err != nil {
		return err
	}
	// Save it, so we can reuse it in other tests
	state := &lib.State{
		Group: rootGroup,
		Options: lib.Options{
			UserAgent: null.StringFrom("TestUserAgent"),
			Paused:    null.BoolFrom(false),
		},
		Samples:        k.samples,
		BuiltinMetrics: metrics.RegisterBuiltinMetrics(metrics.NewRegistry()),
	}
	k.rt.MoveToVUContext(state)
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
