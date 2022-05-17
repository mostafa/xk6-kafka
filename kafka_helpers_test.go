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

// struct to keep all the things test need in one place
type kafkaTest struct {
	rt     *goja.Runtime
	module *KafkaModule
	vu     *modulestest.VU
}

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
		rt:     rt,
		module: mi,
		vu:     mockVU,
	}
}

func (k *kafkaTest) moveToVUCode() error {
	rootGroup, err := lib.NewGroup("", nil)
	if err != nil {
		return err
	}
	samples := make(chan metrics.SampleContainer, 1000)
	state := &lib.State{
		Group: rootGroup,
		Options: lib.Options{
			UserAgent: null.StringFrom("TestUserAgent"),
			Paused:    null.BoolFrom(false),
		},
		Samples:        samples,
		BuiltinMetrics: metrics.RegisterBuiltinMetrics(metrics.NewRegistry()),
	}
	k.vu.StateField = state
	k.vu.InitEnvField = nil
	return nil
}
