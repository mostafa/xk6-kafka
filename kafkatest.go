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

func GetTestModuleInstance(t testing.TB) (*goja.Runtime, *KafkaModule) {
	rootGroup, err := lib.NewGroup("", nil)
	require.NoError(t, err)

	rt := goja.New()
	rt.SetFieldNameMapper(common.FieldNameMapper{})

	samples := make(chan metrics.SampleContainer, 1000)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	state := &lib.State{
		Group: rootGroup,
		Options: lib.Options{
			UserAgent: null.StringFrom("TestUserAgent"),
			Paused:    null.BoolFrom(false),
		},
		Samples:        samples,
		BuiltinMetrics: metrics.RegisterBuiltinMetrics(metrics.NewRegistry()),
	}

	root := New()
	mockVU := &modulestest.VU{
		RuntimeField: rt,
		InitEnvField: &common.InitEnvironment{
			Registry: metrics.NewRegistry(),
		},
		CtxField:   ctx,
		StateField: state,
	}
	mi, ok := root.NewModuleInstance(mockVU).(*KafkaModule)
	require.True(t, ok)

	require.NoError(t, rt.Set("kafka", mi.Exports().Default))

	return rt, mi
}
