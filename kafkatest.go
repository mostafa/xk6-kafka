package kafka

import (
	"context"
	"testing"

	"github.com/dop251/goja"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modulestest"
	"go.k6.io/k6/metrics"
)

func GetTestModuleInstance(t testing.TB) (*goja.Runtime, *KafkaModule) {
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
		CtxField:   ctx,
		StateField: nil,
	}
	mi, ok := root.NewModuleInstance(mockVU).(*KafkaModule)
	require.True(t, ok)

	require.NoError(t, rt.Set("kafka", mi.Exports().Default))

	return rt, mi
}
