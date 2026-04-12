package kafka

import (
	"testing"

	"github.com/grafana/sobek"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func requireGoErrorMessage(t *testing.T, fn func(), expected string) {
	t.Helper()

	defer func() {
		t.Helper()

		err := recover()
		require.NotNil(t, err)

		errObj, ok := err.(*sobek.Object)
		require.True(t, ok)
		assert.Equal(t, GoErrorPrefix+expected, errObj.ToString().String())
	}()

	fn()
}

func TestWriterClassRejectsNonObjectConfig(t *testing.T) {
	test := getTestModuleInstance(t)

	requireGoErrorMessage(t, func() {
		test.module.writerClass(sobek.ConstructorCall{
			Arguments: []sobek.Value{test.rt.ToValue("invalid")},
		})
	}, "Invalid writer config, OriginalError: expected object, got string")
}

func TestReaderClassRejectsNonObjectConfig(t *testing.T) {
	test := getTestModuleInstance(t)

	requireGoErrorMessage(t, func() {
		test.module.readerClass(sobek.ConstructorCall{
			Arguments: []sobek.Value{test.rt.ToValue("invalid")},
		})
	}, "Invalid reader config, OriginalError: expected object, got string")
}

func TestConnectionClassRejectsMissingAddress(t *testing.T) {
	test := getTestModuleInstance(t)

	requireGoErrorMessage(t, func() {
		test.module.connectionClass(sobek.ConstructorCall{
			Arguments: []sobek.Value{test.rt.ToValue(map[string]any{})},
		})
	}, "Invalid connection config, OriginalError: address must not be empty")
}

func TestSchemaRegistryClientClassRejectsMissingURL(t *testing.T) {
	test := getTestModuleInstance(t)

	requireGoErrorMessage(t, func() {
		test.module.schemaRegistryClientClass(sobek.ConstructorCall{
			Arguments: []sobek.Value{test.rt.ToValue(map[string]any{})},
		})
	}, "Invalid schema registry config, OriginalError: url must not be empty")
}

func TestSerializeRejectsMissingMetadata(t *testing.T) {
	test := getTestModuleInstance(t)

	requireGoErrorMessage(t, func() {
		test.module.serialize(nil)
	}, "serialize metadata is required")
}

func TestEncodeWireFormatRejectsOutOfRangeSchemaID(t *testing.T) {
	test := getTestModuleInstance(t)

	requireGoErrorMessage(t, func() {
		test.module.encodeWireFormat([]byte("value"), -1)
	}, "Invalid schema id -1: must be within uint32 range")
}

func TestProduceRejectsBalancerFuncWithoutKeys(t *testing.T) {
	test := getTestModuleInstance(t)
	test.moveToVUCode()

	writer := &kafkago.Writer{
		Balancer: kafkago.BalancerFunc(func(_ kafkago.Message, _ ...int) int { return 0 }),
	}

	requireGoErrorMessage(t, func() {
		test.module.produce(writer, &ProduceConfig{
			Messages: []Message{{Value: []byte("value")}},
		})
	}, "Balancer function requires message keys")
}
