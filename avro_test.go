package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSerializeAvro(t *testing.T) {
	config := Configuration{
		Producer: ProducerConfiguration{
			ValueSerializer: "io.confluent.kafka.serializers.KafkaAvroSerializer",
			KeySerializer:   "io.confluent.kafka.serializers.KafkaAvroSerializer",
		},
	}

	keySchema := `{"type":"record","name":"Key","namespace":"io.confluent.kafka.avro","fields":[{"name":"field","type":"string"}]}`
	keyData := `{"field":"value"}`

	serialized, err := SerializeAvro(config, "topic", keyData, Key, keySchema, 0)
	if err != nil {
		t.Errorf("SerializeAvro failed: %s", err)
		return
	}
	assert.NotNil(t, serialized)
	// 4 bytes for magic byte, 1 byte for schema ID, and the rest is the data
	assert.GreaterOrEqual(t, len(serialized), 10)

	valueSchema := `{"type":"record","name":"Value","namespace":"io.confluent.kafka.avro","fields":[{"name":"field","type":"string"}]}`
	valueData := `{"field":"value"}`

	serialized, err = SerializeAvro(config, "topic", valueData, Value, valueSchema, 0)
	if err != nil {
		t.Errorf("SerializeAvro failed: %s", err)
		return
	}
	assert.NotNil(t, serialized)
	// 4 bytes for magic byte, 1 byte for schema ID, and the rest is the data
	assert.GreaterOrEqual(t, len(serialized), 10)
}

func TestSerializeAvroFailsOnSchemaError(t *testing.T) {
	config := Configuration{
		Producer: ProducerConfiguration{
			ValueSerializer: "io.confluent.kafka.serializers.KafkaAvroSerializer",
			KeySerializer:   "io.confluent.kafka.serializers.KafkaAvroSerializer",
		},
	}

	schema := `{}`
	data := `{"field":"value"}`

	for _, element := range []Element{Key, Value} {
		serialized, err := SerializeAvro(config, "topic", data, element, schema, 0)
		assert.Nil(t, serialized)
		assert.Error(t, err.Unwrap())
		assert.Equal(t, "Failed to create codec for encoding Avro", err.Message)
		assert.Equal(t, failedCreateAvroCodec, err.Code)
	}
}

func TestSerializeAvroFailsOnEncodeError(t *testing.T) {
	config := Configuration{
		Producer: ProducerConfiguration{
			ValueSerializer: "io.confluent.kafka.serializers.KafkaAvroSerializer",
			KeySerializer:   "io.confluent.kafka.serializers.KafkaAvroSerializer",
		},
	}

	schema := `{"type":"record","name":"Value","namespace":"io.confluent.kafka.avro","fields":[{"name":"field","type":"string"}]}`
	data := `{"nonExistingField":"value"}`

	for _, element := range []Element{Key, Value} {
		serialized, err := SerializeAvro(config, "topic", data, element, schema, 0)
		assert.Nil(t, serialized)
		assert.Error(t, err.Unwrap())
		assert.Equal(t, "Failed to encode data into Avro", err.Message)
		assert.Equal(t, failedEncodeToAvro, err.Code)
	}
}
