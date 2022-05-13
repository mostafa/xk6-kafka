package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSerializeAvro(t *testing.T) {
	config := Configuration{
		Consumer: ConsumerConfiguration{
			ValueDeserializer: "io.confluent.kafka.serializers.KafkaAvroDeserializer",
			KeyDeserializer:   "io.confluent.kafka.serializers.KafkaAvroDeserializer",
		},
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
}
