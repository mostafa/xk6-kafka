package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	avroConfig Configuration = Configuration{
		Producer: ProducerConfiguration{
			ValueSerializer: AvroSerializer,
			KeySerializer:   AvroSerializer,
		},
		Consumer: ConsumerConfiguration{
			ValueDeserializer: AvroDeserializer,
			KeyDeserializer:   AvroDeserializer,
		},
	}
	avroSchema string = `{"type":"record","name":"Schema","namespace":"io.confluent.kafka.avro","fields":[{"name":"field","type":"string"}]}`
)

func TestSerializeDeserializeAvro(t *testing.T) {
	// Test with a schema registry, which fails and manually (de)serializes the data
	for _, element := range []Element{Key, Value} {
		// Serialize the key or value
		serialized, err := SerializeAvro(avroConfig, "topic", `{"field":"value"}`, element, avroSchema, 0)
		assert.NotNil(t, serialized)
		// 4 bytes for magic byte, 1 byte for schema ID, and the rest is the data
		assert.GreaterOrEqual(t, len(serialized), 10)

		// Deserialize the key or value (removes the magic bytes)
		deserialized, err := DeserializeAvro(avroConfig, "", serialized, element, avroSchema, 0)
		assert.Nil(t, err)
		assert.Equal(t, map[string]interface{}{"field": "value"}, deserialized)
	}
}

func TestSerializeDeserializeAvroWithoutSchemaRegistry(t *testing.T) {
	// Test without a schema registry, which manually (de)serializes the data
	for _, element := range []Element{Key, Value} {
		// Serialize the key or value
		serialized, err := SerializeAvro(avroConfig, "topic", `{"field":"value"}`, element, avroSchema, 0)
		assert.NotNil(t, serialized)
		// 4 bytes for magic byte, 1 byte for schema ID, and the rest is the data
		assert.GreaterOrEqual(t, len(serialized), 10)

		// Deserialize the deserialized (removes the magic bytes)
		deserialized, err := DeserializeAvro(avroConfig, "", serialized, element, avroSchema, 0)
		assert.Nil(t, err)
		assert.Equal(t, map[string]interface{}{"field": "value"}, deserialized)
	}
}

func TestSerializeDeserializeAvroFailsOnSchemaError(t *testing.T) {
	jsonSchema = `{}`

	for _, element := range []Element{Key, Value} {
		// Serialize the key or value
		serialized, err := SerializeAvro(avroConfig, "topic", `{"field":"value"}`, element, jsonSchema, 0)
		assert.Nil(t, serialized)
		assert.Error(t, err.Unwrap())
		assert.Equal(t, "Failed to create codec for encoding Avro", err.Message)
		assert.Equal(t, failedCreateAvroCodec, err.Code)

		// Deserialize the key or value
		deserialized, err := DeserializeAvro(avroConfig, "topic", []byte{1, 2, 3, 4, 5, 6}, element, jsonSchema, 0)
		assert.Nil(t, deserialized)
		assert.Error(t, err.Unwrap())
		assert.Equal(t, "Failed to create codec for decoding Avro", err.Message)
		assert.Equal(t, failedCreateAvroCodec, err.Code)
	}
}

func TestSerializeDeserializeAvroFailsOnWireFormatError(t *testing.T) {
	schema := `{}`

	for _, element := range []Element{Key, Value} {
		// Deserialize an empty key or value
		deserialized, err := DeserializeAvro(avroConfig, "topic", []byte{}, element, schema, 0)
		assert.Nil(t, deserialized)
		assert.Error(t, err.Unwrap())
		assert.Equal(t, "Failed to remove wire format from the binary data", err.Message)
		assert.Equal(t, failedDecodeFromWireFormat, err.Code)

		// Deserialize a broken key or value
		// Proper wire-formatted message has 5 bytes (the wire format) plus data
		deserialized, err = DeserializeAvro(avroConfig, "topic", []byte{1, 2, 3, 4}, element, schema, 0)
		assert.Nil(t, deserialized)
		assert.Error(t, err.Unwrap())
		assert.Equal(t, "Failed to remove wire format from the binary data", err.Message)
		assert.Equal(t, failedDecodeFromWireFormat, err.Code)
	}
}

func TestSerializeDeserializeAvroFailsOnEncodeDecodeError(t *testing.T) {
	data := `{"nonExistingField":"value"}`

	for _, element := range []Element{Key, Value} {
		serialized, err := SerializeAvro(avroConfig, "topic", data, element, avroSchema, 0)
		assert.Nil(t, serialized)
		assert.Error(t, err.Unwrap())
		assert.Equal(t, "Failed to encode data into Avro", err.Message)
		assert.Equal(t, failedEncodeToAvro, err.Code)

		deserialized, err := DeserializeAvro(avroConfig, "topic", []byte{1, 2, 3, 4, 5, 6}, element, avroSchema, 0)
		assert.Nil(t, deserialized)
		assert.Error(t, err.Unwrap())
		assert.Equal(t, "Failed to decode data from Avro", err.Message)
		assert.Equal(t, failedDecodeAvroFromBinary, err.Code)
	}
}
