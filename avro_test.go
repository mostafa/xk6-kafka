package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSerializeAvro(t *testing.T) {
	avroSerde := &AvroSerde{}
	expected := []byte{0x0a, 0x76, 0x61, 0x6c, 0x75, 0x65}
	schema := &Schema{
		ID: 2,
		Schema: `{
			"type":"record",
			"name":"Schema",
			"namespace":"io.confluent.kafka.avro",
			"fields":[{"name":"key","type":"string"}]}`,
		Version: 1,
		Subject: "avro-schema",
	}
	actual, err := avroSerde.Serialize(map[string]interface{}{"key": "value"}, schema)
	assert.Nil(t, err)
	assert.Equal(t, expected, actual)
}

func TestSerializeAvroFailsOnInvalidDataType(t *testing.T) {
	avroSerde := &AvroSerde{}
	schema := &Schema{
		ID: 2,
		Schema: `{
			"type":"record",
			"name":"Schema",
			"namespace":"io.confluent.kafka.avro",
			"fields":[{"name":"key","type":"string"}]}`,
		Version: 1,
		Subject: "avro-schema",
	}
	actual, err := avroSerde.Serialize(`{"key":"value"}`, schema)
	assert.Nil(t, actual)
	assert.NotNil(t, err)
	assert.Equal(t, ErrInvalidDataType, err)
}

func TestSerializeAvroFailsOnInvalidFormat(t *testing.T) {
	avroSerde := &AvroSerde{}
	schema := &Schema{
		ID: 2,
		Schema: `{
			"type":"record",
			"name":"Schema",
			"namespace":"io.confluent.kafka.avro",
			"fields":[{"name":"key","type":"string"}]}`,
		Version: 1,
		Subject: "avro-schema",
	}
	actual, err := avroSerde.Serialize(map[string]interface{}{"value": "key"}, schema)
	assert.Nil(t, actual)
	assert.NotNil(t, err)
	assert.Equal(t, "Failed to encode data", err.Message)
	assert.Equal(t, failedToEncode, err.Code)
}

func TestDeserialize(t *testing.T) {
	avroSerde := &AvroSerde{}
	schema := &Schema{
		ID: 2,
		Schema: `{
			"type":"record",
			"name":"Schema",
			"namespace":"io.confluent.kafka.avro",
			"fields":[{"name":"key","type":"string"}]}`,
		Version: 1,
		Subject: "avro-schema",
	}
	expected := map[string]interface{}{"key": "value"}
	actual, err := avroSerde.Deserialize([]byte{0x0a, 0x76, 0x61, 0x6c, 0x75, 0x65}, schema)
	assert.Nil(t, err)
	assert.Equal(t, expected, actual)
}

func TestDeserializeFailsOnDecodingBinary(t *testing.T) {
	avroSerde := &AvroSerde{}
	schema := &Schema{
		ID: 2,
		Schema: `{
			"type":"record",
			"name":"Schema",
			"namespace":"io.confluent.kafka.avro",
			"fields":[{"name":"key","type":"string"}]}`,
		Version: 1,
		Subject: "avro-schema",
	}
	actual, err := avroSerde.Deserialize([]byte{0x76, 0x61, 0x6c, 0x75, 0x65}, schema)
	assert.Nil(t, actual)
	assert.NotNil(t, err)
	assert.Equal(t, "Failed to decode data", err.Message)
	assert.Equal(t, failedToDecodeFromBinary, err.Code)
}
