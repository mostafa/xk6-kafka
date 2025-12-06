package kafka

import (
	"encoding/json"

	"github.com/hamba/avro/v2"
)

type AvroSerde struct {
	Serdes
}

// Serialize serializes a JSON object into Avro binary.
func (*AvroSerde) Serialize(data any, schema *Schema) ([]byte, *Xk6KafkaError) {
	jsonBytes, err := toJSONBytes(data)
	if err != nil {
		return nil, err
	}

	avroSchema := schema.Codec()
	if avroSchema == nil {
		return nil, NewXk6KafkaError(failedToEncode, "Failed to parse Avro schema", nil)
	}

	// Parse JSON data into a map for marshaling
	var jsonData any
	jsonErr := json.Unmarshal(jsonBytes, &jsonData)
	if jsonErr != nil {
		return nil, NewXk6KafkaError(failedToEncode, "Failed to parse JSON data", jsonErr)
	}

	// Marshal to binary using hamba/avro
	bytesData, originalErr := avro.Marshal(avroSchema, jsonData)
	if originalErr != nil {
		return nil, NewXk6KafkaError(failedToEncodeToBinary,
			"Failed to encode data into binary",
			originalErr)
	}

	return bytesData, nil
}

// Deserialize deserializes a Avro binary into a JSON object.
func (*AvroSerde) Deserialize(data []byte, schema *Schema) (any, *Xk6KafkaError) {
	avroSchema := schema.Codec()
	if avroSchema == nil {
		return nil, NewXk6KafkaError(failedToDecodeFromBinary, "Failed to parse Avro schema", nil)
	}

	var decodedData any
	err := avro.Unmarshal(avroSchema, data, &decodedData)
	if err != nil {
		return nil, NewXk6KafkaError(
			failedToDecodeFromBinary, "Failed to decode data", err)
	}

	if data, ok := decodedData.(map[string]any); ok {
		return data, nil
	}
	return decodedData, nil
}
