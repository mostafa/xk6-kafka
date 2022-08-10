package kafka

type AvroSerde struct {
	Serdes
}

// Serialize serializes a JSON object into Avro binary.
func (*AvroSerde) Serialize(data interface{}, schema *Schema) ([]byte, *Xk6KafkaError) {
	jsonBytes, err := toJSONBytes(data)
	if err != nil {
		return nil, err
	}

	encodedData, _, originalErr := schema.Codec().NativeFromTextual(jsonBytes)
	if originalErr != nil {
		return nil, NewXk6KafkaError(failedToEncode, "Failed to encode data", originalErr)
	}

	bytesData, originalErr := schema.Codec().BinaryFromNative(nil, encodedData)
	if originalErr != nil {
		return nil, NewXk6KafkaError(failedToEncodeToBinary,
			"Failed to encode data into binary",
			originalErr)
	}

	return bytesData, nil
}

// Deserialize deserializes a Avro binary into a JSON object.
func (*AvroSerde) Deserialize(data []byte, schema *Schema) (interface{}, *Xk6KafkaError) {
	decodedData, _, err := schema.Codec().NativeFromBinary(data)
	if err != nil {
		return nil, NewXk6KafkaError(
			failedToDecodeFromBinary, "Failed to decode data", err)
	}

	if data, ok := decodedData.(map[string]interface{}); ok {
		return data, nil
	} else {
		return nil, ErrInvalidDataType
	}
}
