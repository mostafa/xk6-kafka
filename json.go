package kafka

import (
	"encoding/json"
)

type JSONSerde struct {
	Serdes
}

// Serialize serializes a JSON object as map to bytes.
func (*JSONSerde) Serialize(data interface{}, schema *Schema) ([]byte, *Xk6KafkaError) {
	var jsonObject []byte
	if data, ok := data.(map[string]interface{}); ok {
		if encodedData, err := json.Marshal(data); err == nil {
			jsonObject = encodedData
		} else {
			return nil, ErrInvalidDataType
		}
	} else {
		return nil, ErrInvalidDataType
	}

	if schema != nil {
		// Validate the JSON object against the schema only if the schema is
		// provided.
		jsonSchema := schema.JsonSchema()
		if jsonSchema != nil {
			if err := jsonSchema.Validate(data); err != nil {
				return nil, NewXk6KafkaError(failedValidateJSON,
					"Failed to validate JSON against schema",
					err)
			}
		} else {
			return nil, ErrInvalidSchema
		}
	}

	return jsonObject, nil
}

// Deserialize deserializes a map from bytes to be exported as object to JS.
func (*JSONSerde) Deserialize(data []byte, schema *Schema) (interface{}, *Xk6KafkaError) {
	var jsonObject interface{}
	if err := json.Unmarshal(data, &jsonObject); err != nil {
		return nil, NewXk6KafkaError(failedUnmarshalJSON,
			"Failed to unmarshal JSON data",
			err)
	}

	if schema != nil {
		// Validate the JSON object against the schema only if the schema is
		// provided.
		if err := schema.JsonSchema().Validate(jsonObject); err != nil {
			err := NewXk6KafkaError(failedDecodeJSONFromBinary,
				"Failed to decode data from JSON",
				err)
			return nil, err
		}
	}

	return jsonObject, nil
}
