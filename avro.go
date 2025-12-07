package kafka

import (
	"encoding/json"
	"fmt"

	"github.com/hamba/avro/v2"
)

type AvroSerde struct {
	Serdes
}

// convertPrimitiveType converts a primitive value to the correct Avro type.
// Handles float64->int32/int64 conversion and array->bytes conversion.
func convertPrimitiveType(data any, schema avro.Schema) (any, error) {
	switch schema.Type() {
	case avro.Bytes:
		// Convert array of numbers to []byte for bytes fields
		if arr, ok := data.([]any); ok {
			bytes := make([]byte, len(arr))
			for i, v := range arr {
				switch val := v.(type) {
				case float64:
					bytes[i] = byte(val)
				case int:
					bytes[i] = byte(val)
				case int32:
					bytes[i] = byte(val)
				case int64:
					bytes[i] = byte(val)
				default:
					return nil, fmt.Errorf("cannot convert %T to byte at index %d", v, i)
				}
			}
			return bytes, nil
		}
		if bytes, ok := data.([]byte); ok {
			return bytes, nil
		}
		return data, nil
	case avro.Int:
		if f, ok := data.(float64); ok {
			if f != float64(int32(f)) {
				return nil, fmt.Errorf("cannot convert float64 %f to int32: not an integer", f)
			}
			return int32(f), nil
		}
		return data, nil
	case avro.Long:
		if f, ok := data.(float64); ok {
			if f != float64(int64(f)) {
				return nil, fmt.Errorf("cannot convert float64 %f to int64: not an integer", f)
			}
			return int64(f), nil
		}
		return data, nil

	default:
		return data, nil
	}
}

// convertUnionField converts a union field value, wrapping named schemas appropriately.
func convertUnionField(fieldValue any, unionSchema *avro.UnionSchema) (any, error) {
	if fieldValue == nil {
		return nil, nil
	}

	types := unionSchema.Types()

	// Handle map values (could be wrapped union or record)
	if fieldValueMap, ok := fieldValue.(map[string]any); ok {
		// Check if it's already wrapped: {"typeName": value}
		if len(fieldValueMap) == 1 {
			for key, wrappedValue := range fieldValueMap {
				// Try to find matching named schema
				for _, unionType := range types {
					if unionType.Type() == avro.Null {
						continue
					}
					actualType := unionType
					if refSchema, ok := unionType.(*avro.RefSchema); ok {
						actualType = refSchema.Schema()
					}
					if namedSchema, ok := actualType.(avro.NamedSchema); ok && namedSchema.FullName() == key {
						// Already wrapped, convert nested value
						converted, err := convertFloat64ToIntForIntegerFields(wrappedValue, actualType)
						if err != nil {
							return nil, err
						}
						return map[string]any{key: converted}, nil
					}
				}
			}
		}

		// Not wrapped, try to match as record
		for _, unionType := range types {
			if unionType.Type() == avro.Null {
				continue
			}
			actualType := unionType
			if refSchema, ok := unionType.(*avro.RefSchema); ok {
				actualType = refSchema.Schema()
			}
			if actualType.Type() == avro.Record {
				converted, err := convertFloat64ToIntForIntegerFields(fieldValueMap, actualType)
				if err == nil {
					if namedSchema, ok := actualType.(avro.NamedSchema); ok {
						return map[string]any{namedSchema.FullName(): converted}, nil
					}
					return converted, nil
				}
			}
		}
	}

	// Handle non-map values (primitives, enums)
	for _, unionType := range types {
		if unionType.Type() == avro.Null {
			continue
		}
		actualType := unionType
		if refSchema, ok := unionType.(*avro.RefSchema); ok {
			actualType = refSchema.Schema()
		}

		// Named schemas (enums, fixed) need wrapping
		if namedSchema, ok := actualType.(avro.NamedSchema); ok {
			if actualType.Type() == avro.Enum {
				// Enums are strings, wrap directly
				return map[string]any{namedSchema.FullName(): fieldValue}, nil
			}
			// Other named types, try converting first
			converted, err := convertFloat64ToIntForIntegerFields(fieldValue, actualType)
			if err == nil {
				return map[string]any{namedSchema.FullName(): converted}, nil
			}
		} else {
			// Primitive types, convert and return directly
			converted, err := convertFloat64ToIntForIntegerFields(fieldValue, actualType)
			if err == nil {
				return converted, nil
			}
		}
	}

	// Couldn't match, return as-is
	return fieldValue, nil
}

// convertFloat64ToIntForIntegerFields converts float64 values to int32/int64 for int/long schema fields.
// This is necessary because JSON unmarshaling converts all numbers to float64,
// but Avro int fields require int32 values and long fields require int64 values.
func convertFloat64ToIntForIntegerFields(data any, schema avro.Schema) (any, error) {
	if schema == nil {
		return data, nil
	}

	// Handle schema references
	if refSchema, ok := schema.(*avro.RefSchema); ok {
		schema = refSchema.Schema()
	}

	switch schema.Type() {
	case avro.Bytes, avro.Int, avro.Long:
		return convertPrimitiveType(data, schema)
	case avro.Record:
		recordSchema, ok := schema.(*avro.RecordSchema)
		if !ok {
			return data, nil
		}

		dataMap, ok := data.(map[string]any)
		if !ok {
			return data, nil
		}

		convertedMap := make(map[string]any)
		for _, field := range recordSchema.Fields() {
			fieldName := field.Name()
			fieldValue, exists := dataMap[fieldName]
			if !exists {
				continue
			}

			// Handle union types specially
			fieldType := field.Type()
			if unionSchema, ok := fieldType.(*avro.UnionSchema); ok {
				convertedValue, err := convertUnionField(fieldValue, unionSchema)
				if err != nil {
					return nil, fmt.Errorf("field %s: %w", fieldName, err)
				}
				convertedMap[fieldName] = convertedValue
			} else {
				// Normal field type, convert normally
				convertedValue, err := convertFloat64ToIntForIntegerFields(fieldValue, fieldType)
				if err != nil {
					return nil, fmt.Errorf("field %s: %w", fieldName, err)
				}
				convertedMap[fieldName] = convertedValue
			}
		}

		// Copy any remaining fields that aren't in the schema
		for k, v := range dataMap {
			if _, exists := convertedMap[k]; !exists {
				convertedMap[k] = v
			}
		}

		return convertedMap, nil
	case avro.Array:
		arraySchema, ok := schema.(*avro.ArraySchema)
		if !ok {
			return data, nil
		}

		dataArray, ok := data.([]any)
		if !ok {
			return data, nil
		}

		convertedArray := make([]any, len(dataArray))
		for i, item := range dataArray {
			convertedItem, err := convertFloat64ToIntForIntegerFields(item, arraySchema.Items())
			if err != nil {
				return nil, fmt.Errorf("array index %d: %w", i, err)
			}
			convertedArray[i] = convertedItem
		}

		return convertedArray, nil
	case avro.Map:
		mapSchema, ok := schema.(*avro.MapSchema)
		if !ok {
			return data, nil
		}

		dataMap, ok := data.(map[string]any)
		if !ok {
			return data, nil
		}

		convertedMap := make(map[string]any)
		for k, v := range dataMap {
			convertedValue, err := convertFloat64ToIntForIntegerFields(v, mapSchema.Values())
			if err != nil {
				return nil, fmt.Errorf("map key %s: %w", k, err)
			}
			convertedMap[k] = convertedValue
		}

		return convertedMap, nil
	case avro.Union:
		fallthrough
	default:
		return data, nil
	}
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

	// Convert float64 to int32/int64 for int/long fields before marshaling
	convertedData, convertErr := convertFloat64ToIntForIntegerFields(jsonData, avroSchema)
	if convertErr != nil {
		return nil, NewXk6KafkaError(failedToEncode,
			fmt.Sprintf("Failed to convert float64 to int32/int64 for integer fields: %v", convertErr),
			convertErr)
	}

	// Marshal to binary using hamba/avro
	bytesData, originalErr := avro.Marshal(avroSchema, convertedData)
	if originalErr != nil {
		return nil, NewXk6KafkaError(failedToEncodeToBinary,
			"Failed to encode data into binary",
			originalErr)
	}

	return bytesData, nil
}

// unwrapUnionValues recursively unwraps union values that are wrapped in the
// {"typeName": value} format returned by hamba/avro for named types in unions.
func unwrapUnionValues(data any, schema avro.Schema) (any, error) {
	if data == nil {
		return nil, nil
	}

	switch schema.Type() {
	case avro.Record:
		recordSchema, ok := schema.(*avro.RecordSchema)
		if !ok {
			return data, nil
		}

		dataMap, ok := data.(map[string]any)
		if !ok {
			return data, nil
		}

		unwrappedMap := make(map[string]any)
		for _, field := range recordSchema.Fields() {
			fieldName := field.Name()
			fieldValue, exists := dataMap[fieldName]
			if !exists {
				continue
			}

			fieldType := field.Type()
			if unionSchema, ok := fieldType.(*avro.UnionSchema); ok {
				// Field is a union type - unwrap if needed
				unwrappedValue, err := unwrapUnionValue(fieldValue, unionSchema)
				if err != nil {
					return nil, fmt.Errorf("field %s: %w", fieldName, err)
				}
				unwrappedMap[fieldName] = unwrappedValue
			} else {
				// Recursively unwrap nested structures
				unwrappedValue, err := unwrapUnionValues(fieldValue, fieldType)
				if err != nil {
					return nil, fmt.Errorf("field %s: %w", fieldName, err)
				}
				unwrappedMap[fieldName] = unwrappedValue
			}
		}

		// Copy any remaining fields that aren't in the schema
		for k, v := range dataMap {
			if _, exists := unwrappedMap[k]; !exists {
				unwrappedMap[k] = v
			}
		}

		return unwrappedMap, nil
	case avro.Array:
		arraySchema, ok := schema.(*avro.ArraySchema)
		if !ok {
			return data, nil
		}

		dataArray, ok := data.([]any)
		if !ok {
			return data, nil
		}

		unwrappedArray := make([]any, len(dataArray))
		for i, item := range dataArray {
			unwrappedItem, err := unwrapUnionValues(item, arraySchema.Items())
			if err != nil {
				return nil, fmt.Errorf("array index %d: %w", i, err)
			}
			unwrappedArray[i] = unwrappedItem
		}

		return unwrappedArray, nil
	case avro.Map:
		mapSchema, ok := schema.(*avro.MapSchema)
		if !ok {
			return data, nil
		}

		dataMap, ok := data.(map[string]any)
		if !ok {
			return data, nil
		}

		unwrappedMap := make(map[string]any)
		for k, v := range dataMap {
			unwrappedValue, err := unwrapUnionValues(v, mapSchema.Values())
			if err != nil {
				return nil, fmt.Errorf("map key %s: %w", k, err)
			}
			unwrappedMap[k] = unwrappedValue
		}

		return unwrappedMap, nil
	default:
		return data, nil
	}
}

// unwrapUnionValue unwraps a single union value if it's wrapped in {"typeName": value} format.
func unwrapUnionValue(value any, unionSchema *avro.UnionSchema) (any, error) {
	if value == nil {
		return nil, nil
	}

	// Check if value is wrapped as {"typeName": value}
	if valueMap, ok := value.(map[string]any); ok && len(valueMap) == 1 {
		for key, wrappedValue := range valueMap {
			// Check if key matches any union type's full name
			for _, unionType := range unionSchema.Types() {
				if unionType.Type() == avro.Null {
					continue
				}
				actualType := unionType
				if refSchema, ok := unionType.(*avro.RefSchema); ok {
					actualType = refSchema.Schema()
				}

				if namedSchema, ok := actualType.(avro.NamedSchema); ok && namedSchema.FullName() == key {
					// Found matching type - unwrap and recursively process
					return unwrapUnionValues(wrappedValue, actualType)
				}
			}
		}
	}

	// Not wrapped - try to recursively unwrap nested structures
	// Find the first matching union type that can successfully unwrap the value
	for _, unionType := range unionSchema.Types() {
		if unionType.Type() == avro.Null {
			continue
		}
		actualType := unionType
		if refSchema, ok := unionType.(*avro.RefSchema); ok {
			actualType = refSchema.Schema()
		}

		if unwrapped, err := unwrapUnionValues(value, actualType); err == nil {
			return unwrapped, nil
		}
	}

	// If we can't determine the type, return as-is
	return value, nil
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

	// Unwrap union values that are wrapped in {"typeName": value} format
	unwrappedData, unwrapErr := unwrapUnionValues(decodedData, avroSchema)
	if unwrapErr != nil {
		// Return original data if unwrapping fails
		unwrappedData = decodedData
	}

	if data, ok := unwrappedData.(map[string]any); ok {
		return data, nil
	}
	return unwrappedData, nil
}
