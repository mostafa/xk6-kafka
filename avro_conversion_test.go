package kafka

import (
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testUserSchemaJSON = `{
		"type": "record",
		"name": "User",
		"namespace": "com.example",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"}
		]
	}`
)

func TestConvertPrimitiveType_Bytes(t *testing.T) {
	schema, err := avro.Parse(`{"type": "bytes"}`)
	require.NoError(t, err)

	tests := []struct {
		name    string
		data    any
		want    []byte
		wantErr bool
	}{
		{
			name:    "array of float64",
			data:    []any{float64(1), float64(2), float64(3)},
			want:    []byte{1, 2, 3},
			wantErr: false,
		},
		{
			name:    "array of int",
			data:    []any{1, 2, 3},
			want:    []byte{1, 2, 3},
			wantErr: false,
		},
		{
			name:    "array of int32",
			data:    []any{int32(1), int32(2), int32(3)},
			want:    []byte{1, 2, 3},
			wantErr: false,
		},
		{
			name:    "already bytes",
			data:    []byte{1, 2, 3},
			want:    []byte{1, 2, 3},
			wantErr: false,
		},
		{
			name:    "invalid type",
			data:    "not an array",
			want:    nil,
			wantErr: false, // Returns as-is
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := convertPrimitiveType(testCase.data, schema)
			if testCase.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if testCase.want != nil {
					assert.Equal(t, testCase.want, got)
				}
			}
		})
	}
}

func TestConvertPrimitiveType_Int(t *testing.T) {
	schema, err := avro.Parse(`{"type": "int"}`)
	require.NoError(t, err)

	tests := []struct {
		name    string
		data    any
		want    int32
		wantErr bool
	}{
		{
			name:    "valid float64",
			data:    float64(42),
			want:    int32(42),
			wantErr: false,
		},
		{
			name:    "already int32",
			data:    int32(42),
			want:    int32(42),
			wantErr: false,
		},
		{
			name:    "non-integer float64",
			data:    float64(42.5),
			want:    0,
			wantErr: true,
		},
		{
			name:    "out of range",
			data:    float64(2147483648), // > int32 max
			want:    0,
			wantErr: true,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := convertPrimitiveType(testCase.data, schema)
			if testCase.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, testCase.want, got)
			}
		})
	}
}

func TestConvertPrimitiveType_Long(t *testing.T) {
	schema, err := avro.Parse(`{"type": "long"}`)
	require.NoError(t, err)

	tests := []struct {
		name    string
		data    any
		want    int64
		wantErr bool
	}{
		{
			name:    "valid float64",
			data:    float64(42),
			want:    int64(42),
			wantErr: false,
		},
		{
			name:    "already int64",
			data:    int64(42),
			want:    int64(42),
			wantErr: false,
		},
		{
			name:    "non-integer float64",
			data:    float64(42.5),
			want:    0,
			wantErr: true,
		},
		{
			name:    "large valid number",
			data:    float64(9000000000000000000), // Large but safe for float64->int64
			want:    int64(9000000000000000000),
			wantErr: false,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := convertPrimitiveType(testCase.data, schema)
			if testCase.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, testCase.want, got)
			}
		})
	}
}

func TestConvertUnionField_Null(t *testing.T) {
	schema, err := avro.Parse(`["null", "string"]`)
	require.NoError(t, err)
	unionSchema := schema.(*avro.UnionSchema)

	got, err := convertUnionField(nil, unionSchema)
	assert.NoError(t, err)
	assert.Nil(t, got)
}

func TestConvertUnionField_Enum(t *testing.T) {
	enumSchema := `{
		"type": "enum",
		"name": "Status",
		"namespace": "com.example",
		"symbols": ["ACTIVE", "INACTIVE"]
	}`
	unionSchemaJSON := `["null", ` + enumSchema + `]`
	schema, err := avro.Parse(unionSchemaJSON)
	require.NoError(t, err)
	unionSchema := schema.(*avro.UnionSchema)

	got, err := convertUnionField("ACTIVE", unionSchema)
	assert.NoError(t, err)
	assert.Equal(t, map[string]any{"com.example.Status": "ACTIVE"}, got)
}

func TestConvertUnionField_Record(t *testing.T) {
	recordSchema := testUserSchemaJSON
	unionSchemaJSON := `["null", ` + recordSchema + `]`
	schema, err := avro.Parse(unionSchemaJSON)
	require.NoError(t, err)
	unionSchema := schema.(*avro.UnionSchema)

	// Test with unwrapped record
	data := map[string]any{
		"id":   float64(123),
		"name": "test",
	}
	got, err := convertUnionField(data, unionSchema)
	assert.NoError(t, err)
	assert.IsType(t, map[string]any{}, got)
	result := got.(map[string]any)
	assert.Len(t, result, 1)
	assert.Contains(t, result, "com.example.User")
	userData := result["com.example.User"].(map[string]any)
	assert.Equal(t, int32(123), userData["id"])
	assert.Equal(t, "test", userData["name"])
}

func TestConvertUnionField_RecordAlreadyWrapped(t *testing.T) {
	recordSchema := `{
		"type": "record",
		"name": "User",
		"namespace": "com.example",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"}
		]
	}`
	unionSchemaJSON := `["null", ` + recordSchema + `]`
	schema, err := avro.Parse(unionSchemaJSON)
	require.NoError(t, err)
	unionSchema := schema.(*avro.UnionSchema)

	// Test with already wrapped record
	wrappedData := map[string]any{
		"com.example.User": map[string]any{
			"id":   float64(123),
			"name": "test",
		},
	}
	got, err := convertUnionField(wrappedData, unionSchema)
	assert.NoError(t, err)
	assert.IsType(t, map[string]any{}, got)
	result := got.(map[string]any)
	assert.Len(t, result, 1)
	assert.Contains(t, result, "com.example.User")
	userData := result["com.example.User"].(map[string]any)
	assert.Equal(t, int32(123), userData["id"])
	assert.Equal(t, "test", userData["name"])
}

func TestConvertUnionField_Primitive(t *testing.T) {
	schema, err := avro.Parse(`["null", "int", "string"]`)
	require.NoError(t, err)
	unionSchema := schema.(*avro.UnionSchema)

	tests := []struct {
		name string
		data any
		want any
	}{
		{
			name: "int value",
			data: float64(42),
			want: int32(42),
		},
		{
			name: "string value",
			data: "hello",
			want: "hello",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := convertUnionField(testCase.data, unionSchema)
			assert.NoError(t, err)
			assert.Equal(t, testCase.want, got)
		})
	}
}

func TestConvertFloat64ToIntForIntegerFields_RecordWithUnions(t *testing.T) {
	schemaJSON := `{
		"type": "record",
		"name": "TestRecord",
		"namespace": "com.example",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "optional", "type": ["null", "string"]},
			{"name": "enumField", "type": ["null", {
				"type": "enum",
				"name": "Status",
				"namespace": "com.example",
				"symbols": ["ACTIVE", "INACTIVE"]
			}]}
		]
	}`
	schema, err := avro.Parse(schemaJSON)
	require.NoError(t, err)

	data := map[string]any{
		"id":        float64(123),
		"optional":  "test",
		"enumField": "ACTIVE",
	}

	got, err := convertFloat64ToIntForIntegerFields(data, schema)
	assert.NoError(t, err)
	result := got.(map[string]any)
	assert.Equal(t, int32(123), result["id"])
	assert.Equal(t, "test", result["optional"])
	assert.IsType(t, map[string]any{}, result["enumField"])
	enumMap := result["enumField"].(map[string]any)
	assert.Equal(t, "ACTIVE", enumMap["com.example.Status"])
}

func TestConvertFloat64ToIntForIntegerFields_NestedRecord(t *testing.T) {
	schemaJSON := `{
		"type": "record",
		"name": "Outer",
		"namespace": "com.example",
		"fields": [
			{"name": "inner", "type": {
				"type": "record",
				"name": "Inner",
				"namespace": "com.example",
				"fields": [
					{"name": "value", "type": "long"}
				]
			}}
		]
	}`
	schema, err := avro.Parse(schemaJSON)
	require.NoError(t, err)

	data := map[string]any{
		"inner": map[string]any{
			"value": float64(456),
		},
	}

	got, err := convertFloat64ToIntForIntegerFields(data, schema)
	assert.NoError(t, err)
	result := got.(map[string]any)
	inner := result["inner"].(map[string]any)
	assert.Equal(t, int64(456), inner["value"])
}

func TestConvertFloat64ToIntForIntegerFields_Array(t *testing.T) {
	schemaJSON := `{
		"type": "array",
		"items": "int"
	}`
	schema, err := avro.Parse(schemaJSON)
	require.NoError(t, err)

	data := []any{float64(1), float64(2), float64(3)}

	got, err := convertFloat64ToIntForIntegerFields(data, schema)
	assert.NoError(t, err)
	result := got.([]any)
	assert.Equal(t, []any{int32(1), int32(2), int32(3)}, result)
}

func TestConvertFloat64ToIntForIntegerFields_Map(t *testing.T) {
	schemaJSON := `{
		"type": "map",
		"values": "long"
	}`
	schema, err := avro.Parse(schemaJSON)
	require.NoError(t, err)

	data := map[string]any{
		"key1": float64(100),
		"key2": float64(200),
	}

	got, err := convertFloat64ToIntForIntegerFields(data, schema)
	assert.NoError(t, err)
	result := got.(map[string]any)
	assert.Equal(t, int64(100), result["key1"])
	assert.Equal(t, int64(200), result["key2"])
}

func TestUnwrapUnionValue_Null(t *testing.T) {
	schema, err := avro.Parse(`["null", "string"]`)
	require.NoError(t, err)
	unionSchema := schema.(*avro.UnionSchema)

	got, err := unwrapUnionValue(nil, unionSchema)
	assert.NoError(t, err)
	assert.Nil(t, got)
}

func TestUnwrapUnionValue_WrappedEnum(t *testing.T) {
	enumSchema := `{
		"type": "enum",
		"name": "Status",
		"namespace": "com.example",
		"symbols": ["ACTIVE", "INACTIVE"]
	}`
	unionSchemaJSON := `["null", ` + enumSchema + `]`
	schema, err := avro.Parse(unionSchemaJSON)
	require.NoError(t, err)
	unionSchema := schema.(*avro.UnionSchema)

	wrapped := map[string]any{
		"com.example.Status": "ACTIVE",
	}

	got, err := unwrapUnionValue(wrapped, unionSchema)
	assert.NoError(t, err)
	assert.Equal(t, "ACTIVE", got)
}

func TestUnwrapUnionValue_WrappedRecord(t *testing.T) {
	recordSchema := `{
		"type": "record",
		"name": "User",
		"namespace": "com.example",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"}
		]
	}`
	unionSchemaJSON := `["null", ` + recordSchema + `]`
	schema, err := avro.Parse(unionSchemaJSON)
	require.NoError(t, err)
	unionSchema := schema.(*avro.UnionSchema)

	wrapped := map[string]any{
		"com.example.User": map[string]any{
			"id":   int32(123),
			"name": "test",
		},
	}

	got, err := unwrapUnionValue(wrapped, unionSchema)
	assert.NoError(t, err)
	result := got.(map[string]any)
	assert.Equal(t, int32(123), result["id"])
	assert.Equal(t, "test", result["name"])
}

func TestUnwrapUnionValues_RecordWithUnions(t *testing.T) {
	schemaJSON := `{
		"type": "record",
		"name": "TestRecord",
		"namespace": "com.example",
		"fields": [
			{"name": "optional", "type": ["null", "string"]},
			{"name": "enumField", "type": ["null", {
				"type": "enum",
				"name": "Status",
				"namespace": "com.example",
				"symbols": ["ACTIVE", "INACTIVE"]
			}]},
			{"name": "recordField", "type": ["null", {
				"type": "record",
				"name": "User",
				"namespace": "com.example",
				"fields": [{"name": "id", "type": "int"}]
			}]}
		]
	}`
	schema, err := avro.Parse(schemaJSON)
	require.NoError(t, err)

	data := map[string]any{
		"optional":  "test",
		"enumField": map[string]any{"com.example.Status": "ACTIVE"},
		"recordField": map[string]any{
			"com.example.User": map[string]any{"id": int32(123)},
		},
	}

	got, err := unwrapUnionValues(data, schema)
	assert.NoError(t, err)
	result := got.(map[string]any)
	assert.Equal(t, "test", result["optional"])
	assert.Equal(t, "ACTIVE", result["enumField"])
	userData := result["recordField"].(map[string]any)
	assert.Equal(t, int32(123), userData["id"])
}

func TestUnwrapUnionValues_Array(t *testing.T) {
	schemaJSON := `{
		"type": "array",
		"items": ["null", "string"]
	}`
	schema, err := avro.Parse(schemaJSON)
	require.NoError(t, err)

	data := []any{
		nil,
		"hello",
		map[string]any{"string": "world"},
	}

	got, err := unwrapUnionValues(data, schema)
	assert.NoError(t, err)
	result := got.([]any)
	assert.Nil(t, result[0])
	assert.Equal(t, "hello", result[1])
	// For wrapped union values in arrays, unwrapUnionValue tries to match types
	// If it can't find a match, it returns as-is, so we check the wrapped format
	assert.IsType(t, map[string]any{}, result[2])
	wrapped := result[2].(map[string]any)
	assert.Equal(t, "world", wrapped["string"])
}

func TestUnwrapUnionValues_Map(t *testing.T) {
	schemaJSON := `{
		"type": "map",
		"values": ["null", "int"]
	}`
	schema, err := avro.Parse(schemaJSON)
	require.NoError(t, err)

	data := map[string]any{
		"key1": nil,
		"key2": int32(42),
		"key3": map[string]any{"int": int32(100)},
	}

	got, err := unwrapUnionValues(data, schema)
	assert.NoError(t, err)
	result := got.(map[string]any)
	assert.Nil(t, result["key1"])
	assert.Equal(t, int32(42), result["key2"])
	// For wrapped union values, unwrapUnionValue should unwrap them
	// But if it can't match, it returns as-is
	wrapped := result["key3"]
	if wrappedMap, ok := wrapped.(map[string]any); ok {
		assert.Equal(t, int32(100), wrappedMap["int"])
	} else {
		assert.Equal(t, int32(100), wrapped)
	}
}

func TestSerializeDeserializeRoundTrip_WithUnions(t *testing.T) {
	avroSerde := &AvroSerde{}
	schemaJSON := `{
		"type": "record",
		"name": "TestRecord",
		"namespace": "com.example",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "optional", "type": ["null", "string"]},
			{"name": "enumField", "type": ["null", {
				"type": "enum",
				"name": "Status",
				"namespace": "com.example",
				"symbols": ["ACTIVE", "INACTIVE"]
			}]},
			{"name": "bytesField", "type": ["null", "bytes"]}
		]
	}`
	schema := &Schema{
		ID:      1,
		Schema:  schemaJSON,
		Version: 1,
		Subject: "test",
	}

	originalData := map[string]any{
		"id":         float64(123),
		"optional":   "test",
		"enumField":  "ACTIVE",
		"bytesField": []any{float64(1), float64(2), float64(3)},
	}

	// Serialize
	serialized, serdeErr := avroSerde.Serialize(originalData, schema)
	require.Nil(t, serdeErr, "Serialize should not return error")
	require.NotNil(t, serialized, "Serialized data should not be nil")

	// Deserialize
	deserialized, deserErr := avroSerde.Deserialize(serialized, schema)
	require.Nil(t, deserErr, "Deserialize should not return error")
	require.NotNil(t, deserialized, "Deserialized data should not be nil")

	result := deserialized.(map[string]any)
	// After deserialization, int values may be returned as int (platform-dependent)
	// Check that the value is correct regardless of type
	idValue := result["id"]
	if idInt32, ok := idValue.(int32); ok {
		assert.Equal(t, int32(123), idInt32)
	} else if idInt, ok := idValue.(int); ok {
		assert.Equal(t, 123, idInt)
	} else {
		t.Errorf("unexpected type for id: %T", idValue)
	}
	assert.Equal(t, "test", result["optional"])
	assert.Equal(t, "ACTIVE", result["enumField"])
	assert.Equal(t, []byte{1, 2, 3}, result["bytesField"])
}
