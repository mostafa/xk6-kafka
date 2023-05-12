package kafka

import (
	"github.com/riferrei/srclient"
	"go.k6.io/k6/js/common"
)

type Container struct {
	Data       interface{}         `json:"data"`
	Schema     *Schema             `json:"schema"`
	SchemaType srclient.SchemaType `json:"schemaType"`
}

// serialize checks whether the incoming data has a schema or not.
// If the data has a schema, it encodes the data into Avro, JSONSchema or Protocol Buffer.
// Then it adds the wire format prefix and returns the binary to be used in key or value.
// If no schema is passed, it treats the data as a byte array, a string or a JSON object without
// a JSONSchema. Then, it returns the data as a byte array.
// nolint: funlen
func (k *Kafka) serialize(container *Container) []byte {
	if container.Schema == nil {
		// we are dealing with a byte array, a string or a JSON object without a JSONSchema
		serde, err := GetSerdes(container.SchemaType)
		if err != nil {
			common.Throw(k.vu.Runtime(), err)
			return nil
		}

		data, err := serde.Serialize(container.Data, nil)
		if err != nil {
			common.Throw(k.vu.Runtime(), err)
			return nil
		}
		return data
	} else {
		// we are dealing with binary data to be encoded with Avro, JSONSchema or Protocol Buffer

		switch container.SchemaType {
		case srclient.Avro, srclient.Json:
			serde, err := GetSerdes(container.SchemaType)
			if err != nil {
				common.Throw(k.vu.Runtime(), err)
				return nil
			}

			bytesData, err := serde.Serialize(container.Data, container.Schema)
			if err != nil {
				common.Throw(k.vu.Runtime(), err)
				return nil
			}

			return k.encodeWireFormat(bytesData, container.Schema.ID)
		default:
			common.Throw(k.vu.Runtime(), ErrUnsupportedOperation)
			return nil
		}
	}
}

// deserialize checks whether the incoming data has a schema or not.
// If the data has a schema, it removes the wire format prefix and decodes the data into JSON
// using Avro, JSONSchema or Protocol Buffer schemas. It returns the decoded data as JSON object.
// If no schema is passed, it treats the data as a byte array, a string or a JSON object without
// a JSONSchema. Then, it returns the data based on how it can decode it.
// nolint: funlen
func (k *Kafka) deserialize(container *Container) interface{} {
	if container.Schema == nil {
		// we are dealing with a byte array, a string or a JSON object without a JSONSchema
		serde, err := GetSerdes(container.SchemaType)
		if err != nil {
			common.Throw(k.vu.Runtime(), err)
			return nil
		}

		switch container.Data.(type) {
		case []byte:
			switch container.SchemaType {
			case String:
				return string(container.Data.([]byte))
			default:
				if isJSON(container.Data.([]byte)) {
					js, err := toMap(container.Data.([]byte))
					if err != nil {
						common.Throw(k.vu.Runtime(), err)
						return nil
					}
					return js
				}
				return container.Data.([]byte)
			}
		case string:
			if isBase64Encoded(container.Data.(string)) {
				if data, err := base64ToBytes(container.Data.(string)); err != nil {
					common.Throw(k.vu.Runtime(), err)
					return nil
				} else {
					if result, err := serde.Deserialize(data, nil); err != nil {
						common.Throw(k.vu.Runtime(), err)
						return nil
					} else {
						return result
					}
				}
			}

			return []byte(container.Data.(string))
		default:
			return container.Data
		}
	} else {
		// we are dealing with binary data to be encoded with Avro, JSONSchema or Protocol Buffer
		runtime := k.vu.Runtime()

		var jsonBytes []byte

		switch container.Data.(type) {
		case []byte:
			jsonBytes = container.Data.([]byte)
		case string:
			// Decode the data into JSON bytes from base64-encoded data
			if isBase64Encoded(container.Data.(string)) {
				if data, err := base64ToBytes(container.Data.(string)); err != nil {
					common.Throw(k.vu.Runtime(), err)
					return nil
				} else {
					jsonBytes = data
				}
			}
		}

		// Remove wire format prefix
		jsonBytes = k.decodeWireFormat(jsonBytes)

		switch container.SchemaType {
		case srclient.Avro, srclient.Json:
			serde, err := GetSerdes(container.SchemaType)
			if err != nil {
				common.Throw(k.vu.Runtime(), err)
				return nil
			}

			deserialized, err := serde.Deserialize(jsonBytes, container.Schema)
			if err != nil {
				common.Throw(k.vu.Runtime(), err)
				return nil
			}

			if jsonObj, ok := deserialized.(map[string]interface{}); ok {
				return jsonObj
			} else {
				common.Throw(k.vu.Runtime(), ErrInvalidDataType)
				return nil
			}
		default:
			common.Throw(runtime, ErrUnsupportedOperation)
			return nil
		}
	}
}
