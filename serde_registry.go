package kafka

import "github.com/riferrei/srclient"

type Serde[T Serializer | Deserializer] struct {
	Registry map[string]*SerdeType[T]
}

// NewSerializersRegistry creates a new instance of the Serializer registry.
func NewSerializersRegistry() *Serde[Serializer] {
	return &Serde[Serializer]{
		Registry: map[string]*SerdeType[Serializer]{
			StringSerializer:     NewSerdes[Serializer](SerializeString, StringSerializer, String, false),
			ByteArraySerializer:  NewSerdes[Serializer](SerializeByteArray, ByteArraySerializer, ByteArray, false),
			AvroSerializer:       NewSerdes[Serializer](SerializeAvro, AvroSerializer, srclient.Avro, true),
			ProtobufSerializer:   NewSerdes[Serializer](nil, ProtobufSerializer, srclient.Protobuf, true),
			JsonSchemaSerializer: NewSerdes[Serializer](SerializeJson, JsonSchemaSerializer, srclient.Json, true),
		},
	}
}

// NewDeserializersRegistry creates a new instance of the Deserializer registry.
func NewDeserializersRegistry() *Serde[Deserializer] {
	return &Serde[Deserializer]{
		Registry: map[string]*SerdeType[Deserializer]{
			StringDeserializer:     NewSerdes[Deserializer](DeserializeString, StringDeserializer, String, false),
			ByteArrayDeserializer:  NewSerdes[Deserializer](DeserializeByteArray, ByteArrayDeserializer, ByteArray, false),
			AvroDeserializer:       NewSerdes[Deserializer](DeserializeAvro, AvroDeserializer, srclient.Avro, true),
			ProtobufDeserializer:   NewSerdes[Deserializer](nil, ProtobufDeserializer, srclient.Protobuf, true),
			JsonSchemaDeserializer: NewSerdes[Deserializer](DeserializeJson, JsonSchemaDeserializer, srclient.Json, true),
		},
	}
}
