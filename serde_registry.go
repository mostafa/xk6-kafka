package kafka

type Serde[T Serializer | Deserializer] struct {
	Registry map[string]*SerdeType[T]
}

func NewSerializersRegistry() *Serde[Serializer] {
	return &Serde[Serializer]{
		Registry: map[string]*SerdeType[Serializer]{
			StringSerializer:     NewSerdes[Serializer](SerializeString, StringSerializer, String, false),
			ByteArraySerializer:  NewSerdes[Serializer](SerializeByteArray, ByteArraySerializer, ByteArray, false),
			AvroSerializer:       NewSerdes[Serializer](SerializeAvro, AvroSerializer, String, true),
			ProtobufSerializer:   NewSerdes[Serializer](nil, ProtobufSerializer, String, true),
			JsonSchemaSerializer: NewSerdes[Serializer](SerializeJsonSchema, JsonSchemaSerializer, String, true),
		},
	}
}

func NewDeserializersRegistry() *Serde[Deserializer] {
	return &Serde[Deserializer]{
		Registry: map[string]*SerdeType[Deserializer]{
			StringDeserializer:     NewSerdes[Deserializer](DeserializeString, StringDeserializer, String, false),
			ByteArrayDeserializer:  NewSerdes[Deserializer](DeserializeByteArray, ByteArrayDeserializer, ByteArray, false),
			AvroDeserializer:       NewSerdes[Deserializer](DeserializeAvro, AvroDeserializer, String, true),
			ProtobufDeserializer:   NewSerdes[Deserializer](nil, ProtobufDeserializer, String, true),
			JsonSchemaDeserializer: NewSerdes[Deserializer](DeserializeJsonSchema, JsonSchemaDeserializer, String, true),
		},
	}
}
