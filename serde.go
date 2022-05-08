package kafka

type Serializer func(configuration Configuration, topic string, data interface{}, element Element, schema string, version int) ([]byte, error)
type Deserializer func(configuration Configuration, data []byte, element Element, schema string, version int) interface{}

var (
	// TODO: Find a better way to do this, like serde registry or something
	Serializers = map[string]Serializer{
		"org.apache.kafka.common.serialization.StringSerializer":          SerializeString,
		"org.apache.kafka.common.serialization.ByteArraySerializer":       SerializeByteArray,
		"io.confluent.kafka.serializers.KafkaAvroSerializer":              SerializeAvro,
		"io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer": nil,
		"io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer":   nil,
	}

	Deserializers = map[string]Deserializer{
		"org.apache.kafka.common.serialization.StringDeserializer":          DeserializeString,
		"org.apache.kafka.common.serialization.ByteArrayDeserializer":       DeserializeByteArray,
		"io.confluent.kafka.serializers.KafkaAvroDeserializer":              DeserializeAvro,
		"io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer": nil,
		"io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer":   nil,
	}

	WireFormattedCodecs = map[string]bool{
		// Serializers
		"org.apache.kafka.common.serialization.StringSerializer":          false,
		"org.apache.kafka.common.serialization.ByteArraySerializer":       false,
		"io.confluent.kafka.serializers.KafkaAvroSerializer":              true,
		"io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer": true,
		"io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer":   true,

		// Deserializers
		"org.apache.kafka.common.serialization.StringDeserializer":          false,
		"org.apache.kafka.common.serialization.ByteArrayDeserializer":       false,
		"io.confluent.kafka.serializers.KafkaAvroDeserializer":              true,
		"io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer": true,
		"io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer":   true,
	}
)

func useSerializer(configuration Configuration, element Element) bool {
	if (Configuration{}) == configuration || (ProducerConfiguration{}) == configuration.Producer &&
		(element == Key && configuration.Producer.KeySerializer != "") || (element == Value && configuration.Producer.ValueSerializer != "") {
		return true
	}
	return false
}

func useDeserializer(configuration Configuration, element Element) bool {
	if (Configuration{}) == configuration || (ConsumerConfiguration{}) == configuration.Consumer &&
		(element == Key && configuration.Consumer.KeyDeserializer != "") || (element == Value && configuration.Consumer.ValueDeserializer != "") {
		return true
	}
	return false
}

func isWireFormatted(serde string) bool {
	return WireFormattedCodecs[serde]
}

func GetSerializer(serializer string, schema string) Serializer {
	// if schema exists default to Avro without schema registry
	// TODO: deprecate this
	if schema != "" {
		return SerializeAvro
	}

	serializerFunction := Serializers[serializer]
	if serializerFunction == nil {
		return SerializeString
	}
	return serializerFunction
}

func GetDeserializer(deserializer string, schema string) Deserializer {
	// if schema exists default to Avro without schema registry
	// TODO: deprecate this
	if schema != "" {
		return DeserializeAvro
	}

	deserializerFunction := Deserializers[deserializer]
	if deserializerFunction == nil {
		return DeserializeString
	}
	return deserializerFunction
}
