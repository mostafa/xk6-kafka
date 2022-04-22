package kafka

type Serializer func(configuration Configuration, topic string, data interface{}, keyOrValue string, schema string) ([]byte, error)
type Deserializer func(configuration Configuration, data []byte, keyOrValue string, schema string) interface{}

func GetSerializer(serializer string, schema string) Serializer {
	// if schema exists default to AVRO without schema registry
	if schema != "" {
		return SerializeAvro
	}

	switch serializer {
	case "org.apache.kafka.common.serialization.ByteArraySerializer":
		return SerializeByteArray
	case "org.apache.kafka.common.serialization.StringSerializer":
		return SerializeString
	case "io.confluent.kafka.serializers.KafkaAvroSerializer":
		return SerializeAvro
	default:
		return SerializeString
	}
}

func GetDeserializer(deserializer string, schema string) Deserializer {
	// if schema exists default to AVRO without schema registry
	if schema != "" {
		return DeserializeAvro
	}

	switch deserializer {
	case "org.apache.kafka.common.serialization.ByteArrayDeserializer":
		return DeserializeByteArray
	case "org.apache.kafka.common.serialization.StringDeserializer":
		return DeserializeString
	case "io.confluent.kafka.serializers.KafkaAvroDeserializer":
		return DeserializeAvro
	default:
		return DeserializeString
	}
}
