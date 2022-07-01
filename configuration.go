package kafka

type ConsumerConfiguration struct {
	KeyDeserializer     string `json:"keyDeserializer"`
	ValueDeserializer   string `json:"valueDeserializer"`
	SubjectNameStrategy string `json:"subjectNameStrategy"`
	UseMagicPrefix      bool   `json:"useMagicPrefix"`
}

type ProducerConfiguration struct {
	KeySerializer       string `json:"keySerializer"`
	ValueSerializer     string `json:"valueSerializer"`
	SubjectNameStrategy string `json:"subjectNameStrategy"`
}

type Configuration struct {
	Consumer       ConsumerConfiguration       `json:"consumer"`
	Producer       ProducerConfiguration       `json:"producer"`
	SchemaRegistry SchemaRegistryConfiguration `json:"schemaRegistry"`
}

// ValidateConfiguration validates the given configuration.
func ValidateConfiguration(configuration Configuration) *Xk6KafkaError {
	if (Configuration{}) == configuration {
		// No configuration, fallback to default
		return nil
	}

	if useSerializer(configuration, Key) || useSerializer(configuration, Value) {
		return nil
	}

	return nil
}

// GivenCredentials returns true if the given configuration has credentials.
func GivenCredentials(configuration Configuration) bool {
	if (Configuration{}) == configuration ||
		(SchemaRegistryConfiguration{}) == configuration.SchemaRegistry ||
		(BasicAuth{}) == configuration.SchemaRegistry.BasicAuth {
		return false
	}
	return configuration.SchemaRegistry.BasicAuth.Username != "" &&
		configuration.SchemaRegistry.BasicAuth.Password != ""
}
