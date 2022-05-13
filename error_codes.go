package kafka

type errCode uint32

const (
	// non specific
	kafkaForbiddenInInitContext errCode = 1000
	configurationError          errCode = 1001
	contextCancelled            errCode = 1002
	cannotReportStats           errCode = 1003
	fileNotFound                errCode = 1004
	dialerError                 errCode = 1005

	// serdes errors
	invalidDataType             errCode = 2000
	failedEncodeToWireFormat    errCode = 2001
	failedDecodeFromWireFormat  errCode = 2002
	failedCreateAvroCodec       errCode = 2003
	failedEncodeToAvro          errCode = 2004
	failedEncodeAvroToBinary    errCode = 2005
	failedDecodeAvroFromBinary  errCode = 2006
	failedCreateJsonSchemaCodec errCode = 2007
	failedUnmarshalJsonSchema   errCode = 2008
	failedValidateJsonSchema    errCode = 2009

	// producer
	failedWriteMessage errCode = 3000

	// consumer
	failedSetOffset   errCode = 4000
	failedReadMessage errCode = 4001
	noMoreMessages    errCode = 4002

	// authentication
	failedCreateDialerWithScram errCode = 5000
	failedUnmarshalCreds        errCode = 5001
	failedLoadX509KeyPair       errCode = 5002
	failedReadCaCertFile        errCode = 5003

	// schema registry
	messageTooShort      errCode = 6000
	schemaCreationFailed errCode = 6001

	// topics
	failedCreateTopic    errCode = 7000
	failedDeleteTopic    errCode = 7001
	failedReadPartitions errCode = 7002
)

var (
	// ErrorForbiddenInInitContext is used when a Kafka producer was used in the init context
	ErrorForbiddenInInitContext = NewXk6KafkaError(
		kafkaForbiddenInInitContext,
		"Producing Kafka messages in the init context is not supported",
		nil)
)

type Xk6KafkaError struct {
	Code          errCode
	Message       string
	OriginalError error
}

// NewXk6KafkaError is the constructor for Xk6KafkaError
func NewXk6KafkaError(code errCode, msg string, originalErr error) *Xk6KafkaError {
	return &Xk6KafkaError{Code: code, Message: msg, OriginalError: originalErr}
}

// Error implements the `error` interface, so Xk6KafkaError are normal Go errors.
func (e Xk6KafkaError) Error() string {
	return e.Message
}

// Unwrap implements the `xerrors.Wrapper` interface, so Xk6KafkaError are a bit
// future-proof Go 2 errors.
func (e Xk6KafkaError) Unwrap() error {
	return e.OriginalError
}
