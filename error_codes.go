package kafka

import "fmt"

type errCode uint32

const (
	// non specific
	kafkaForbiddenInInitContext errCode = 1000
	configurationError          errCode = 1001
	noContextError              errCode = 1002
	contextCancelled            errCode = 1003
	cannotReportStats           errCode = 1004
	fileNotFound                errCode = 1005
	dialerError                 errCode = 1006
	noTLSConfig                 errCode = 1007

	// serdes errors
	invalidDataType             errCode = 2000
	failedDecodeFromWireFormat  errCode = 2001
	failedCreateAvroCodec       errCode = 2002
	failedEncodeToAvro          errCode = 2003
	failedEncodeAvroToBinary    errCode = 2004
	failedDecodeAvroFromBinary  errCode = 2005
	failedCreateJsonSchemaCodec errCode = 2006
	failedUnmarshalJson         errCode = 2007
	failedValidateJson          errCode = 2008
	failedEncodeToJson          errCode = 2009
	failedEncodeJsonToBinary    errCode = 2010
	failedDecodeJsonFromBinary  errCode = 2011

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
	failedAppendCaCertFile      errCode = 5004

	// schema registry
	messageTooShort      errCode = 6000
	schemaNotFound       errCode = 6001
	schemaCreationFailed errCode = 6002

	// topics
	failedCreateTopic    errCode = 7000
	failedDeleteTopic    errCode = 7001
	failedReadPartitions errCode = 7002
)

// ErrorForbiddenInInitContext is used when a Kafka producer was used in the init context
var ErrorForbiddenInInitContext = NewXk6KafkaError(
	kafkaForbiddenInInitContext,
	"Producing Kafka messages in the init context is not supported",
	nil)

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
	if e.OriginalError == nil {
		return e.Message
	}
	return fmt.Sprintf(e.Message+", OriginalError: %w", e.OriginalError)
}

// Unwrap implements the `xerrors.Wrapper` interface, so Xk6KafkaError are a bit
// future-proof Go 2 errors.
func (e Xk6KafkaError) Unwrap() error {
	return e.OriginalError
}
