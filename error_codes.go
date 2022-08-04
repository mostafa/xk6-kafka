package kafka

import (
	"errors"
	"fmt"
)

type errCode uint32

const (
	// non specific.
	kafkaForbiddenInInitContext errCode = 1000
	noContextError              errCode = 1001
	contextCancelled            errCode = 1002
	cannotReportStats           errCode = 1003
	fileNotFound                errCode = 1004
	dialerError                 errCode = 1005
	noTLSConfig                 errCode = 1006
	failedTypeCast              errCode = 1007

	// serdes errors.
	invalidDataType             errCode = 2000
	failedDecodeFromWireFormat  errCode = 2001
	failedCreateAvroCodec       errCode = 2002
	failedEncodeToAvro          errCode = 2003
	failedEncodeAvroToBinary    errCode = 2004
	failedDecodeAvroFromBinary  errCode = 2005
	failedCreateJSONSchemaCodec errCode = 2006
	failedUnmarshalJSON         errCode = 2007
	failedValidateJSON          errCode = 2008
	failedEncodeToJSON          errCode = 2009
	failedDecodeJSONFromBinary  errCode = 2010
	failedToUnmarshalSchema     errCode = 2011

	// producer.
	failedWriteMessage errCode = 3000

	// consumer.
	failedSetOffset   errCode = 4000
	failedReadMessage errCode = 4001
	noMoreMessages    errCode = 4002

	// authentication.
	failedCreateDialerWithScram   errCode = 5000
	failedCreateDialerWithSaslSSL errCode = 5001
	failedLoadX509KeyPair         errCode = 5002
	failedReadCaCertFile          errCode = 5003
	failedAppendCaCertFile        errCode = 5004

	// schema registry.
	messageTooShort      errCode = 6000
	schemaNotFound       errCode = 6001
	schemaCreationFailed errCode = 6002

	// topics.
	failedGetController  errCode = 7000
	failedCreateTopic    errCode = 7001
	failedDeleteTopic    errCode = 7002
	failedReadPartitions errCode = 7003
)

var (
	// ErrForbiddenInInitContext is used when a Kafka producer was used in the init context.
	ErrForbiddenInInitContext = NewXk6KafkaError(
		kafkaForbiddenInInitContext,
		"Producing Kafka messages in the init context is not supported",
		nil)

	// ErrInvalidDataType is used when a data type is not supported.
	ErrInvalidDataType = NewXk6KafkaError(
		invalidDataType,
		"Invalid data type provided for serializer/deserializer",
		nil)

	// ErrNotEnoughArguments is used when a function is called with too few arguments.
	ErrNotEnoughArguments = errors.New("not enough arguments")

	ErrInvalidPEMData = errors.New("tls: failed to find any PEM data in certificate input")
)

type Xk6KafkaError struct {
	Code          errCode
	Message       string
	OriginalError error
}

// NewXk6KafkaError is the constructor for Xk6KafkaError.
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
