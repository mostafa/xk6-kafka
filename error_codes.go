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
	unsupportedOperation        errCode = 1008

	// serdes errors.
	invalidDataType            errCode = 2000
	failedToEncode             errCode = 2001
	failedToEncodeToBinary     errCode = 2002
	failedToDecodeFromBinary   errCode = 2003
	failedUnmarshalJSON        errCode = 2004
	failedValidateJSON         errCode = 2005
	failedDecodeJSONFromBinary errCode = 2006
	failedUnmarshalSchema      errCode = 2007
	invalidSerdeType           errCode = 2008
	failedDecodeBase64         errCode = 2009

	// consumer.
	failedSetOffset   errCode = 3000
	failedReadMessage errCode = 3001
	noMoreMessages    errCode = 3002

	// authentication.
	failedCreateDialerWithScram   errCode = 4000
	failedCreateDialerWithSaslSSL errCode = 4001
	failedLoadX509KeyPair         errCode = 4002
	failedReadCaCertFile          errCode = 4003
	failedAppendCaCertFile        errCode = 4004

	// schema registry.
	messageTooShort                     errCode = 5000
	schemaNotFound                      errCode = 5001
	schemaCreationFailed                errCode = 5002
	failedConfigureSchemaRegistryClient errCode = 5003

	// topics.
	failedGetController  errCode = 6000
	failedCreateTopic    errCode = 6001
	failedDeleteTopic    errCode = 6002
	failedReadPartitions errCode = 6003
)

var (
	// ErrUnsupported is the error returned when the operation is not supported.
	ErrUnsupportedOperation = NewXk6KafkaError(unsupportedOperation, "Operation not supported", nil)

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

	// ErrInvalidSchema is used when a schema is not supported or is malformed.
	ErrInvalidSchema = NewXk6KafkaError(failedUnmarshalSchema, "Failed to unmarshal schema", nil)

	// ErrFailedTypeCast is used when a type cast failed.
	ErrFailedTypeCast = NewXk6KafkaError(failedTypeCast, "Failed to cast type", nil)

	// ErrUnknownSerdesType is used when a serdes type is not supported.
	ErrUnknownSerdesType = NewXk6KafkaError(invalidSerdeType, "Unknown serdes type", nil)

	// ErrNotEnoughArguments is used when a function is called with too few arguments.
	ErrNotEnoughArguments = errors.New("not enough arguments")

	// ErrNoSchemaRegistryClient is used when a schema registry client is not configured correctly.
	ErrNoSchemaRegistryClient = NewXk6KafkaError(
		failedConfigureSchemaRegistryClient,
		"Failed to configure the schema registry client",
		nil)

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
