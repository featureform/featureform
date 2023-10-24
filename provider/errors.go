package provider

import (
	"fmt"

	pt "github.com/featureform/provider/provider_type"
)

type Action string

const (
	FilePathCreation     Action = "file_path_creation"
	Write                Action = "write"
	Read                 Action = "read"
	JobSubmission        Action = "job_submission"
	ConfigDeserialize    Action = "config_deserialize"
	ConfigSerialize      Action = "config_serialize"
	ClientInitialization Action = "client_initialization"
	Ping                 Action = "ping"
)

type ProviderErrorType string

const (
	Connection ProviderErrorType = "connection_error"
	Runtime    ProviderErrorType = "runtime_error"
	Internal   ProviderErrorType = "internal_error"
)

type InvalidQueryError struct {
	error string
}

func (e InvalidQueryError) Error() string {
	return e.error
}

type SQLError struct {
	error error
}

func (e SQLError) Error() string {
	return e.error.Error()
}

type TransformationTypeError struct {
	error string
}

func (e TransformationTypeError) Error() string {
	return e.error
}

type EmptyParquetFileError struct{}

func (e EmptyParquetFileError) Error() string {
	return "could not read empty parquet file"
}

// type FileStoreError struct {
// 	Type    fs.FileStoreType
// 	Message string
// }

// func (e FileStoreError) Error() string {
// 	return fmt.Sprintf("(%s) %s", e.Type, e.Message)
// }

// type SparkExecutorError struct {
// 	Message string
// }

// func (e SparkExecutorError) Error() string {
// 	return e.Message
// }

type BaseError struct {
	Type pt.Type
	Action
	Message string
}

func (e BaseError) Error() string {
	return fmt.Sprintf("(%s - %s) %s", e.Type, e.Action, e.Message)
}

type ConnectionError struct {
	BaseError
}

type RuntimeError struct {
	BaseError
}
type InternalError struct {
	BaseError
}

func NewProviderError(errorType ProviderErrorType, providerType pt.Type, action Action, message string) error {
	switch errorType {
	case Connection:
		return ConnectionError{
			BaseError: BaseError{
				Type:    providerType,
				Action:  action,
				Message: message,
			},
		}
	case Runtime:
		return RuntimeError{
			BaseError: BaseError{
				Type:    providerType,
				Action:  action,
				Message: message,
			},
		}
	case Internal:
		return InternalError{
			BaseError: BaseError{
				Type:    providerType,
				Action:  action,
				Message: message,
			},
		}
	default:
		return nil
	}
}
