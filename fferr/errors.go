package fferr

import (
	"fmt"

	"github.com/rotisserie/eris"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ResourceType string

const (
	// PROVIDERS:
	EXECUTION_ERROR  = "Execution Error"
	CONNECTION_ERROR = "Connection Error"

	// DATA:
	DATASET_NOT_FOUND             = "Dataset Not Found"
	DATASET_ALREADY_EXISTS        = "Dataset Already Exists"
	DATATYPE_NOT_FOUND            = "Datatype Not Found"
	TRANSFORMATION_NOT_FOUND      = "Transformation Not Found"
	ENTITY_NOT_FOUND              = "Entity Not Found"
	FEATURE_NOT_FOUND             = "Feature Not Found"
	TRAINING_SET_NOT_FOUND        = "Training Set Not Found"
	INVALID_RESOURCE_TYPE         = "Invalid Resource Type"
	INVALID_RESOURCE_NAME_VARIANT = "Invalid Resource Name Variant"
	INVALID_FILE_TYPE             = "Invalid File Type"
	RESOURCE_CHANGED              = "Resource Changed"

	// MISCELLANEOUS:
	INTERNAL_ERROR   = "Internal Error"
	INVALID_ARGUMENT = "Invalid Argument"

	// JOBS:
	JOB_DOES_NOT_EXIST        = "Job Does Not Exist"
	JOB_ALREADY_EXISTS        = "Job Already Exists"
	RESOURCE_ALREADY_COMPLETE = "Resource Already Complete"
	RESOURCE_ALREADY_FAILED   = "Resource Already Failed"
	RESOURCE_NOT_READY        = "Resource Not Ready"
	RESOURCE_FAILED           = "Resource Failed"

	// ETCD
	KEY_NOT_FOUND = "Key Not Found"

	// RESOURCE TYPES:
	FEATURE              ResourceType = "FEATURE"
	LABEL                ResourceType = "LABEL"
	TRAINING_SET         ResourceType = "TRAINING_SET"
	SOURCE               ResourceType = "SOURCE"
	FEATURE_VARIANT      ResourceType = "FEATURE_VARIANT"
	LABEL_VARIANT        ResourceType = "LABEL_VARIANT"
	TRAINING_SET_VARIANT ResourceType = "TRAINING_SET_VARIANT"
	SOURCE_VARIANT       ResourceType = "SOURCE_VARIANT"
	PROVIDER             ResourceType = "PROVIDER"
	ENTITY               ResourceType = "ENTITY"
	MODEL                ResourceType = "MODEL"
	USER                 ResourceType = "USER"
)

type JSONStackTrace map[string]interface{}

type GRPCError interface {
	GetCode() codes.Code
	GetType() string
	ToErr() error
	AddDetail(key, value string)
	Error() string
}

func FromErr(err error) GRPCError {
	// If the error is nil, then simply pass it through to
	// avoid having to check for nil errors at the call site
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return NewInternalError(err)
	}
	var grpcError GRPCError
	for _, detail := range st.Details() {
		if errorInfo, ok := detail.(*errdetails.ErrorInfo); ok {
			baseGRPCError := baseGRPCError{
				code:      st.Code(),
				errorType: errorInfo.Reason,
				GenericError: GenericError{
					msg:     err.Error(),
					err:     eris.New(err.Error()),
					details: errorInfo.Metadata,
				},
			}
			switch errorInfo.Reason {
			case KEY_NOT_FOUND:
				grpcError = &KeyNotFoundError{baseGRPCError}
			case EXECUTION_ERROR:
				grpcError = &ExecutionError{baseGRPCError}
			case CONNECTION_ERROR:
				grpcError = &ConnectionError{baseGRPCError}
			case DATASET_NOT_FOUND:
				grpcError = &DatasetNotFoundError{baseGRPCError}
			case DATASET_ALREADY_EXISTS:
				grpcError = &DatasetAlreadyExistsError{baseGRPCError}
			case DATATYPE_NOT_FOUND:
				grpcError = &DataTypeNotFoundError{baseGRPCError}
			case TRANSFORMATION_NOT_FOUND:
				grpcError = &TransformationNotFoundError{baseGRPCError}
			case ENTITY_NOT_FOUND:
				grpcError = &EntityNotFoundError{baseGRPCError}
			case FEATURE_NOT_FOUND:
				grpcError = &FeatureNotFoundError{baseGRPCError}
			case TRAINING_SET_NOT_FOUND:
				grpcError = &TrainingSetNotFoundError{baseGRPCError}
			case INVALID_RESOURCE_TYPE:
				grpcError = &InvalidResourceTypeError{baseGRPCError}
			case INVALID_RESOURCE_NAME_VARIANT:
				grpcError = &InvalidResourceNameVariantError{baseGRPCError}
			case INVALID_FILE_TYPE:
				grpcError = &InvalidFileTypeError{baseGRPCError}
			case RESOURCE_CHANGED:
				grpcError = &ResourceChangedError{baseGRPCError}
			case INTERNAL_ERROR:
				grpcError = &InternalError{baseGRPCError}
			case INVALID_ARGUMENT:
				grpcError = &InvalidArgument{baseGRPCError}
			case JOB_DOES_NOT_EXIST:
				grpcError = &JobDoesNotExistError{baseGRPCError}
			case JOB_ALREADY_EXISTS:
				grpcError = &JobAlreadyExistsError{baseGRPCError}
			case RESOURCE_ALREADY_COMPLETE:
				grpcError = &ResourceAlreadyCompleteError{baseGRPCError}
			case RESOURCE_ALREADY_FAILED:
				grpcError = &ResourceAlreadyFailedError{baseGRPCError}
			case RESOURCE_NOT_READY:
				grpcError = &ResourceNotReadyError{baseGRPCError}
			case RESOURCE_FAILED:
				grpcError = &ResourceFailedError{baseGRPCError}
			default:
				grpcError = &InternalError{baseGRPCError}
			}
		} else {
			grpcError = NewInternalError(err)
		}
	}
	return grpcError
}

func newBaseGRPCError(err error, errorType string, code codes.Code) baseGRPCError {
	if err == nil {
		err = fmt.Errorf("initial error")
	}
	genericError := NewGenericError(err)

	return baseGRPCError{
		code:         code,
		errorType:    errorType,
		GenericError: genericError,
	}
}

type baseGRPCError struct {
	code      codes.Code
	errorType string
	GenericError
}

func (e *baseGRPCError) GetCode() codes.Code {
	return e.code
}

func (e *baseGRPCError) GetType() string {
	return string(e.errorType)
}

func (e *baseGRPCError) ToErr() error {
	st := status.New(e.code, e.msg)
	ef := &errdetails.ErrorInfo{
		Reason:   e.errorType,
		Metadata: e.details,
	}
	statusWithDetails, err := st.WithDetails(ef)
	if err == nil {
		return statusWithDetails.Err()
	}
	return st.Err()
}

func (e *baseGRPCError) AddDetail(key, value string) {
	e.GenericError.AddDetail(key, value)
}

func (e *baseGRPCError) Error() string {
	return e.GenericError.Error()
}
