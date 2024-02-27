package fferr

import (
	"errors"
	"fmt"
	pb "github.com/featureform/metadata/proto"
	"github.com/rotisserie/eris"
	"golang.org/x/net/context"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"reflect"
	"testing"
)

func setErrorType(err baseGRPCError, errorType string) error {
	switch errorType {
	case EXECUTION_ERROR:
		return &ExecutionError{err}
	case CONNECTION_ERROR:
		return &ConnectionError{err}
	case DATASET_NOT_FOUND:
		return &DatasetNotFoundError{err}
	case DATASET_ALREADY_EXISTS:
		return &DatasetAlreadyExistsError{err}
	case DATATYPE_NOT_FOUND:
		return &DataTypeNotFoundError{err}
	case TRANSFORMATION_NOT_FOUND:
		return &TransformationNotFoundError{err}
	case ENTITY_NOT_FOUND:
		return &EntityNotFoundError{err}
	case FEATURE_NOT_FOUND:
		return &FeatureNotFoundError{err}
	case TRAINING_SET_NOT_FOUND:
		return &TrainingSetNotFoundError{err}
	case INVALID_RESOURCE_TYPE:
		return &InvalidResourceTypeError{err}
	case INVALID_RESOURCE_NAME_VARIANT:
		return &InvalidResourceNameVariantError{err}
	case INVALID_FILE_TYPE:
		return &InvalidFileTypeError{err}
	case RESOURCE_CHANGED:
		return &ResourceChangedError{err}
	case INTERNAL_ERROR:
		return &InternalError{err}
	case INVALID_ARGUMENT:
		return &InvalidArgumentError{err}

	// JOBS:
	case JOB_DOES_NOT_EXIST:
		return &JobDoesNotExistError{err}
	case JOB_ALREADY_EXISTS:
		return &JobAlreadyExistsError{err}
	case RESOURCE_ALREADY_COMPLETE:
		return &ResourceAlreadyCompleteError{err}
	case RESOURCE_ALREADY_FAILED:
		return &ResourceAlreadyFailedError{err}
	case RESOURCE_NOT_READY:
		return &ResourceNotReadyError{err}
	case RESOURCE_FAILED:
		return &ResourceFailedError{err}

	// ETCD
	case KEY_NOT_FOUND:
		return &KeyNotFoundError{err}

	}
	return nil
}

func TestNewError(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		innerError error
		errorType  string
		errorCode  codes.Code
		details    []map[string]string
	}{
		{"Execution Error", NewExecutionError("postgres", fmt.Errorf("test error")), fmt.Errorf("test error"), EXECUTION_ERROR, codes.Internal, []map[string]string{{"provider": "postgres"}}},
		{"Feature Not Found Error", NewFeatureNotFoundError("name", "variant", fmt.Errorf("test error")), fmt.Errorf("test error"), FEATURE_NOT_FOUND, codes.NotFound, []map[string]string{{"feature_name": "name"}, {"feature_variant": "variant"}}},
		{"Connection Error", NewConnectionError("postgres", fmt.Errorf("test error")), fmt.Errorf("test error"), CONNECTION_ERROR, codes.Internal, []map[string]string{{"provider": "postgres"}}},
		{"Dataset Not Found Error", NewDatasetNotFoundError("name", "variant", fmt.Errorf("test error")), fmt.Errorf("test error"), DATASET_NOT_FOUND, codes.NotFound, []map[string]string{{"resource_name": "name"}, {"resource_variant": "variant"}}},
		{"Entity Not Found Error", NewEntityNotFoundError("name", "variant", "entity", fmt.Errorf("test error")), fmt.Errorf("test error"), ENTITY_NOT_FOUND, codes.NotFound, []map[string]string{{"feature_name": "name"}, {"feature_variant": "variant"}, {"entity_name": "entity"}}},
		{"Dataset Already Exists Error", NewDatasetAlreadyExistsError("name", "variant", fmt.Errorf("test error")), fmt.Errorf("test error"), DATASET_ALREADY_EXISTS, codes.AlreadyExists, []map[string]string{{"resource_name": "name"}, {"resource_variant": "variant"}}},
		{"Datatype Not Found Error", NewDataTypeNotFoundError("datatype", fmt.Errorf("test error")), fmt.Errorf("test error"), DATATYPE_NOT_FOUND, codes.NotFound, []map[string]string{{"value_type": "datatype"}}},
		{"Transformation Not Found Error", NewTransformationNotFoundError("name", "variant", fmt.Errorf("test error")), fmt.Errorf("test error"), TRANSFORMATION_NOT_FOUND, codes.NotFound, []map[string]string{{"resource_name": "name"}, {"resource_variant": "variant"}}},
		{"Feature Not Found Error", NewFeatureNotFoundError("name", "variant", fmt.Errorf("test error")), fmt.Errorf("test error"), FEATURE_NOT_FOUND, codes.NotFound, []map[string]string{{"feature_name": "name"}, {"feature_variant": "variant"}}},
		{"Training Set Not Found Error", NewTrainingSetNotFoundError("name", "variant", fmt.Errorf("test error")), fmt.Errorf("test error"), TRAINING_SET_NOT_FOUND, codes.NotFound, []map[string]string{{"resource_name": "name"}, {"resource_variant": "variant"}}},
		{"Invalid Resource Type Error", NewInvalidResourceTypeError("name", "variant", "type", fmt.Errorf("test error")), fmt.Errorf("test error"), INVALID_RESOURCE_TYPE, codes.InvalidArgument, []map[string]string{{"resource_name": "name"}, {"resource_variant": "variant"}, {"resource_type": "type"}}},
		{"Invalid File Type Error", NewInvalidFileTypeError("parquet", fmt.Errorf("test error")), fmt.Errorf("test error"), INVALID_FILE_TYPE, codes.InvalidArgument, []map[string]string{{"extension": "parquet"}}},
		{"Resource Changed Error", NewResourceChangedError("name", "variant", FEATURE_VARIANT, fmt.Errorf("test error")), fmt.Errorf("test error"), RESOURCE_CHANGED, codes.FailedPrecondition, []map[string]string{{"resource_name": "name"}, {"resource_variant": "variant"}, {"resource_type": string(FEATURE_VARIANT)}}},
		{"Internal Error", NewInternalError(fmt.Errorf("test error")), fmt.Errorf("test error"), INTERNAL_ERROR, codes.Internal, []map[string]string{}},
		{"Invalid Argument Error", NewInvalidArgumentError(fmt.Errorf("test error")), fmt.Errorf("test error"), INVALID_ARGUMENT, codes.InvalidArgument, []map[string]string{}},
		{"Job Already Exists Error", NewJobAlreadyExistsError("name", fmt.Errorf("test error")), fmt.Errorf("test error"), JOB_ALREADY_EXISTS, codes.AlreadyExists, []map[string]string{{"key": "name"}}},
		{"Job Does Not Exist Error", NewJobDoesNotExistError("name", fmt.Errorf("test error")), fmt.Errorf("test error"), JOB_DOES_NOT_EXIST, codes.NotFound, []map[string]string{{"key": "name"}}},
		{"Resource Already Complete Error", NewResourceAlreadyCompleteError("name", "variant", FEATURE_VARIANT, fmt.Errorf("test error")), fmt.Errorf("test error"), RESOURCE_ALREADY_COMPLETE, codes.FailedPrecondition, []map[string]string{{"resource_name": "name"}, {"resource_variant": "variant"}, {"resource_type": string(FEATURE_VARIANT)}}},
		{"Resource Already Failed Error", NewResourceAlreadyFailedError("name", "variant", FEATURE_VARIANT, fmt.Errorf("test error")), fmt.Errorf("test error"), RESOURCE_ALREADY_FAILED, codes.FailedPrecondition, []map[string]string{{"resource_name": "name"}, {"resource_variant": "variant"}, {"resource_type": string(FEATURE_VARIANT)}}},
		{"Resource Failed Error", NewResourceFailedError("name", "variant", FEATURE_VARIANT, fmt.Errorf("test error")), fmt.Errorf("test error"), RESOURCE_FAILED, codes.FailedPrecondition, []map[string]string{{"resource_name": "name"}, {"resource_variant": "variant"}, {"resource_type": string(FEATURE_VARIANT)}}},
		{"Resource Not Ready Error", NewResourceNotReadyError("name", "variant", FEATURE_VARIANT, fmt.Errorf("test error")), fmt.Errorf("test error"), RESOURCE_NOT_READY, codes.FailedPrecondition, []map[string]string{{"resource_name": "name"}, {"resource_variant": "variant"}, {"resource_type": string(FEATURE_VARIANT)}}},
		{"Key Not Found Error", NewKeyNotFoundError("key", fmt.Errorf("test error")), fmt.Errorf("test error"), KEY_NOT_FOUND, codes.NotFound, []map[string]string{{"key": "key"}}},
		{"Resource Internal Error", NewResourceInternalError("name", "variant", FEATURE_VARIANT, fmt.Errorf("test error")), fmt.Errorf("test error"), INTERNAL_ERROR, codes.Internal, []map[string]string{{"resource_name": "name"}, {"resource_variant": "variant"}, {"resource_type": string(FEATURE_VARIANT)}}},
		{"NewInvalidResourceVariantNameError", NewInvalidResourceVariantNameError("name", "variant", FEATURE_VARIANT, fmt.Errorf("test error")), fmt.Errorf("test error"), INVALID_RESOURCE_TYPE, codes.InvalidArgument, []map[string]string{{"resource_name": "name"}, {"resource_variant": "variant"}, {"resource_type": string(FEATURE_VARIANT)}}},
		{"NewResourceExecutionError", NewResourceExecutionError("provider", "name", "variant", FEATURE_VARIANT, fmt.Errorf("test error")), fmt.Errorf("test error"), EXECUTION_ERROR, codes.FailedPrecondition, []map[string]string{{"provider": "provider"}, {"resource_name": "name"}, {"resource_variant": "variant"}, {"resource_type": string(FEATURE_VARIANT)}}},
		{"NewProviderConfigError", NewProviderConfigError("provider", fmt.Errorf("test error")), fmt.Errorf("test error"), EXECUTION_ERROR, codes.InvalidArgument, []map[string]string{{"provider": "provider"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseError := newBaseGRPCError(tt.innerError, tt.errorType, tt.errorCode)
			for _, detail := range tt.details {
				for k, v := range detail {
					baseError.AddDetail(k, v)
				}
			}
			err := setErrorType(baseError, tt.errorType)
			if !reflect.DeepEqual(tt.err.Error(), err.Error()) {
				t.Errorf("Error() = %v, want %v", tt.err.Error(), err.Error())
			}
		})
	}
}

func TestNewErrorEmptyInner(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		innerError error
		errorType  string
		errorCode  codes.Code
		details    []map[string]string
	}{
		{"Execution Error", NewExecutionError("postgres", nil), fmt.Errorf("execution failed"), EXECUTION_ERROR, codes.Internal, []map[string]string{{"provider": "postgres"}}},
		{"Feature Not Found Error", NewFeatureNotFoundError("name", "variant", nil), fmt.Errorf("feature not found"), FEATURE_NOT_FOUND, codes.NotFound, []map[string]string{{"feature_name": "name"}, {"feature_variant": "variant"}}},
		{"Connection Error", NewConnectionError("postgres", nil), fmt.Errorf("failed connection"), CONNECTION_ERROR, codes.Internal, []map[string]string{{"provider": "postgres"}}},
		{"Dataset Not Found Error", NewDatasetNotFoundError("name", "variant", nil), fmt.Errorf("dataset not found"), DATASET_NOT_FOUND, codes.NotFound, []map[string]string{{"resource_name": "name"}, {"resource_variant": "variant"}}},
		{"Entity Not Found Error", NewEntityNotFoundError("name", "variant", "entity", nil), fmt.Errorf("entity not found"), ENTITY_NOT_FOUND, codes.NotFound, []map[string]string{{"feature_name": "name"}, {"feature_variant": "variant"}, {"entity_name": "entity"}}},
		{"Dataset Already Exists Error", NewDatasetAlreadyExistsError("name", "variant", nil), fmt.Errorf("dataset already exists"), DATASET_ALREADY_EXISTS, codes.AlreadyExists, []map[string]string{{"resource_name": "name"}, {"resource_variant": "variant"}}},
		{"Datatype Not Found Error", NewDataTypeNotFoundError("datatype", nil), fmt.Errorf("datatype not found"), DATATYPE_NOT_FOUND, codes.NotFound, []map[string]string{{"value_type": "datatype"}}},
		{"Transformation Not Found Error", NewTransformationNotFoundError("name", "variant", nil), fmt.Errorf("transformation not found"), TRANSFORMATION_NOT_FOUND, codes.NotFound, []map[string]string{{"resource_name": "name"}, {"resource_variant": "variant"}}},
		{"Feature Not Found Error", NewFeatureNotFoundError("name", "variant", nil), fmt.Errorf("feature not found"), FEATURE_NOT_FOUND, codes.NotFound, []map[string]string{{"feature_name": "name"}, {"feature_variant": "variant"}}},
		{"Training Set Not Found Error", NewTrainingSetNotFoundError("name", "variant", nil), fmt.Errorf("training set not found"), TRAINING_SET_NOT_FOUND, codes.NotFound, []map[string]string{{"resource_name": "name"}, {"resource_variant": "variant"}}},
		{"Invalid Resource Type Error", NewInvalidResourceTypeError("name", "variant", "type", nil), fmt.Errorf("invalid resource type"), INVALID_RESOURCE_TYPE, codes.InvalidArgument, []map[string]string{{"resource_name": "name"}, {"resource_variant": "variant"}, {"resource_type": "type"}}},
		{"Invalid File Type Error", NewInvalidFileTypeError("parquet", nil), fmt.Errorf("invalid filetype"), INVALID_FILE_TYPE, codes.InvalidArgument, []map[string]string{{"extension": "parquet"}}},
		{"Resource Changed Error", NewResourceChangedError("name", "variant", FEATURE_VARIANT, nil), fmt.Errorf("a resource with the same name and variant already exists but differs from the one you're trying to create; use a different variant name or autogenerated variant name"), RESOURCE_CHANGED, codes.FailedPrecondition, []map[string]string{{"resource_name": "name"}, {"resource_variant": "variant"}, {"resource_type": string(FEATURE_VARIANT)}}},
		{"Internal Error", NewInternalError(nil), fmt.Errorf("internal"), INTERNAL_ERROR, codes.Internal, []map[string]string{}},
		{"Invalid Argument Error", NewInvalidArgumentError(nil), fmt.Errorf("invalid argument"), INVALID_ARGUMENT, codes.InvalidArgument, []map[string]string{}},
		{"Job Already Exists Error", NewJobAlreadyExistsError("name", nil), fmt.Errorf("job already exists"), JOB_ALREADY_EXISTS, codes.AlreadyExists, []map[string]string{{"key": "name"}}},
		{"Job Does Not Exist Error", NewJobDoesNotExistError("name", nil), fmt.Errorf("job does not exist"), JOB_DOES_NOT_EXIST, codes.NotFound, []map[string]string{{"key": "name"}}},
		{"Resource Already Complete Error", NewResourceAlreadyCompleteError("name", "variant", FEATURE_VARIANT, nil), fmt.Errorf("resource already complete"), RESOURCE_ALREADY_COMPLETE, codes.FailedPrecondition, []map[string]string{{"resource_name": "name"}, {"resource_variant": "variant"}, {"resource_type": string(FEATURE_VARIANT)}}},
		{"Resource Already Failed Error", NewResourceAlreadyFailedError("name", "variant", FEATURE_VARIANT, nil), fmt.Errorf("resource already failed"), RESOURCE_ALREADY_FAILED, codes.FailedPrecondition, []map[string]string{{"resource_name": "name"}, {"resource_variant": "variant"}, {"resource_type": string(FEATURE_VARIANT)}}},
		{"Resource Failed Error", NewResourceFailedError("name", "variant", FEATURE_VARIANT, nil), fmt.Errorf("resource failed"), RESOURCE_FAILED, codes.FailedPrecondition, []map[string]string{{"resource_name": "name"}, {"resource_variant": "variant"}, {"resource_type": string(FEATURE_VARIANT)}}},
		{"Resource Not Ready Error", NewResourceNotReadyError("name", "variant", FEATURE_VARIANT, nil), fmt.Errorf("resource not ready"), RESOURCE_NOT_READY, codes.FailedPrecondition, []map[string]string{{"resource_name": "name"}, {"resource_variant": "variant"}, {"resource_type": string(FEATURE_VARIANT)}}},
		{"Key Not Found Error", NewKeyNotFoundError("key", nil), fmt.Errorf("key not found"), KEY_NOT_FOUND, codes.NotFound, []map[string]string{{"key": "key"}}},
		{"Resource Internal Error", NewResourceInternalError("name", "variant", FEATURE_VARIANT, nil), fmt.Errorf("internal error"), INTERNAL_ERROR, codes.Internal, []map[string]string{{"resource_name": "name"}, {"resource_variant": "variant"}, {"resource_type": string(FEATURE_VARIANT)}}},
		{"NewInvalidResourceVariantNameError", NewInvalidResourceVariantNameError("name", "variant", FEATURE_VARIANT, nil), fmt.Errorf("invalid resource variant name or variant"), INVALID_RESOURCE_TYPE, codes.InvalidArgument, []map[string]string{{"resource_name": "name"}, {"resource_variant": "variant"}, {"resource_type": string(FEATURE_VARIANT)}}},
		{"NewResourceExecutionError", NewResourceExecutionError("provider", "name", "variant", FEATURE_VARIANT, nil), fmt.Errorf("execution failed on resource"), EXECUTION_ERROR, codes.FailedPrecondition, []map[string]string{{"provider": "provider"}, {"resource_name": "name"}, {"resource_variant": "variant"}, {"resource_type": string(FEATURE_VARIANT)}}},
		{"NewProviderConfigError", NewProviderConfigError("provider", nil), fmt.Errorf("provider config"), EXECUTION_ERROR, codes.InvalidArgument, []map[string]string{{"provider": "provider"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseError := newBaseGRPCError(tt.innerError, tt.errorType, tt.errorCode)
			for _, detail := range tt.details {
				for k, v := range detail {
					baseError.AddDetail(k, v)
				}
			}
			err := setErrorType(baseError, tt.errorType)
			if !reflect.DeepEqual(tt.err.Error(), err.Error()) {
				t.Errorf("Error() = %v, want %v", tt.err.Error(), err.Error())
			}
		})
	}
}

func TestFromErr(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: "",
		},
		{
			name: "already a GRPCError",
			err: &baseGRPCError{
				code:      codes.Internal,
				errorType: "Reason",
				GenericError: GenericError{
					msg: "Message",
					err: eris.New("mock grpc error"),
				},
			},
			expected: "Reason: Message\n",
		},
		{
			name:     "regular error",
			err:      errors.New("regular error"),
			expected: "Internal Error: regular error\n", // Assuming NewInternalError wraps any non-GRPC, non-status errors into a MockGRPCError
		},
		{
			name:     "status error without details",
			err:      status.Error(codes.Internal, "status error without details"),
			expected: "Internal Error: status error without details\n", // Assuming NewInternalError is used for errors without details
		},
		{
			name: "status error with ErrorInfo detail",
			err: func() error {
				st := status.New(codes.Internal, "invalid argument")
				detail, _ := st.WithDetails(&errdetails.ErrorInfo{
					Reason:   "Reason",
					Metadata: map[string]string{"detail": "more info"},
				})
				return detail.Err()
			}(),
			expected: "Reason: \nDetails:\n*detail: more info\n", // Assuming the error message is handled differently for detailed status errors
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			grpcError := FromErr(tt.err)
			if grpcError == nil && tt.expected != "" {
				t.Errorf("Expected non-nil GRPCError for %v", tt.name)
			} else if grpcError != nil && grpcError.Error() != tt.expected {
				t.Errorf("FromErr(%v) = %v, want %v", tt.name, grpcError.Error(), tt.expected)
			}
		})
	}
}

func TestGenericError_AddDetail(t *testing.T) {
	type fields struct {
		msg     string
		err     error
		details map[string]string
	}
	type args struct {
		key   string
		value string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{"Simple", fields{"", fmt.Errorf(""), map[string]string{}}, args{"key", "value"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &GenericError{
				msg:     tt.fields.msg,
				err:     tt.fields.err,
				details: tt.fields.details,
			}
			e.AddDetail(tt.args.key, tt.args.value)
		})
	}
}

func TestGenericError_Details(t *testing.T) {
	type fields struct {
		msg     string
		err     error
		details map[string]string
	}
	tests := []struct {
		name   string
		fields fields
		want   map[string]string
	}{
		{"Simple", fields{"", fmt.Errorf(""), map[string]string{"key": "value"}}, map[string]string{"key": "value"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &GenericError{
				msg:     tt.fields.msg,
				err:     tt.fields.err,
				details: tt.fields.details,
			}
			if got := e.Details(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Details() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGenericError_Error(t *testing.T) {
	type fields struct {
		msg     string
		err     error
		details map[string]string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{"Simple", fields{"message", fmt.Errorf("test error"), map[string]string{"key": "value"}}, "message"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &GenericError{
				msg:     tt.fields.msg,
				err:     tt.fields.err,
				details: tt.fields.details,
			}
			if got := e.Error(); got != tt.want {
				t.Errorf("Error() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGenericError_SetMessage(t *testing.T) {
	type fields struct {
		msg     string
		err     error
		details map[string]string
	}
	type args struct {
		msg string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{"Simple", fields{"child", fmt.Errorf("test error"), map[string]string{"key": "value"}}, args{"parent"}, "parent: child"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &GenericError{
				msg:     tt.fields.msg,
				err:     tt.fields.err,
				details: tt.fields.details,
			}
			e.SetMessage(tt.args.msg)
			if e.msg != tt.want {
				t.Errorf("Message = %v, want %v", e.msg, tt.want)
			}
		})
	}
}

func TestGenericError_Stack(t *testing.T) {
	type fields struct {
		msg     string
		err     error
		details map[string]string
	}
	tests := []struct {
		name   string
		fields fields
		want   JSONStackTrace
	}{
		{"Simple", fields{"child", fmt.Errorf("test error"), map[string]string{"key": "value"}}, JSONStackTrace{"external": "test error"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &GenericError{
				msg:     tt.fields.msg,
				err:     tt.fields.err,
				details: tt.fields.details,
			}
			if got := e.Stack(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Stack() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewGenericError(t *testing.T) {
	type args struct {
		err error
	}
	tests := []struct {
		name string
		args args
		want GenericError
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewGenericError(tt.args.err); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewGenericError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestToDashboardError(t *testing.T) {
	tests := []struct {
		name     string
		status   *pb.ErrorStatus
		expected string
	}{
		{
			name: "no details",
			status: &pb.ErrorStatus{
				Message: "An error occurred",
			},
			expected: ": An error occurred",
		},
		{
			name: "with details",
			status: &pb.ErrorStatus{
				Message: "An error occurred",
				Details: []*anypb.Any{
					func() *anypb.Any {
						detail, _ := anypb.New(&errdetails.ErrorInfo{
							Reason: "INVALID_ARGUMENT",
						})
						return detail
					}(),
				},
			},
			expected: "INVALID_ARGUMENT: An error occurred",
		},
		{
			name: "with non-errorinfo details",
			status: &pb.ErrorStatus{
				Message: "Partial failure",
				Details: []*anypb.Any{
					func() *anypb.Any {
						detail, _ := anypb.New(&pb.Feature{})
						return detail
					}(), // Assuming SomeOtherMessage is a message that cannot be unmarshaled into ErrorInfo
				},
			},
			expected: ": Partial failure",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ToDashboardError(tt.status)
			if result != tt.expected {
				t.Errorf("ToDashboardError() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestUnaryServerInterceptor(t *testing.T) {
	type args struct {
		ctx     context.Context
		req     interface{}
		info    *grpc.UnaryServerInfo
		handler grpc.UnaryHandler
	}
	tests := []struct {
		name    string
		args    args
		want    interface{}
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := UnaryServerInterceptor(tt.args.ctx, tt.args.req, tt.args.info, tt.args.handler)
			if (err != nil) != tt.wantErr {
				t.Errorf("UnaryServerInterceptor() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("UnaryServerInterceptor() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_baseGRPCError_AddDetail(t *testing.T) {
	type fields struct {
		code    codes.Code
		errType string
		details map[string]string
	}
	type args struct {
		key   string
		value string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{"Simple", fields{codes.Internal, "Some Error", map[string]string{}}, args{"key", "value"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &baseGRPCError{
				code:      tt.fields.code,
				errorType: tt.fields.errType,
				GenericError: GenericError{
					details: tt.fields.details,
				},
			}
			e.AddDetail(tt.args.key, tt.args.value)
		})
	}
}

func Test_baseGRPCError_Error(t *testing.T) {
	type fields struct {
		code         codes.Code
		errorType    string
		GenericError GenericError
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{"Simple", fields{codes.Internal, "Some Error", GenericError{}}, "Some Error: \n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &baseGRPCError{
				code:         tt.fields.code,
				errorType:    tt.fields.errorType,
				GenericError: tt.fields.GenericError,
			}
			if got := e.Error(); got != tt.want {
				t.Errorf("Error() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_baseGRPCError_GRPCStatus(t *testing.T) {
	type fields struct {
		code         codes.Code
		errorType    string
		GenericError GenericError
	}
	tests := []struct {
		name   string
		fields fields
		want   *status.Status
	}{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &baseGRPCError{
				code:         tt.fields.code,
				errorType:    tt.fields.errorType,
				GenericError: tt.fields.GenericError,
			}
			if got := e.GRPCStatus(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GRPCStatus() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_baseGRPCError_GetCode(t *testing.T) {
	type fields struct {
		code         codes.Code
		errorType    string
		GenericError GenericError
	}
	tests := []struct {
		name   string
		fields fields
		want   codes.Code
	}{
		{"Simple", fields{codes.Internal, "Some Error", GenericError{}}, codes.Internal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &baseGRPCError{
				code:         tt.fields.code,
				errorType:    tt.fields.errorType,
				GenericError: tt.fields.GenericError,
			}
			if got := e.GetCode(); got != tt.want {
				t.Errorf("GetCode() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_baseGRPCError_GetType(t *testing.T) {
	type fields struct {
		code         codes.Code
		errorType    string
		GenericError GenericError
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{"Simple", fields{codes.Internal, "Some Error", GenericError{}}, "Some Error"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &baseGRPCError{
				code:         tt.fields.code,
				errorType:    tt.fields.errorType,
				GenericError: tt.fields.GenericError,
			}
			if got := e.GetType(); got != tt.want {
				t.Errorf("GetType() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_baseGRPCError_Stack(t *testing.T) {
	type fields struct {
		code         codes.Code
		errorType    string
		GenericError GenericError
	}
	tests := []struct {
		name   string
		fields fields
		want   JSONStackTrace
	}{
		{"Simple", fields{codes.Internal, "Some Error", GenericError{}}, JSONStackTrace{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &baseGRPCError{
				code:         tt.fields.code,
				errorType:    tt.fields.errorType,
				GenericError: tt.fields.GenericError,
			}
			if got := e.Stack(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Stack() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_baseGRPCError_ToErr(t *testing.T) {
	type fields struct {
		code         codes.Code
		errorType    string
		GenericError GenericError
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{"Simple", fields{codes.Internal, "Some Error", GenericError{}}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &baseGRPCError{
				code:         tt.fields.code,
				errorType:    tt.fields.errorType,
				GenericError: tt.fields.GenericError,
			}
			if err := e.ToErr(); (err != nil) != tt.wantErr {
				t.Errorf("ToErr() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_newBaseGRPCError(t *testing.T) {
	type args struct {
		err       error
		errorType string
		code      codes.Code
	}
	tests := []struct {
		name string
		args args
		want baseGRPCError
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newBaseGRPCError(tt.args.err, tt.args.errorType, tt.args.code); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newBaseGRPCError() = %v, want %v", got, tt.want)
			}
		})
	}
}
