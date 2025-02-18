// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package fferr

import (
	"errors"
	"fmt"
	"strings"

	pb "github.com/featureform/metadata/proto"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/rotisserie/eris"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ResourceType string

func (rt ResourceType) String() string {
	return string(rt)
}

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
	TYPE_ERROR                    = "Type Error"

	// MISCELLANEOUS:
	INTERNAL_ERROR      = "Internal Error"
	INVALID_ARGUMENT    = "Invalid Argument"
	PARSING_ERROR       = "Parsing Error"
	UNIMPLEMENTED_ERROR = "Unimplemented"

	// JOBS:
	JOB_DOES_NOT_EXIST        = "Job Does Not Exist"
	JOB_ALREADY_EXISTS        = "Job Already Exists"
	RESOURCE_ALREADY_COMPLETE = "Resource Already Complete"
	RESOURCE_ALREADY_FAILED   = "Resource Already Failed"
	RESOURCE_NOT_READY        = "Resource Not Ready"
	RESOURCE_FAILED           = "Resource Failed"
	INVALID_JOB_TARGET        = "Invalid Job Target"
	DEPENDENCY_FAILED         = "Dependency Failed"
	TASK_RUN_FAILED           = "Task Run Failed"

	// ETCD
	KEY_NOT_FOUND = "Key Not Found"

	// LOCKING
	KEY_ALREADY_LOCKED = "Key Already Locked"
	KEY_NOT_LOCKED     = "Key Not Locked"
	LOCK_EMPTY_KEY     = "Cannot Lock Empty Key"
	UNLOCK_EMPTY_KEY   = "Cannot Unlock Empty Key"
	EXCEEDED_WAIT_TIME = "Lock Exceeded Wait Time"

	// RESOURCE TYPES:
	PRIMARY_DATASET         ResourceType = "PRIMARY_DATASET"
	TRANSFORMATION          ResourceType = "TRANSFORMATION"
	FEATURE                 ResourceType = "FEATURE"
	LABEL                   ResourceType = "LABEL"
	TRAINING_SET            ResourceType = "TRAINING_SET"
	SOURCE                  ResourceType = "SOURCE"
	FEATURE_VARIANT         ResourceType = "FEATURE_VARIANT"
	FEATURE_MATERIALIZATION ResourceType = "FEATURE_MATERIALIZATION"
	LABEL_VARIANT           ResourceType = "LABEL_VARIANT"
	TRAINING_SET_VARIANT    ResourceType = "TRAINING_SET_VARIANT"
	SOURCE_VARIANT          ResourceType = "SOURCE_VARIANT"
	PROVIDER                ResourceType = "PROVIDER"
	ENTITY                  ResourceType = "ENTITY"
	MODEL                   ResourceType = "MODEL"
	USER                    ResourceType = "USER"

	// TASKS
	RESOURCE_TASK_FAILED = "Resource Task Failed"
	NO_RUNS_FOR_TASK     = "No Runs For Task"
)

type JSONStackTrace map[string]interface{}

type Error interface {
	GetCode() codes.Code
	GetType() string
	GRPCStatus() *status.Status
	ToErr() error
	AddDetail(key, value string)

	AddDetails(keysAndValues ...interface{})

	Error() string
	Stack() JSONStackTrace
}

func ToDashboardError(status *pb.ResourceStatus) string {
	errorStatus := status.ErrorStatus
	var reason string
	details := make(map[string]string)
	for _, detail := range errorStatus.GetDetails() {
		errorInfo := &errdetails.ErrorInfo{}
		if err := anypb.UnmarshalTo(detail, errorInfo, proto.UnmarshalOptions{}); err == nil {
			reason = errorInfo.Reason
			details = errorInfo.Metadata
			break // Assuming we only care about the first error detail.
		}
	}
	if reason == "" {
		if status.ErrorMessage != "" {
			return status.ErrorMessage
		}
		return ""
	}
	err := fmt.Sprintf("%s: %s", reason, errorStatus.GetMessage())
	for k, v := range details {
		err = fmt.Sprintf("%s\n>>> %s: %s", err, k, v)
	}
	return err
}

func FromErr(err error) Error {
	// If the error is nil, then simply pass it through to
	// avoid having to check for nil errors at the call site
	if err == nil {
		return nil
	}
	var grpcError Error
	if errors.As(err, &grpcError) {
		return grpcError
	}
	st, ok := status.FromError(err)
	if !ok {
		return NewInternalError(err)
	}
	// If the error is a valid status error but doesn't have any details, it stems from a
	// location in the codebase we haven't covered yet. In this case, we'll just return an
	// InternalError
	if len(st.Details()) == 0 {
		fmt.Println("No Details")
		return NewInternalError(fmt.Errorf(st.Message()))
	}
	// All fferr errors should have an ErrorInfo detail, so we'll iterate through the details
	// and cast them to ErrorInfo. If we find one, we'll create the appropriate error type
	// and return it
	for _, detail := range st.Details() {
		if errorInfo, ok := detail.(*errdetails.ErrorInfo); ok {
			errorMsg := err.Error()
			// This addresses the edge case where we receive a status error from another service and persist the
			// error message to ETCD, which currently only occurs in the coordinator service. If the error message
			// contains "rpc error:", we'll just return an empty string and let the GRPCError implementation of Error()
			// handle the error message
			if strings.Contains(err.Error(), "rpc error:") {
				errorMsg = ""
			}
			var detailKeys []string
			for k := range errorInfo.Metadata {
				detailKeys = append(detailKeys, k)
			}
			grpcError = &baseError{
				code:      st.Code(),
				errorType: errorInfo.Reason,
				GenericError: GenericError{
					msg:        errorMsg,
					err:        eris.New(err.Error()),
					details:    errorInfo.Metadata,
					detailKeys: detailKeys,
				},
			}
			// If there's a need to return a public implementation of GRPCError (e.g. InvalidArgumentError)
			// we can add a switch statement that checks the error type and returns the appropriate
			// error type, which will simply wrap the baseGRPCError (e.g. &InvalidArgumentError{baseGRPCError})
		} else {
			grpcError = NewInternalError(err)
		}
	}
	return grpcError
}

func newBaseError(err error, errorType string, code codes.Code) baseError {
	if err == nil {
		err = fmt.Errorf("initial error")
	}
	genericError := NewGenericError(err)

	return baseError{
		code:         code,
		errorType:    errorType,
		GenericError: genericError,
	}
}

type baseError struct {
	code      codes.Code
	errorType string
	GenericError
}

func (e *baseError) GetCode() codes.Code {
	return e.code
}

func (e *baseError) GetType() string {
	return e.errorType
}

func (e *baseError) GRPCStatus() *status.Status {
	// Assumes ToErr() returns an error compatible with gRPC status errors.
	// If not, you might need to adjust this to directly create and return
	// a new status.Status from the baseError fields.
	return status.Convert(e.ToErr())
}

func (e *baseError) ToErr() error {
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

func (e *baseError) AddDetail(key, value string) {
	e.GenericError.AddDetail(key, value)
}

func (e *baseError) AddDetails(keysAndValues ...interface{}) {
	e.GenericError.AddDetails(keysAndValues...)
}

func (e *baseError) AddFixSuggestion(suggestion string) {
	e.GenericError.AddDetail("fix_suggestion", suggestion)
}

func (e *baseError) Error() string {
	msg := fmt.Sprintf("%s: %s\n", e.errorType, e.msg)
	if len(e.details) == 0 {
		return msg
	}
	msg = fmt.Sprintf("%sDetails:\n", msg)
	for _, k := range e.detailKeys {
		msg = fmt.Sprintf("%s*%s: %s\n", msg, k, e.details[k])
	}
	return msg
}

func (e *baseError) Stack() JSONStackTrace {
	return e.GenericError.Stack()
}
