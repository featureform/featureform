package fferr

import (
	"fmt"
	"google.golang.org/grpc/codes"
)

type ResourceTaskFailedError struct {
	baseGRPCError
}

func NewResourceTaskFailedError(name, variant, resType string, err error) *ResourceTaskFailedError {
	if err == nil {
		err = fmt.Errorf("resource %s (%s) type %s failed to run: %w", name, variant, resType, err)
	}
	baseError := newBaseGRPCError(err, RESOURCE_TASK_FAILED, codes.Internal)

	return &ResourceTaskFailedError{
		baseError,
	}
}

type NoRunsForTaskError struct {
	baseGRPCError
}

func NewNoRunsForTaskError(taskID string) *NoRunsForTaskError {
	err := fmt.Errorf("no runs found for task %s", taskID)
	baseError := newBaseGRPCError(err, NO_RUNS_FOR_TASK, codes.Internal)

	return &NoRunsForTaskError{
		baseError,
	}
}
