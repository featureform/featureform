package fferr

import (
	"fmt"
	"google.golang.org/grpc/codes"
)

type ResourceTaskFailed struct {
	baseGRPCError
}

func NewResourceTaskFailed(name, variant, resType string, err error) *ResourceTaskFailed {
	if err == nil {
		err = fmt.Errorf("resource %s (%s) type %s failed to run: %w", name, variant, resType, err)
	}
	baseError := newBaseGRPCError(err, RESOURCE_TASK_FAILED, codes.Internal)

	return &ResourceTaskFailed{
		baseError,
	}
}
