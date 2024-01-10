package fferr

import (
	"fmt"

	"google.golang.org/grpc/codes"
)

func NewInternalError(err error) InternalError {
	if err == nil {
		err = fmt.Errorf("initial internal error")
	}
	baseError := newBaseGRPCError(err, INTERNAL_ERROR, codes.Internal)

	return InternalError{
		baseError,
	}
}

type InternalError struct {
	baseGRPCError
}
