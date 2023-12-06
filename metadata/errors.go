package metadata

import (
	"fmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type KeyNotFoundError struct {
	key string
}

func (e KeyNotFoundError) Error() string {
	return fmt.Sprintf("Key Not Found: %s", e.key)
}

type ResourceExistsError struct {
	ID ResourceID
}

func (err *ResourceExistsError) Error() string {
	id := err.ID
	name, variant, t := id.Name, id.Variant, id.Type
	errMsg := fmt.Sprintf("%s Exists.\nName: %s", t, name)
	if variant != "" {
		errMsg += "\nVariant: " + variant
	}
	return errMsg
}

func (err *ResourceExistsError) GRPCStatus() *status.Status {
	return status.New(codes.AlreadyExists, err.Error())
}

type ResourceNotFoundError struct {
	ID ResourceID
	E  error
}

func (err *ResourceNotFoundError) Error() string {
	id := err.ID
	var errMsg string
	if err.E != nil {
		errMsg = fmt.Sprintf("resource not found. %s err: %v", id.String(), err.E)
	} else {
		errMsg = fmt.Sprintf("resource not found. %s", id.String())
	}
	return errMsg
}

func (err *ResourceNotFoundError) GRPCStatus() *status.Status {
	return status.New(codes.NotFound, err.Error())
}

type ResourceChangedError struct {
	ResourceID
}

func (err *ResourceChangedError) Error() string {
	id := err.ResourceID
	errMsg := fmt.Sprintf("resource %s has changed. Please use a new variant.", id.String())
	return errMsg
}

func (err *ResourceChangedError) GRPCStatus() *status.Status {
	return status.New(codes.Internal, err.Error())
}
