package fferr

import (
	"fmt"

	"google.golang.org/grpc/codes"
)

func NewDatasetNotFoundError(resourceName, resourceVariant string, err error) *DatasetNotFoundError {
	if err == nil {
		err = fmt.Errorf("initial dataset not found error")
	}
	baseError := newBaseGRPCError(err, DATASET_NOT_FOUND, codes.NotFound)
	baseError.AddDetail("Resource_Name", resourceName)
	baseError.AddDetail("Resource_Variant", resourceVariant)

	return &DatasetNotFoundError{
		baseError,
	}
}

type DatasetNotFoundError struct {
	baseGRPCError
}

func NewDataTypeNotFoundError(valueType string, err error) *DataTypeNotFoundError {
	if err == nil {
		err = fmt.Errorf("initial datatype not found error")
	}
	baseError := newBaseGRPCError(err, DATATYPE_NOT_FOUND, codes.NotFound)
	baseError.AddDetail("Value Type", valueType)

	return &DataTypeNotFoundError{
		baseError,
	}
}

type DataTypeNotFoundError struct {
	baseGRPCError
}

func NewTransformationNotFoundError(resourceName, resourceVariant string, err error) *TransformationNotFoundError {
	if err == nil {
		err = fmt.Errorf("initial transformation not found error")
	}
	baseError := newBaseGRPCError(err, TRANSFORMATION_NOT_FOUND, codes.NotFound)
	baseError.AddDetail("Resource_Name", resourceName)
	baseError.AddDetail("Resource_Variant", resourceVariant)

	return &TransformationNotFoundError{
		baseError,
	}
}

type TransformationNotFoundError struct {
	baseGRPCError
}

func NewEntityNotFoundError(featureName, featureVariant, entityName string, err error) *EntityNotFoundError {
	if err == nil {
		err = fmt.Errorf("initial entity not found error")
	}
	baseError := newBaseGRPCError(err, ENTITY_NOT_FOUND, codes.NotFound)
	baseError.AddDetail("Feature Name", featureName)
	baseError.AddDetail("Feature Variant", featureVariant)
	baseError.AddDetail("Entity Name", entityName)

	return &EntityNotFoundError{
		baseError,
	}
}

type EntityNotFoundError struct {
	baseGRPCError
}

func NewFeatureNotFoundError(featureName, featureVariant string, err error) *FeatureNotFoundError {
	if err == nil {
		err = fmt.Errorf("initial feature not found error")
	}
	baseError := newBaseGRPCError(err, FEATURE_NOT_FOUND, codes.NotFound)
	baseError.AddDetail("Feature Name", featureName)
	baseError.AddDetail("Feature Variant", featureVariant)

	return &FeatureNotFoundError{
		baseError,
	}
}

type FeatureNotFoundError struct {
	baseGRPCError
}

func NewTrainingSetNotFoundError(resourceName, resourceVariant string, err error) *TrainingSetNotFoundError {
	if err == nil {
		err = fmt.Errorf("initial training set not found error")
	}
	baseError := newBaseGRPCError(err, TRAINING_SET_NOT_FOUND, codes.NotFound)
	baseError.AddDetail("Resource Name", resourceName)
	baseError.AddDetail("Resource Variant", resourceVariant)

	return &TrainingSetNotFoundError{
		baseError,
	}
}

type TrainingSetNotFoundError struct {
	baseGRPCError
}

func NewInvalidResourceTypeError(resourceName, resourceVariant, resourceType string, err error) *InvalidResourceTypeError {
	if err == nil {
		err = fmt.Errorf("initial invalid resource type not found error")
	}
	baseError := newBaseGRPCError(err, INVALID_RESOURCE_TYPE, codes.InvalidArgument)
	baseError.AddDetail("Resource Name", resourceName)
	baseError.AddDetail("Resource Variant", resourceVariant)
	baseError.AddDetail("Resource Type", resourceType)

	return &InvalidResourceTypeError{
		baseError,
	}
}

type InvalidResourceTypeError struct {
	baseGRPCError
}

func NewInvalidResourceVariantNameError(resourceName, resourceVariant, resourceType string, err error) *InvalidResourceTypeError {
	if err == nil {
		err = fmt.Errorf("invalid resource variant name")
	}
	baseError := newBaseGRPCError(err, INVALID_RESOURCE_TYPE, codes.InvalidArgument)
	baseError.AddDetail("Resource_Name", resourceName)
	baseError.AddDetail("Resource_Variant", resourceVariant)
	baseError.AddDetail("Resource_Type", resourceType)

	return &InvalidResourceTypeError{
		baseError,
	}
}

type InvalidResourceNameVariantError struct {
	baseGRPCError
}
