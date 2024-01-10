package fferr

import (
	"fmt"

	"google.golang.org/grpc/codes"
)

func NewDatasetNotFoundError(resourceName, resourceVariant string, err error) DatasetNotFoundError {
	if err == nil {
		err = fmt.Errorf("initial dataset not found error")
	}
	baseError := newBaseGRPCError(err, DATASET_NOT_FOUND, codes.NotFound)
	baseError.AddDetail("Resource Name", resourceName)
	baseError.AddDetail("Resource Variant", resourceVariant)

	return DatasetNotFoundError{
		baseError,
	}
}

type DatasetNotFoundError struct {
	baseGRPCError
}

func NewTransformationNotFound(resourceName, resourceVariant string, err error) TransformationNotFound {
	if err == nil {
		err = fmt.Errorf("initial transformation not found error")
	}
	baseError := newBaseGRPCError(err, TRANSFORMATION_NOT_FOUND, codes.NotFound)
	baseError.AddDetail("Resource Name", resourceName)
	baseError.AddDetail("Resource Variant", resourceVariant)

	return TransformationNotFound{
		baseError,
	}
}

type TransformationNotFound struct {
	baseGRPCError
}

func NewEntityNotFound(featureName, featureVariant, entityName string, err error) EntityNotFound {
	if err == nil {
		err = fmt.Errorf("initial entity not found error")
	}
	baseError := newBaseGRPCError(err, ENTITY_NOT_FOUND, codes.NotFound)
	baseError.AddDetail("Feature Name", featureName)
	baseError.AddDetail("Feature Variant", featureVariant)
	baseError.AddDetail("Entity Name", entityName)

	return EntityNotFound{
		baseError,
	}
}

type EntityNotFound struct {
	baseGRPCError
}

func NewFeatureNotFound(featureName, featureVariant string, err error) FeatureNotFound {
	if err == nil {
		err = fmt.Errorf("initial feature not found error")
	}
	baseError := newBaseGRPCError(err, FEATURE_NOT_FOUND, codes.NotFound)
	baseError.AddDetail("Feature Name", featureName)
	baseError.AddDetail("Feature Variant", featureVariant)

	return FeatureNotFound{
		baseError,
	}
}

type FeatureNotFound struct {
	baseGRPCError
}

func NewTrainingSetNotFound(resourceName, resourceVariant string, err error) TrainingSetNotFound {
	if err == nil {
		err = fmt.Errorf("initial training set not found error")
	}
	baseError := newBaseGRPCError(err, TRAINING_SET_NOT_FOUND, codes.NotFound)
	baseError.AddDetail("Resource Name", resourceName)
	baseError.AddDetail("Resource Variant", resourceVariant)

	return TrainingSetNotFound{
		baseError,
	}
}

type TrainingSetNotFound struct {
	baseGRPCError
}

func NewInvalidResourceTypeError(resourceName, resourceVariant, resourceType string, err error) InvalidResourceTypeError {
	if err == nil {
		err = fmt.Errorf("initial invalid resource type not found error")
	}
	baseError := newBaseGRPCError(err, INVALID_RESOURCE_TYPE, codes.InvalidArgument)
	baseError.AddDetail("Resource Name", resourceName)
	baseError.AddDetail("Resource Variant", resourceVariant)
	baseError.AddDetail("Resource Type", resourceType)

	return InvalidResourceTypeError{
		baseError,
	}
}

type InvalidResourceTypeError struct {
	baseGRPCError
}
