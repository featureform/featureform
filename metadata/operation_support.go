package metadata

import (
	"github.com/featureform/fferr"
	pt "github.com/featureform/provider/provider_type"
)

type Operation string

const (
	OperationDelete Operation = "DELETE"
)

var SupportedTypesMap = map[Operation][]string{
	OperationDelete: {
		pt.SnowflakeOffline.String(),
	},
}

func ValidateProviderOperation(operation Operation, providerType string) error {
	supportedTypes, exists := SupportedTypesMap[operation]
	if !exists {
		// Operation doesn't exist in the map
		return fferr.NewInternalErrorf("Operation '%s' is not supported", operation)
	}

	for _, t := range supportedTypes {
		if t == providerType {
			// Operation is supported, return nil
			return nil
		}
	}

	return fferr.NewInternalErrorf("Operation '%s' is not supported for provider type '%s'", operation, providerType)
}
