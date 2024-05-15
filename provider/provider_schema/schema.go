// Package provider_schema contains ...
package provider_schema

import (
	"fmt"
)

const (
	base_path = "featureform"
)

// ResourceToDirectoryPath returns the directory path for a given ResourceID in the filestore
func ResourceToDirectoryPath(resourceType, name, variant string) string {
	return fmt.Sprintf("%s/%s/%s/%s", base_path, resourceType, name, variant)
}

// ResourceToPicklePath returns the path to the pickled DataFrame transformation for a given ResourceID
func ResourceToPicklePath(name, variant string) string {
	return fmt.Sprintf("%s/DFTransformations/%s/%s/transformation.pkl", base_path, name, variant)
}
