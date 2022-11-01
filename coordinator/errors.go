package coordinator

import (
	"fmt"
	"github.com/featureform/metadata"
)

type JobDoesNotExistError struct {
	key string
}

func (m JobDoesNotExistError) Error() string {
	return fmt.Sprintf("Coordinator Job No Longer Exists: %s", m.key)
}

type ResourceAlreadyCompleteError struct {
	resourceID metadata.ResourceID
}

func (m ResourceAlreadyCompleteError) Error() string {
	return fmt.Sprintf("resource already in a complete state: %s %s %s", m.resourceID.Type, m.resourceID.Name, m.resourceID.Variant)
}

type ResourceAlreadyFailedError struct {
	resourceID metadata.ResourceID
}

func (m ResourceAlreadyFailedError) Error() string {
	return fmt.Sprintf("resource failed in a previous run: %s %s %s", m.resourceID.Type, m.resourceID.Name, m.resourceID.Variant)
}
