package provider

import (
	"github.com/featureform/serving/metadata"
)

type OfflineResourceType int

const (
	None OfflineResourceType = iota
	Label
	Feature
)

type ResourceDef struct {
	ResourceID
	HasTS bool
}

type ResourceID struct {
	Name, Variant string
	Type          OfflineResourceType
}

type OfflineStore interface {
	CreateResourceTable(ResourceDef) (OfflineTable, error)
	GetResourceTable(id ResourceID) (OfflineTable, error)
}

type ResourceRecord struct {
	Entity string
	Value  interface{}
	TS     time.Time
}

type OfflineTable interface {
	Write(ResourceRecord) error
}
