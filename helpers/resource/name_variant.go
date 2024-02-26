package resource

import (
	"fmt"
	pb "github.com/featureform/metadata/proto"
)

type NameVariant struct {
	Name    string
	Variant string
}

func (variant NameVariant) Serialize() *pb.NameVariant {
	return &pb.NameVariant{
		Name:    variant.Name,
		Variant: variant.Variant,
	}
}

func (variant NameVariant) ClientString() string {
	return fmt.Sprintf("%s.%s", variant.Name, variant.Variant)
}

func ParseNameVariant(serialized *pb.NameVariant) NameVariant {
	return NameVariant{
		Name:    serialized.Name,
		Variant: serialized.Variant,
	}
}

type NameVariants []NameVariant

func (variants NameVariants) Serialize() []*pb.NameVariant {
	serialized := make([]*pb.NameVariant, len(variants))
	for i, variant := range variants {
		serialized[i] = variant.Serialize()
	}
	return serialized
}

func ParseNameVariants(protos []*pb.NameVariant) NameVariants {
	parsed := make([]NameVariant, len(protos))
	for i, serialized := range protos {
		parsed[i] = ParseNameVariant(serialized)
	}
	return parsed
}

func (variants NameVariants) Names() []string {
	names := make([]string, len(variants))
	for i, variant := range variants {
		names[i] = variant.Name
	}
	return names
}

type Tags []string

type Properties map[string]string

func (properties Properties) Serialize() *pb.Properties {
	serialized := &pb.Properties{
		Property: map[string]*pb.Property{},
	}

	for key, val := range properties {
		serialized.Property[key] = &pb.Property{Value: &pb.Property_StringValue{StringValue: val}}
	}

	return serialized
}
