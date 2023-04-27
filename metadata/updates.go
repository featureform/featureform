package metadata

import (
	pb "github.com/featureform/metadata/proto"
)

func unionNameVariants(destination, source []*pb.NameVariant) []*pb.NameVariant {
	type nameVariantKey struct {
		Name    string
		Variant string
	}

	set := make(map[nameVariantKey]bool)

	for _, nameVariant := range destination {
		key := nameVariantKey{nameVariant.Name, nameVariant.Variant}
		set[key] = true
	}

	for _, nameVariant := range source {
		key := nameVariantKey{nameVariant.Name, nameVariant.Variant}
		if _, has := set[key]; !has {
			destination = append(destination, nameVariant)
		}
	}

	return destination
}

func unionTags(destination, source *pb.Tags) *pb.Tags {
	set := make(map[string]bool)

	for _, tag := range destination.GetTag() {
		set[tag] = true
	}

	for _, tag := range source.GetTag() {
		if _, has := set[tag]; !has {
			destination.Tag = append(destination.Tag, tag)
		}
	}

	return destination
}

func mergeProperties(destination, source *pb.Properties) *pb.Properties {
	if destination == nil {
		destination = &pb.Properties{}
	}
	if destination.Property == nil {
		destination.Property = map[string]*pb.Property{}
	}
	for key, value := range source.GetProperty() {
		destination.Property[key] = value
	}
	return destination
}
