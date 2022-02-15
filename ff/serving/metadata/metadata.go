package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"time"

	pb "github.com/featureform/serving/metadata/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type NameVariant struct {
	Name    string
	Variant string
}

// IDEA resources interface with a notify call.

type operation int

const (
	create_op operation = iota
)

type ResourceType int

const (
	FEATURE ResourceType = iota
	FEATURE_VARIANT
	LABEL
	LABEL_VARIANT
	USER
	ENTITY
	TRANSFORMATION
	TRANSFORMATION_VARIANT
	PROIVIDER
	SOURCE
	SOURCE_VARIANT
	TRAINING_SET
	TRAINING_SET_VARIANT
)

type ResourceID struct {
	Name    string
	Variant string
	Type    ResourceType
}

func (id ResourceID) Proto() *pb.NameVariant {
	return &pb.NameVariant{
		Name:    id.Name,
		Variant: id.Variant,
	}
}

type Resource interface {
	Notify(ResourceLookup, operation, Resource)
	ID() ResourceID
	Dependencies(ResourceLookup) ResourceLookup
	Proto() interface{}
}

type ResourceLookup map[ResourceID]Resource

func (lookup ResourceLookup) Submap(ids []ResourceID) (ResourceLookup, error) {
	resources := make(ResourceLookup, len(ids))
	for _, id := range ids {
		resource, has := lookup[id]
		if !has {
			return nil, fmt.Errorf("Resource not found: %v", id)
		}
		resources[id] = resource
	}
	return resources, nil
}

func (lookup ResourceLookup) LookupAll(ids []ResourceID) ([]Resource, error) {
	resources := make([]Resource, len(ids))
	for i, id := range ids {
		resource, has := lookup[id]
		if !has {
			return nil, fmt.Errorf("Resource not found: %v", id)
		}
		resources[i] = resource
	}
	return resources, nil
}

type sourceResource struct {
	serialized *pb.Source
}

func (resource *sourceResource) ID() ResourceID {
	return ResourceID{
		Name: resource.serialized.Name,
		Type: SOURCE,
	}
}

func (resource *sourceResource) Dependencies(lookup ResourceLookup) ResourceLookup {
	name := resource.serialized.Name
	deps := make(ResourceLookup)
	for _, variant := range resource.serialized.Variants {
		id := ResourceID{
			Name:    name,
			Variant: variant,
			Type:    SOURCE_VARIANT,
		}
		deps[id] = lookup[id]
	}
	return deps
}

func (resource *sourceResource) Proto() interface{} {
	return resource.serialized
}

func (this *sourceResource) Notify(lookup ResourceLookup, op operation, that Resource) {
	otherId := that.ID()
	isVariant := otherId.Type == SOURCE_VARIANT && otherId.Name == this.serialized.Name
	if !isVariant {
		return
	}
	this.serialized.Variants = append(this.serialized.Variants, otherId.Variant)
}

type sourceVariantResource struct {
	serialized *pb.SourceVariant
}

func (resource *sourceVariantResource) ID() ResourceID {
	return ResourceID{
		Name:    resource.serialized.Name,
		Variant: resource.serialized.Variant,
		Type:    SOURCE_VARIANT,
	}
}

func (resource *sourceVariantResource) Dependencies(lookup ResourceLookup) ResourceLookup {
	serialized := resource.serialized
	depIds := []ResourceID{
		{
			Name: serialized.Owner,
			Type: USER,
		},
	}
	deps, err := lookup.Submap(depIds)
	if err != nil {
		panic(err)
	}
	return deps
}

func (resource *sourceVariantResource) Proto() interface{} {
	return resource.serialized
}

func (this *sourceVariantResource) Notify(lookup ResourceLookup, op operation, that Resource) {
	id := that.ID()
	t := id.Type
	key := id.Proto()
	serialized := this.serialized
	switch t {
	case TRAINING_SET_VARIANT:
		serialized.Trainingsets = append(serialized.Trainingsets, key)
	case FEATURE_VARIANT:
		serialized.Features = append(serialized.Features, key)
	case LABEL_VARIANT:
		serialized.Labels = append(serialized.Labels, key)
	}
}

type featureResource struct {
	serialized *pb.Feature
}

func (resource *featureResource) ID() ResourceID {
	return ResourceID{
		Name: resource.serialized.Name,
		Type: FEATURE,
	}
}

func (resource *featureResource) Dependencies(lookup ResourceLookup) ResourceLookup {
	name := resource.serialized.Name
	deps := make(ResourceLookup)
	for _, variant := range resource.serialized.Variants {
		id := ResourceID{
			Name:    name,
			Variant: variant,
			Type:    FEATURE_VARIANT,
		}
		deps[id] = lookup[id]
	}
	return deps
}

func (resource *featureResource) Proto() interface{} {
	return resource.serialized
}

func (this *featureResource) Notify(lookup ResourceLookup, op operation, that Resource) {
	otherId := that.ID()
	isVariant := otherId.Type == FEATURE_VARIANT && otherId.Name == this.serialized.Name
	if !isVariant {
		return
	}
	this.serialized.Variants = append(this.serialized.Variants, otherId.Variant)
}

type featureVariantResource struct {
	serialized *pb.FeatureVariant
}

func (resource *featureVariantResource) ID() ResourceID {
	return ResourceID{
		Name:    resource.serialized.Name,
		Variant: resource.serialized.Variant,
		Type:    FEATURE_VARIANT,
	}
}

func (resource *featureVariantResource) Dependencies(lookup ResourceLookup) ResourceLookup {
	return nil
}

func (resource *featureVariantResource) Proto() interface{} {
	return resource.serialized
}

func (this *featureVariantResource) Notify(lookup ResourceLookup, op operation, that Resource) {
	id := that.ID()
	releventOp := op == create_op && id.Type == TRAINING_SET_VARIANT
	if !releventOp {
		return
	}
	key := id.Proto()
	this.serialized.Trainingsets = append(this.serialized.Trainingsets, key)
}

type userResource struct {
	serialized *pb.User
}

func (resource *userResource) ID() ResourceID {
	return ResourceID{
		Name: resource.serialized.Name,
		Type: USER,
	}
}

func (resource *userResource) Dependencies(lookup ResourceLookup) ResourceLookup {
	return make(ResourceLookup)
}

func (resource *userResource) Proto() interface{} {
	return resource.serialized
}

func (this *userResource) Notify(lookup ResourceLookup, op operation, that Resource) {
	userId := this.ID()
	_, userOwns := that.Dependencies(lookup)[userId]
	if !userOwns {
		return
	}
	id := that.ID()
	key := id.Proto()
	t := id.Type
	serialized := this.serialized
	switch t {
	case TRAINING_SET_VARIANT:
		serialized.Trainingsets = append(serialized.Trainingsets, key)
	case FEATURE_VARIANT:
		serialized.Features = append(serialized.Features, key)
	case LABEL_VARIANT:
		serialized.Labels = append(serialized.Labels, key)
	case SOURCE:
		serialized.Sources = append(serialized.Sources, key)
	}
}

type entityResource struct {
	serialized *pb.Entity
}

func (resource *entityResource) ID() ResourceID {
	return ResourceID{
		Name: resource.serialized.Name,
		Type: ENTITY,
	}
}

func (resource *entityResource) Dependencies(lookup ResourceLookup) ResourceLookup {
	return make(ResourceLookup)
}

func (resource *entityResource) Proto() interface{} {
	return resource.serialized
}

func (this *entityResource) Notify(lookup ResourceLookup, op operation, that Resource) {
	entityId := this.ID()
	_, hasEntity := that.Dependencies(lookup)[entityId]
	if !hasEntity {
		return
	}
	id := that.ID()
	key := id.Proto()
	t := id.Type
	serialized := this.serialized
	switch t {
	case TRAINING_SET_VARIANT:
		serialized.Trainingsets = append(serialized.Trainingsets, key)
	case FEATURE_VARIANT:
		serialized.Features = append(serialized.Features, key)
	case LABEL_VARIANT:
		serialized.Labels = append(serialized.Labels, key)
	}
}

type MetadataServer struct {
	features        map[string]*pb.Feature
	featureVariants map[NameVariant]*pb.FeatureVariant
	Logger          *zap.SugaredLogger
	pb.UnimplementedMetadataServer
}

func NewMetadataServer(logger *zap.SugaredLogger) (*MetadataServer, error) {
	logger.Debug("Creating new metadata server")
	return &MetadataServer{
		features:        make(map[string]*pb.Feature),
		featureVariants: make(map[NameVariant]*pb.FeatureVariant),
		Logger:          logger,
	}, nil
}

func (serv *MetadataServer) ListFeatures(_ *pb.Empty, stream pb.Metadata_ListFeaturesServer) error {
	for _, feature := range serv.features {
		if err := stream.Send(feature); err != nil {
			return err
		}
	}
	return nil
}

func (serv *MetadataServer) CreateFeatureVariant(ctx context.Context, variant *pb.FeatureVariant) (*pb.Empty, error) {
	name, variantName := variant.GetName(), variant.GetVariant()
	variantKey := NameVariant{name, variantName}
	if _, has := serv.featureVariants[variantKey]; has {
		return nil, fmt.Errorf("Variant already exists")
	}
	feature, has := serv.features[name]
	variant.Created = time.Now().Format(time.RFC1123)
	if has {
		feature.Variants = append(feature.Variants, variantName)
	} else {
		serv.features[name] = &pb.Feature{
			Name:           name,
			DefaultVariant: variantName,
			Variants:       []string{variantName},
		}
	}
	serv.featureVariants[variantKey] = variant
	return &pb.Empty{}, nil
}

func (serv *MetadataServer) GetFeatures(stream pb.Metadata_GetFeaturesServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		name := req.GetName()
		feature, has := serv.features[name]
		if !has {
			return fmt.Errorf("Feature %s not found", name)
		}
		if err := stream.Send(feature); err != nil {
			return err
		}
	}
}

func (serv *MetadataServer) GetFeatureVariants(stream pb.Metadata_GetFeatureVariantsServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		name, variantName := req.GetName(), req.GetVariant()
		key := NameVariant{name, variantName}
		variant, has := serv.featureVariants[key]
		if !has {
			return fmt.Errorf("FeatureVariant %s %s not found", name, variant)
		}
		if err := stream.Send(variant); err != nil {
			return err
		}
	}
}

func main() {
	logger := zap.NewExample().Sugar()
	port := ":8080"
	lis, err := net.Listen("tcp", port)
	if err != nil {
		logger.Panicw("Failed to listen on port", "Err", err)
	}
	grpcServer := grpc.NewServer()
	serv, err := NewMetadataServer(logger)
	if err != nil {
		logger.Panicw("Failed to create metadata server", "Err", err)
	}
	pb.RegisterMetadataServer(grpcServer, serv)
	logger.Infow("Server starting", "Port", port)
	serveErr := grpcServer.Serve(lis)
	if serveErr != nil {
		logger.Errorw("Serve failed with error", "Err", serveErr)
	}
}
