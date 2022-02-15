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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type NameVariant struct {
	Name    string
	Variant string
}

type operation int

const (
	create_op operation = iota
)

type ResourceType string

const (
	FEATURE                ResourceType = "Feature"
	FEATURE_VARIANT                     = "Feature variant"
	LABEL                               = "Label"
	LABEL_VARIANT                       = "Label variant"
	USER                                = "User"
	ENTITY                              = "Entity"
	TRANSFORMATION                      = "Transformation"
	TRANSFORMATION_VARIANT              = "Transformation variant"
	PROVIDER                            = "Provider"
	SOURCE                              = "Source"
	SOURCE_VARIANT                      = "Source variant"
	TRAINING_SET                        = "Training Set"
	TRAINING_SET_VARIANT                = "Training Set variant"
	MODEL                               = "Model"
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

type ResourceNotFound struct {
	ID ResourceID
}

func (err ResourceNotFound) WrapGRPC() error {
	return status.Error(codes.NotFound, err.Error())
}

func (err *ResourceNotFound) Error() string {
	id := err.ID
	name, variant, t := id.Name, id.Variant, id.Type
	errMsg := fmt.Sprintf("%s Not Found.\nName: %s", t, name)
	if variant != "" {
		errMsg += "\nVariant: " + variant
	}
	return errMsg
}

type ResourceExists struct {
	ID ResourceID
}

func (err ResourceExists) WrapGRPC() error {
	return status.Error(codes.AlreadyExists, err.Error())
}

func (err *ResourceExists) Error() string {
	id := err.ID
	name, variant, t := id.Name, id.Variant, id.Type
	errMsg := fmt.Sprintf("%s Exists.\nName: %s", t, name)
	if variant != "" {
		errMsg += "\nVariant: " + variant
	}
	return errMsg
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
		{
			Name: serialized.Provider,
			Type: PROVIDER,
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
	serialized := resource.serialized
	depIds := []ResourceID{
		{
			Name: serialized.Source,
			Type: SOURCE_VARIANT,
		},
		{
			Name: serialized.Entity,
			Type: ENTITY,
		},
		{
			Name: serialized.Owner,
			Type: USER,
		},
		{
			Name: serialized.Provider,
			Type: PROVIDER,
		},
	}
	deps, err := lookup.Submap(depIds)
	if err != nil {
		panic(err)
	}
	return deps
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

type labelResource struct {
	serialized *pb.Label
}

func (resource *labelResource) ID() ResourceID {
	return ResourceID{
		Name: resource.serialized.Name,
		Type: LABEL,
	}
}

func (resource *labelResource) Dependencies(lookup ResourceLookup) ResourceLookup {
	name := resource.serialized.Name
	deps := make(ResourceLookup)
	for _, variant := range resource.serialized.Variants {
		id := ResourceID{
			Name:    name,
			Variant: variant,
			Type:    LABEL_VARIANT,
		}
		deps[id] = lookup[id]
	}
	return deps
}

func (resource *labelResource) Proto() interface{} {
	return resource.serialized
}

func (this *labelResource) Notify(lookup ResourceLookup, op operation, that Resource) {
	otherId := that.ID()
	isVariant := otherId.Type == LABEL_VARIANT && otherId.Name == this.serialized.Name
	if !isVariant {
		return
	}
	this.serialized.Variants = append(this.serialized.Variants, otherId.Variant)
}

type labelVariantResource struct {
	serialized *pb.LabelVariant
}

func (resource *labelVariantResource) ID() ResourceID {
	return ResourceID{
		Name:    resource.serialized.Name,
		Variant: resource.serialized.Variant,
		Type:    LABEL_VARIANT,
	}
}

func (resource *labelVariantResource) Dependencies(lookup ResourceLookup) ResourceLookup {
	serialized := resource.serialized
	depIds := []ResourceID{
		{
			Name: serialized.Source,
			Type: SOURCE_VARIANT,
		},
		{
			Name: serialized.Entity,
			Type: ENTITY,
		},
		{
			Name: serialized.Owner,
			Type: USER,
		},
		{
			Name: serialized.Provider,
			Type: PROVIDER,
		},
	}
	deps, err := lookup.Submap(depIds)
	if err != nil {
		panic(err)
	}
	return deps
}

func (resource *labelVariantResource) Proto() interface{} {
	return resource.serialized
}

func (this *labelVariantResource) Notify(lookup ResourceLookup, op operation, that Resource) {
	id := that.ID()
	releventOp := op == create_op && id.Type == TRAINING_SET_VARIANT
	if !releventOp {
		return
	}
	key := id.Proto()
	this.serialized.Trainingsets = append(this.serialized.Trainingsets, key)
}

type trainingSetResource struct {
	serialized *pb.TrainingSet
}

func (resource *trainingSetResource) ID() ResourceID {
	return ResourceID{
		Name: resource.serialized.Name,
		Type: TRAINING_SET,
	}
}

func (resource *trainingSetResource) Dependencies(lookup ResourceLookup) ResourceLookup {
	name := resource.serialized.Name
	deps := make(ResourceLookup)
	for _, variant := range resource.serialized.Variants {
		id := ResourceID{
			Name:    name,
			Variant: variant,
			Type:    TRAINING_SET_VARIANT,
		}
		deps[id] = lookup[id]
	}
	return deps
}

func (resource *trainingSetResource) Proto() interface{} {
	return resource.serialized
}

func (this *trainingSetResource) Notify(lookup ResourceLookup, op operation, that Resource) {
	otherId := that.ID()
	isVariant := otherId.Type == TRAINING_SET_VARIANT && otherId.Name == this.serialized.Name
	if !isVariant {
		return
	}
	this.serialized.Variants = append(this.serialized.Variants, otherId.Variant)
}

type trainingSetVariantResource struct {
	serialized *pb.TrainingSetVariant
}

func (resource *trainingSetVariantResource) ID() ResourceID {
	return ResourceID{
		Name:    resource.serialized.Name,
		Variant: resource.serialized.Variant,
		Type:    TRAINING_SET_VARIANT,
	}
}

func (resource *trainingSetVariantResource) Dependencies(lookup ResourceLookup) ResourceLookup {
	serialized := resource.serialized
	depIds := []ResourceID{
		{
			Name: serialized.Owner,
			Type: USER,
		},
		{
			Name: serialized.Provider,
			Type: PROVIDER,
		},
		{
			Name:    serialized.Label.Name,
			Variant: serialized.Label.Variant,
			Type:    LABEL_VARIANT,
		},
	}
	for _, feature := range serialized.Features {
		depIds = append(depIds, ResourceID{
			Name:    feature.Name,
			Variant: feature.Variant,
			Type:    FEATURE_VARIANT,
		})
	}
	deps, err := lookup.Submap(depIds)
	if err != nil {
		panic(err)
	}
	return deps
}

func (resource *trainingSetVariantResource) Proto() interface{} {
	return resource.serialized
}

func (this *trainingSetVariantResource) Notify(lookup ResourceLookup, op operation, that Resource) {
	// Purposely empty.
}

type modelResource struct {
	serialized *pb.Model
}

func (resource *modelResource) ID() ResourceID {
	return ResourceID{
		Name: resource.serialized.Name,
		Type: MODEL,
	}
}

func (resource *modelResource) Dependencies(lookup ResourceLookup) ResourceLookup {
	serialized := resource.serialized
	depIds := make([]ResourceID, 0)
	for _, feature := range serialized.Features {
		depIds = append(depIds, ResourceID{
			Name:    feature.Name,
			Variant: feature.Variant,
			Type:    FEATURE_VARIANT,
		})
	}
	for _, label := range serialized.Labels {
		depIds = append(depIds, ResourceID{
			Name:    label.Name,
			Variant: label.Variant,
			Type:    LABEL_VARIANT,
		})
	}
	for _, ts := range serialized.Trainingsets {
		depIds = append(depIds, ResourceID{
			Name:    ts.Name,
			Variant: ts.Variant,
			Type:    TRAINING_SET_VARIANT,
		})
	}
	deps, err := lookup.Submap(depIds)
	if err != nil {
		panic(err)
	}
	return deps
}

func (resource *modelResource) Proto() interface{} {
	return resource.serialized
}

func (this *modelResource) Notify(lookup ResourceLookup, op operation, that Resource) {
	// Purposely empty.
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
	case SOURCE_VARIANT:
		serialized.Sources = append(serialized.Sources, key)
	}
}

type providerResource struct {
	serialized *pb.Provider
}

func (resource *providerResource) ID() ResourceID {
	return ResourceID{
		Name: resource.serialized.Name,
		Type: PROVIDER,
	}
}

func (resource *providerResource) Dependencies(lookup ResourceLookup) ResourceLookup {
	return make(ResourceLookup)
}

func (resource *providerResource) Proto() interface{} {
	return resource.serialized
}

func (this *providerResource) Notify(lookup ResourceLookup, op operation, that Resource) {
	providerId := this.ID()
	_, providerOwns := that.Dependencies(lookup)[providerId]
	if !providerOwns {
		return
	}
	id := that.ID()
	key := id.Proto()
	t := id.Type
	serialized := this.serialized
	switch t {
	case SOURCE_VARIANT:
		serialized.Sources = append(serialized.Sources, key)
	case FEATURE_VARIANT:
		serialized.Features = append(serialized.Features, key)
	case TRAINING_SET_VARIANT:
		serialized.Trainingsets = append(serialized.Trainingsets, key)
	case LABEL_VARIANT:
		serialized.Labels = append(serialized.Labels, key)
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

const TIME_FORMAT = time.RFC1123

type MetadataServer struct {
	features []string
	lookup   ResourceLookup
	Logger   *zap.SugaredLogger
	pb.UnimplementedMetadataServer
}

func NewMetadataServer(logger *zap.SugaredLogger) (*MetadataServer, error) {
	logger.Debug("Creating new metadata server")
	return &MetadataServer{
		features: make([]string, 0),
		lookup:   make(ResourceLookup),
		Logger:   logger,
	}, nil
}

func (serv *MetadataServer) ListFeatures(_ *pb.Empty, stream pb.Metadata_ListFeaturesServer) error {
	for _, name := range serv.features {
		id := ResourceID{
			Name: name,
			Type: FEATURE,
		}
		feature := serv.lookup[id].Proto().(*pb.Feature)
		if err := stream.Send(feature); err != nil {
			return err
		}
	}
	return nil
}

func (serv *MetadataServer) CreateFeatureVariant(ctx context.Context, variant *pb.FeatureVariant) (*pb.Empty, error) {
	name, variantName := variant.GetName(), variant.GetVariant()
	id := ResourceID{
		Name:    name,
		Variant: variantName,
		Type:    FEATURE_VARIANT,
	}
	if _, has := serv.lookup[id]; has {
		return nil, ResourceExists{id}.WrapGRPC()
	}
	variant.Created = time.Now().Format(TIME_FORMAT)
	parentId := ResourceID{
		Name: name,
		Type: FEATURE,
	}
	_, parentExists := serv.lookup[parentId]
	if !parentExists {
		feature := &pb.Feature{
			Name:           name,
			DefaultVariant: variantName,
			// This will be set when the change is propogated to dependencies.
			Variants: []string{},
		}
		resource := &featureResource{feature}
		serv.lookup[parentId] = resource
		serv.features = append(serv.features, name)
	}
	serv.lookup[id] = &featureVariantResource{variant}
	// TODO verify dependencies, propogate change
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
		id := ResourceID{
			Name: name,
			Type: FEATURE,
		}
		resource, has := serv.lookup[id]
		if !has {
			return ResourceNotFound{id}.WrapGRPC()
		}
		feature := resource.Proto().(*pb.Feature)
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
		name, variant := req.GetName(), req.GetVariant()
		id := ResourceID{
			Name:    name,
			Variant: variant,
			Type:    FEATURE_VARIANT,
		}
		resource, has := serv.lookup[id]
		if !has {
			return ResourceNotFound{id}.WrapGRPC()
		}
		serialized := resource.Proto().(*pb.FeatureVariant)
		if err := stream.Send(serialized); err != nil {
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
