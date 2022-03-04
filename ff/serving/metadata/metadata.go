package metadata

import (
	"context"
	"fmt"
	"io"
	"time"

	pb "github.com/featureform/serving/metadata/proto"
	"github.com/featureform/serving/metadata/search"
	"github.com/typesense/typesense-go/typesense"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const TIME_FORMAT = time.RFC1123

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

var parentMapping = map[ResourceType]ResourceType{
	FEATURE_VARIANT:        FEATURE,
	LABEL_VARIANT:          LABEL,
	TRANSFORMATION_VARIANT: TRANSFORMATION,
	SOURCE_VARIANT:         SOURCE,
	TRAINING_SET_VARIANT:   TRAINING_SET,
}

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

func (id ResourceID) Parent() (ResourceID, bool) {
	parentType, has := parentMapping[id.Type]
	if !has {
		return ResourceID{}, false
	}
	return ResourceID{
		Name: id.Name,
		Type: parentType,
	}, true
}

type ResourceNotFound struct {
	ID ResourceID
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

func (err *ResourceNotFound) GRPCStatus() *status.Status {
	return status.New(codes.NotFound, err.Error())
}

type ResourceExists struct {
	ID ResourceID
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

func (err *ResourceExists) GRPCStatus() *status.Status {
	return status.New(codes.AlreadyExists, err.Error())
}

type Resource interface {
	Notify(ResourceLookup, operation, Resource) error
	ID() ResourceID
	Dependencies(ResourceLookup) (ResourceLookup, error)
	Proto() proto.Message
}

func isDirectDependency(lookup ResourceLookup, dependency, parent Resource) (bool, error) {
	depId := dependency.ID()
	deps, depsErr := parent.Dependencies(lookup)
	if depsErr != nil {
		return false, depsErr
	}
	return deps.Has(depId)
}

type ResourceLookup interface {
	Lookup(ResourceID) (Resource, error)
	Has(ResourceID) (bool, error)
	Set(ResourceID, Resource) error
	Submap([]ResourceID) (ResourceLookup, error)
	ListForType(ResourceType) ([]Resource, error)
	List() ([]Resource, error)
}

type TypesenseWrapper struct {
	Client *typesense.Client
	ResourceLookup
}

func (wrapper TypesenseWrapper) Set(id ResourceID, res Resource) error {
	err := wrapper.ResourceLookup.Set(id, res)
	if err != nil {
		return err
	}
	client := wrapper.Client
	_, errUpsert := client.Collection("resource").Documents().Upsert(id)
	if errUpsert != nil {
		return errUpsert
	}
	return nil
}

type localResourceLookup map[ResourceID]Resource

func (lookup localResourceLookup) Lookup(id ResourceID) (Resource, error) {
	res, has := lookup[id]
	if !has {
		return nil, &ResourceNotFound{id}
	}
	return res, nil
}

func (lookup localResourceLookup) Has(id ResourceID) (bool, error) {
	_, has := lookup[id]
	return has, nil
}

func (lookup localResourceLookup) Set(id ResourceID, res Resource) error {
	lookup[id] = res
	return nil
}

func (lookup localResourceLookup) Submap(ids []ResourceID) (ResourceLookup, error) {
	resources := make(localResourceLookup, len(ids))
	for _, id := range ids {
		resource, has := lookup[id]
		if !has {
			return nil, &ResourceNotFound{id}
		}
		resources[id] = resource
	}
	return resources, nil
}

func (lookup localResourceLookup) ListForType(t ResourceType) ([]Resource, error) {
	resources := make([]Resource, 0)
	for id, res := range lookup {
		if id.Type == t {
			resources = append(resources, res)
		}
	}
	return resources, nil
}

func (lookup localResourceLookup) List() ([]Resource, error) {
	resources := make([]Resource, 0, len(lookup))
	for _, res := range lookup {
		resources = append(resources, res)
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

func (resource *sourceResource) Dependencies(lookup ResourceLookup) (ResourceLookup, error) {
	name := resource.serialized.Name
	deps := make(localResourceLookup)
	for _, variant := range resource.serialized.Variants {
		id := ResourceID{
			Name:    name,
			Variant: variant,
			Type:    SOURCE_VARIANT,
		}
		res, err := lookup.Lookup(id)
		if err != nil {
			return nil, err
		}
		deps[id] = res
	}
	return deps, nil
}

func (resource *sourceResource) Proto() proto.Message {
	return resource.serialized
}

func (this *sourceResource) Notify(lookup ResourceLookup, op operation, that Resource) error {
	otherId := that.ID()
	isVariant := otherId.Type == SOURCE_VARIANT && otherId.Name == this.serialized.Name
	if !isVariant {
		return nil
	}
	this.serialized.Variants = append(this.serialized.Variants, otherId.Variant)
	return nil
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

func (resource *sourceVariantResource) Dependencies(lookup ResourceLookup) (ResourceLookup, error) {
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
			Name: serialized.Name,
			Type: SOURCE,
		},
	}
	deps, err := lookup.Submap(depIds)
	if err != nil {
		return nil, err
	}
	return deps, nil
}

func (resource *sourceVariantResource) Proto() proto.Message {
	return resource.serialized
}

func (this *sourceVariantResource) Notify(lookup ResourceLookup, op operation, that Resource) error {
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
	return nil
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

func (resource *featureResource) Dependencies(lookup ResourceLookup) (ResourceLookup, error) {
	name := resource.serialized.Name
	deps := make(localResourceLookup)
	for _, variant := range resource.serialized.Variants {
		id := ResourceID{
			Name:    name,
			Variant: variant,
			Type:    FEATURE_VARIANT,
		}
		res, err := lookup.Lookup(id)
		if err != nil {
			return nil, err
		}
		deps[id] = res
	}
	return deps, nil
}

func (resource *featureResource) Proto() proto.Message {
	return resource.serialized
}

func (this *featureResource) Notify(lookup ResourceLookup, op operation, that Resource) error {
	otherId := that.ID()
	isVariant := otherId.Type == FEATURE_VARIANT && otherId.Name == this.serialized.Name
	if !isVariant {
		return nil
	}
	this.serialized.Variants = append(this.serialized.Variants, otherId.Variant)
	return nil
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

func (resource *featureVariantResource) Dependencies(lookup ResourceLookup) (ResourceLookup, error) {
	serialized := resource.serialized
	depIds := []ResourceID{
		{
			Name:    serialized.Source.Name,
			Variant: serialized.Source.Variant,
			Type:    SOURCE_VARIANT,
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
		{
			Name: serialized.Name,
			Type: FEATURE,
		},
	}
	deps, err := lookup.Submap(depIds)
	if err != nil {
		return nil, err
	}
	return deps, nil
}

func (resource *featureVariantResource) Proto() proto.Message {
	return resource.serialized
}

func (this *featureVariantResource) Notify(lookup ResourceLookup, op operation, that Resource) error {
	id := that.ID()
	releventOp := op == create_op && id.Type == TRAINING_SET_VARIANT
	if !releventOp {
		return nil
	}
	key := id.Proto()
	this.serialized.Trainingsets = append(this.serialized.Trainingsets, key)
	return nil
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

func (resource *labelResource) Dependencies(lookup ResourceLookup) (ResourceLookup, error) {
	name := resource.serialized.Name
	deps := make(localResourceLookup)
	for _, variant := range resource.serialized.Variants {
		id := ResourceID{
			Name:    name,
			Variant: variant,
			Type:    LABEL_VARIANT,
		}
		res, err := lookup.Lookup(id)
		if err != nil {
			return nil, err
		}
		deps[id] = res
	}
	return deps, nil
}

func (resource *labelResource) Proto() proto.Message {
	return resource.serialized
}

func (this *labelResource) Notify(lookup ResourceLookup, op operation, that Resource) error {
	otherId := that.ID()
	isVariant := otherId.Type == LABEL_VARIANT && otherId.Name == this.serialized.Name
	if !isVariant {
		return nil
	}
	this.serialized.Variants = append(this.serialized.Variants, otherId.Variant)
	return nil
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

func (resource *labelVariantResource) Dependencies(lookup ResourceLookup) (ResourceLookup, error) {
	serialized := resource.serialized
	depIds := []ResourceID{
		{
			Name:    serialized.Source.Name,
			Variant: serialized.Source.Variant,
			Type:    SOURCE_VARIANT,
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
		{
			Name: serialized.Name,
			Type: LABEL,
		},
	}
	deps, err := lookup.Submap(depIds)
	if err != nil {
		return nil, err
	}
	return deps, nil
}

func (resource *labelVariantResource) Proto() proto.Message {
	return resource.serialized
}

func (this *labelVariantResource) Notify(lookup ResourceLookup, op operation, that Resource) error {
	id := that.ID()
	releventOp := op == create_op && id.Type == TRAINING_SET_VARIANT
	if !releventOp {
		return nil
	}
	key := id.Proto()
	this.serialized.Trainingsets = append(this.serialized.Trainingsets, key)
	return nil
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

func (resource *trainingSetResource) Dependencies(lookup ResourceLookup) (ResourceLookup, error) {
	name := resource.serialized.Name
	deps := make(localResourceLookup)
	for _, variant := range resource.serialized.Variants {
		id := ResourceID{
			Name:    name,
			Variant: variant,
			Type:    TRAINING_SET_VARIANT,
		}
		res, err := lookup.Lookup(id)
		if err != nil {
			return nil, err
		}
		deps[id] = res
	}
	return deps, nil
}

func (resource *trainingSetResource) Proto() proto.Message {
	return resource.serialized
}

func (this *trainingSetResource) Notify(lookup ResourceLookup, op operation, that Resource) error {
	otherId := that.ID()
	isVariant := otherId.Type == TRAINING_SET_VARIANT && otherId.Name == this.serialized.Name
	if !isVariant {
		return nil
	}
	this.serialized.Variants = append(this.serialized.Variants, otherId.Variant)
	return nil
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

func (resource *trainingSetVariantResource) Dependencies(lookup ResourceLookup) (ResourceLookup, error) {
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
		{
			Name: serialized.Name,
			Type: TRAINING_SET,
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
		return nil, err
	}
	return deps, nil
}

func (resource *trainingSetVariantResource) Proto() proto.Message {
	return resource.serialized
}

func (this *trainingSetVariantResource) Notify(lookup ResourceLookup, op operation, that Resource) error {
	return nil
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

func (resource *modelResource) Dependencies(lookup ResourceLookup) (ResourceLookup, error) {
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
		return nil, err
	}
	return deps, nil
}

func (resource *modelResource) Proto() proto.Message {
	return resource.serialized
}

func (this *modelResource) Notify(lookup ResourceLookup, op operation, that Resource) error {
	return nil
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

func (resource *userResource) Dependencies(lookup ResourceLookup) (ResourceLookup, error) {
	return make(localResourceLookup), nil
}

func (resource *userResource) Proto() proto.Message {
	return resource.serialized
}

func (this *userResource) Notify(lookup ResourceLookup, op operation, that Resource) error {
	userId := this.ID()
	deps, depsErr := that.Dependencies(lookup)
	if depsErr != nil {
		return depsErr
	}
	_, lookupErr := deps.Lookup(userId)
	if lookupErr != nil {
		return lookupErr
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
	return nil
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

func (resource *providerResource) Dependencies(lookup ResourceLookup) (ResourceLookup, error) {
	return make(localResourceLookup), nil
}

func (resource *providerResource) Proto() proto.Message {
	return resource.serialized
}

func (this *providerResource) Notify(lookup ResourceLookup, op operation, that Resource) error {
	if isDep, err := isDirectDependency(lookup, this, that); err != nil {
		return err
	} else if !isDep {
		return nil
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
	return nil
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

func (resource *entityResource) Dependencies(lookup ResourceLookup) (ResourceLookup, error) {
	return make(localResourceLookup), nil
}

func (resource *entityResource) Proto() proto.Message {
	return resource.serialized
}

func (this *entityResource) Notify(lookup ResourceLookup, op operation, that Resource) error {
	if isDep, err := isDirectDependency(lookup, this, that); err != nil {
		return err
	} else if !isDep {
		return nil
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
	return nil
}

type MetadataServer struct {
	lookup ResourceLookup
	Logger *zap.SugaredLogger
	pb.UnimplementedMetadataServer
}

func NewMetadataServer(server *TypeSense) (*MetadataServer, error) {
	server.Logger.Debug("Creating new metadata server")
	if server.Params == nil {
		return &MetadataServer{
			lookup: make(localResourceLookup),
			Logger: server.Logger,
		}, nil
	} else {
		server.Logger.Debug("Creating new typesense metadata server")
		client := typesense.NewClient(
			typesense.WithServer(fmt.Sprintf("http://%s:%s", server.Params.Host, server.Params.Port)),
			typesense.WithAPIKey(server.Params.ApiKey))
		server.Logger.Debugf("Creating typsense client on http://%s:%s with apiKey %s", server.Params.Host, server.Params.Port, server.Params.ApiKey)
		_, err3 := client.Collection("resource").Retrieve()
		if err3 != nil {
			errsch := search.MakeSchema(client)
			if errsch != nil {
				return nil, errsch
			}
			server.Logger.Debug("Creating typsense schema")
		}
		errInit := search.InitializeCollection(client)
		if errInit != nil {
			return nil, errInit
		}
		return &MetadataServer{
			lookup: TypesenseWrapper{
				Client:         client,
				ResourceLookup: make(localResourceLookup),
			},
			Logger: server.Logger,
		}, nil
	}
}

type TypeSenseParams struct {
	Host   string
	Port   string
	ApiKey string
}

type TypeSense struct {
	Logger *zap.SugaredLogger
	Params *TypeSenseParams
}

func (serv *MetadataServer) ListFeatures(_ *pb.Empty, stream pb.Metadata_ListFeaturesServer) error {
	return serv.genericList(FEATURE, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.Feature))
	})
}

func (serv *MetadataServer) CreateFeatureVariant(ctx context.Context, variant *pb.FeatureVariant) (*pb.Empty, error) {
	variant.Created = time.Now().Format(TIME_FORMAT)
	return serv.genericCreate(ctx, &featureVariantResource{variant}, func(name, variant string) Resource {
		return &featureResource{
			&pb.Feature{
				Name:           name,
				DefaultVariant: variant,
				// This will be set when the change is propogated to dependencies.
				Variants: []string{},
			},
		}
	})
}

func (serv *MetadataServer) GetFeatures(stream pb.Metadata_GetFeaturesServer) error {
	return serv.genericGet(stream, FEATURE, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.Feature))
	})
}

func (serv *MetadataServer) GetFeatureVariants(stream pb.Metadata_GetFeatureVariantsServer) error {
	return serv.genericGet(stream, FEATURE_VARIANT, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.FeatureVariant))
	})
}

func (serv *MetadataServer) ListLabels(_ *pb.Empty, stream pb.Metadata_ListLabelsServer) error {
	return serv.genericList(LABEL, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.Label))
	})
}

func (serv *MetadataServer) CreateLabelVariant(ctx context.Context, variant *pb.LabelVariant) (*pb.Empty, error) {
	variant.Created = time.Now().Format(TIME_FORMAT)
	return serv.genericCreate(ctx, &labelVariantResource{variant}, func(name, variant string) Resource {
		return &labelResource{
			&pb.Label{
				Name:           name,
				DefaultVariant: variant,
				// This will be set when the change is propogated to dependencies.
				Variants: []string{},
			},
		}
	})
}

func (serv *MetadataServer) GetLabels(stream pb.Metadata_GetLabelsServer) error {
	return serv.genericGet(stream, LABEL, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.Label))
	})
}

func (serv *MetadataServer) GetLabelVariants(stream pb.Metadata_GetLabelVariantsServer) error {
	return serv.genericGet(stream, LABEL_VARIANT, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.LabelVariant))
	})
}

func (serv *MetadataServer) ListTrainingSets(_ *pb.Empty, stream pb.Metadata_ListTrainingSetsServer) error {
	return serv.genericList(TRAINING_SET, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.TrainingSet))
	})
}

func (serv *MetadataServer) CreateTrainingSetVariant(ctx context.Context, variant *pb.TrainingSetVariant) (*pb.Empty, error) {
	variant.Created = time.Now().Format(TIME_FORMAT)
	return serv.genericCreate(ctx, &trainingSetVariantResource{variant}, func(name, variant string) Resource {
		return &trainingSetResource{
			&pb.TrainingSet{
				Name:           name,
				DefaultVariant: variant,
				// This will be set when the change is propogated to dependencies.
				Variants: []string{},
			},
		}
	})
}

func (serv *MetadataServer) GetTrainingSets(stream pb.Metadata_GetTrainingSetsServer) error {
	return serv.genericGet(stream, TRAINING_SET, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.TrainingSet))
	})
}

func (serv *MetadataServer) GetTrainingSetVariants(stream pb.Metadata_GetTrainingSetVariantsServer) error {
	return serv.genericGet(stream, TRAINING_SET_VARIANT, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.TrainingSetVariant))
	})
}

func (serv *MetadataServer) ListSources(_ *pb.Empty, stream pb.Metadata_ListSourcesServer) error {
	return serv.genericList(SOURCE, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.Source))
	})
}

func (serv *MetadataServer) CreateSourceVariant(ctx context.Context, variant *pb.SourceVariant) (*pb.Empty, error) {
	variant.Created = time.Now().Format(TIME_FORMAT)
	return serv.genericCreate(ctx, &sourceVariantResource{variant}, func(name, variant string) Resource {
		return &sourceResource{
			&pb.Source{
				Name:           name,
				DefaultVariant: variant,
				// This will be set when the change is propogated to dependencies.
				Variants: []string{},
			},
		}
	})
}

func (serv *MetadataServer) GetSources(stream pb.Metadata_GetSourcesServer) error {
	return serv.genericGet(stream, SOURCE, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.Source))
	})
}

func (serv *MetadataServer) GetSourceVariants(stream pb.Metadata_GetSourceVariantsServer) error {
	return serv.genericGet(stream, SOURCE_VARIANT, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.SourceVariant))
	})
}

func (serv *MetadataServer) ListUsers(_ *pb.Empty, stream pb.Metadata_ListUsersServer) error {
	return serv.genericList(USER, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.User))
	})
}

func (serv *MetadataServer) CreateUser(ctx context.Context, user *pb.User) (*pb.Empty, error) {
	return serv.genericCreate(ctx, &userResource{user}, nil)
}

func (serv *MetadataServer) GetUsers(stream pb.Metadata_GetUsersServer) error {
	return serv.genericGet(stream, USER, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.User))
	})
}

func (serv *MetadataServer) ListProviders(_ *pb.Empty, stream pb.Metadata_ListProvidersServer) error {
	return serv.genericList(PROVIDER, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.Provider))
	})
}

func (serv *MetadataServer) CreateProvider(ctx context.Context, provider *pb.Provider) (*pb.Empty, error) {
	return serv.genericCreate(ctx, &providerResource{provider}, nil)
}

func (serv *MetadataServer) GetProviders(stream pb.Metadata_GetProvidersServer) error {
	return serv.genericGet(stream, PROVIDER, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.Provider))
	})
}

func (serv *MetadataServer) ListEntities(_ *pb.Empty, stream pb.Metadata_ListEntitiesServer) error {
	return serv.genericList(ENTITY, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.Entity))
	})
}

func (serv *MetadataServer) CreateEntity(ctx context.Context, entity *pb.Entity) (*pb.Empty, error) {
	return serv.genericCreate(ctx, &entityResource{entity}, nil)
}

func (serv *MetadataServer) GetEntities(stream pb.Metadata_GetEntitiesServer) error {
	return serv.genericGet(stream, ENTITY, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.Entity))
	})
}

func (serv *MetadataServer) ListModels(_ *pb.Empty, stream pb.Metadata_ListModelsServer) error {
	return serv.genericList(MODEL, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.Model))
	})
}

func (serv *MetadataServer) CreateModel(ctx context.Context, model *pb.Model) (*pb.Empty, error) {
	return serv.genericCreate(ctx, &modelResource{model}, nil)
}

func (serv *MetadataServer) GetModels(stream pb.Metadata_GetModelsServer) error {
	return serv.genericGet(stream, MODEL, func(msg proto.Message) error {
		return stream.Send(msg.(*pb.Model))
	})
}

type nameStream interface {
	Recv() (*pb.Name, error)
}

type variantStream interface {
	Recv() (*pb.NameVariant, error)
}

type sendFn func(proto.Message) error

type initParentFn func(name, variant string) Resource

func (serv *MetadataServer) genericCreate(ctx context.Context, res Resource, init initParentFn) (*pb.Empty, error) {
	id := res.ID()
	if has, err := serv.lookup.Has(id); err != nil {
		return nil, err
	} else if has {
		return nil, &ResourceExists{id}
	}
	if err := serv.lookup.Set(id, res); err != nil {
		return nil, err
	}
	parentId, hasParent := id.Parent()
	if hasParent {
		if parentExists, err := serv.lookup.Has(parentId); err != nil {
			return nil, err
		} else if !parentExists {
			parent := init(id.Name, id.Variant)
			if err := serv.lookup.Set(parentId, parent); err != nil {
				return nil, err
			}
		}
	}
	if err := serv.propogateChange(res); err != nil {
		return nil, err
	}
	return &pb.Empty{}, nil
}

func (serv *MetadataServer) propogateChange(newRes Resource) error {
	visited := make(map[ResourceID]struct{})
	// We have to make it a var so that the anonymous function can call itself.
	var propogateChange func(parent Resource) error
	propogateChange = func(parent Resource) error {
		deps, err := parent.Dependencies(serv.lookup)
		if err != nil {
			return err
		}
		depList, err := deps.List()
		if err != nil {
			return err
		}
		for _, res := range depList {
			id := res.ID()
			if _, has := visited[id]; has {
				continue
			}
			visited[id] = struct{}{}
			if err := res.Notify(serv.lookup, create_op, newRes); err != nil {
				return err
			}
			if err := propogateChange(res); err != nil {
				return err
			}
		}
		return nil
	}
	return propogateChange(newRes)
}

func (serv *MetadataServer) genericGet(stream interface{}, t ResourceType, send sendFn) error {
	for {
		var recvErr error
		var id ResourceID
		switch casted := stream.(type) {
		case nameStream:
			req, err := casted.Recv()
			recvErr = err
			id = ResourceID{
				Name: req.GetName(),
				Type: t,
			}
		case variantStream:
			req, err := casted.Recv()
			recvErr = err
			id = ResourceID{
				Name:    req.GetName(),
				Variant: req.GetVariant(),
				Type:    t,
			}
		default:
			return fmt.Errorf("Invalid Stream for Get: %T", casted)
		}
		if recvErr == io.EOF {
			return nil
		}
		if recvErr != nil {
			return recvErr
		}
		resource, err := serv.lookup.Lookup(id)
		if err != nil {
			return err
		}
		serialized := resource.Proto()
		if err := send(serialized); err != nil {
			return err
		}
	}
}

func (serv *MetadataServer) genericList(t ResourceType, send sendFn) error {
	resources, err := serv.lookup.ListForType(t)
	if err != nil {
		return err
	}
	for _, res := range resources {
		serialized := res.Proto()
		if err := send(serialized); err != nil {
			return err
		}
	}
	return nil
}
