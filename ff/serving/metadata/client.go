package main

import (
	"context"
	"fmt"
	"io"
	"time"

	pb "github.com/featureform/serving/metadata/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const TIME_FORMAT = time.RFC1123

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

func parseNameVariant(serialized *pb.NameVariant) NameVariant {
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

func parseNameVariants(protos []*pb.NameVariant) NameVariants {
	parsed := make([]NameVariant, len(protos))
	for i, serialized := range protos {
		parsed[i] = parseNameVariant(serialized)
	}
	return parsed
}

type Client struct {
	Logger   *zap.SugaredLogger
	conn     *grpc.ClientConn
	grpcConn pb.MetadataClient
}

func (client *Client) ListFeatures(ctx context.Context) ([]*Feature, error) {
	stream, err := client.grpcConn.ListFeatures(ctx, &pb.Empty{})
	if err != nil {
		return nil, err
	}
	return client.parseFeatureStream(stream)
}

func (client *Client) GetFeatures(ctx context.Context, features []string) ([]*Feature, error) {
	stream, err := client.grpcConn.GetFeatures(ctx)
	if err != nil {
		return nil, err
	}
	go func() {
		for _, feature := range features {
			stream.Send(&pb.Name{Name: feature})
		}
	}()
	return client.parseFeatureStream(stream)
}

func (client *Client) GetFeatureVariants(ctx context.Context, ids []NameVariant) ([]*FeatureVariant, error) {
	stream, err := client.grpcConn.GetFeatureVariants(ctx)
	if err != nil {
		return nil, err
	}
	go func() {
		for _, id := range ids {
			stream.Send(&pb.NameVariant{Name: id.Name, Variant: id.Variant})
		}
	}()
	return client.parseFeatureVariantStream(stream)
}

func (client *Client) GetFeatureVariant(ctx context.Context, id NameVariant) (*FeatureVariant, error) {
	stream, err := client.grpcConn.GetFeatureVariants(ctx)
	if err != nil {
		return nil, err
	}
	go stream.Send(id.Serialize())
	variants, err := client.parseFeatureVariantStream(stream)
    if err != nil {
        return nil, err
    }
    return variants[0], nil
}

type FeatureDef struct {
	Name        string
	Variant     string
	Source      string
	Type        string
	Entity      string
	Owner       string
	Description string
	Provider    string
}

func (client *Client) CreateFeatureVariant(ctx context.Context, def FeatureDef) error {
	serialized := &pb.FeatureVariant{
		Name:        def.Name,
		Variant:     def.Variant,
		Source:      def.Source,
		Type:        def.Type,
		Entity:      def.Entity,
		Owner:       def.Owner,
		Description: def.Description,
		Provider:    def.Provider,
	}
	_, err := client.grpcConn.CreateFeatureVariant(ctx, serialized)
	return err
}

type featureStream interface {
	Recv() (*pb.Feature, error)
}

func (client *Client) parseFeatureStream(stream featureStream) ([]*Feature, error) {
	features := make([]*Feature, 0)
	for {
		serial, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		features = append(features, wrapProtoFeature(serial))
	}
	return features, nil
}

type featureVariantStream interface {
	Recv() (*pb.FeatureVariant, error)
}

func (client *Client) parseFeatureVariantStream(stream featureVariantStream) ([]*FeatureVariant, error) {
	features := make([]*FeatureVariant, 0)
	for {
		serial, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		features = append(features, wrapProtoFeatureVariant(serial))
	}
	return features, nil
}

func (client *Client) ListLabels(ctx context.Context) ([]*Label, error) {
	stream, err := client.grpcConn.ListLabels(ctx, &pb.Empty{})
	if err != nil {
		return nil, err
	}
	return client.parseLabelStream(stream)
}

func (client *Client) GetLabels(ctx context.Context, labels []string) ([]*Label, error) {
	stream, err := client.grpcConn.GetLabels(ctx)
	if err != nil {
		return nil, err
	}
	go func() {
		for _, label := range labels {
			stream.Send(&pb.Name{Name: label})
		}
	}()
	return client.parseLabelStream(stream)
}

type LabelDef struct {
	Name        string
	Variant     string
	Description string
	Type        string
	Source      string
	Entity      string
	Owner       string
	Provider    string
}

func (client *Client) CreateLabelVariant(ctx context.Context, def LabelDef) error {
	serialized := &pb.LabelVariant{
		Name:        def.Name,
		Variant:     def.Variant,
		Description: def.Description,
		Type:        def.Type,
		Source:      def.Source,
		Entity:      def.Entity,
		Owner:       def.Owner,
		Provider:    def.Provider,
	}
	_, err := client.grpcConn.CreateLabelVariant(ctx, serialized)
	return err
}

func (client *Client) GetLabelVariants(ctx context.Context, ids []NameVariant) ([]*LabelVariant, error) {
	stream, err := client.grpcConn.GetLabelVariants(ctx)
	if err != nil {
		return nil, err
	}
	go func() {
		for _, id := range ids {
			stream.Send(&pb.NameVariant{Name: id.Name, Variant: id.Variant})
		}
	}()
	return client.parseLabelVariantStream(stream)
}

func (client *Client) GetLabelVariant(ctx context.Context, id NameVariant) (*LabelVariant, error) {
	stream, err := client.grpcConn.GetLabelVariants(ctx)
	if err != nil {
		return nil, err
	}
	go stream.Send(id.Serialize())
	variants, err := client.parseLabelVariantStream(stream)
    if err != nil {
        return nil, err
    }
    return variants[0], nil
}

type labelStream interface {
	Recv() (*pb.Label, error)
}

func (client *Client) parseLabelStream(stream labelStream) ([]*Label, error) {
	labels := make([]*Label, 0)
	for {
		serial, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		labels = append(labels, wrapProtoLabel(serial))
	}
	return labels, nil
}

type labelVariantStream interface {
	Recv() (*pb.LabelVariant, error)
}

func (client *Client) parseLabelVariantStream(stream labelVariantStream) ([]*LabelVariant, error) {
	features := make([]*LabelVariant, 0)
	for {
		serial, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		features = append(features, wrapProtoLabelVariant(serial))
	}
	return features, nil
}

func (client *Client) ListTrainingSets(ctx context.Context) ([]*TrainingSet, error) {
	stream, err := client.grpcConn.ListTrainingSets(ctx, &pb.Empty{})
	if err != nil {
		return nil, err
	}
	return client.parseTrainingSetStream(stream)
}

func (client *Client) GetTrainingSets(ctx context.Context, trainingSets []string) ([]*TrainingSet, error) {
	stream, err := client.grpcConn.GetTrainingSets(ctx)
	if err != nil {
		return nil, err
	}
	go func() {
		for _, trainingSet := range trainingSets {
			stream.Send(&pb.Name{Name: trainingSet})
		}
	}()
	return client.parseTrainingSetStream(stream)
}

type TrainingSetDef struct {
	Name        string
	Variant     string
	Description string
	Owner       string
	Provider    string
	Label       NameVariant
	Features    NameVariants
}

func (client *Client) CreateTrainingSetVariant(ctx context.Context, def TrainingSetDef) error {
	serialized := &pb.TrainingSetVariant{
		Name:        def.Name,
		Variant:     def.Variant,
		Description: def.Description,
		Owner:       def.Owner,
		Provider:    def.Provider,
		Label:       def.Label.Serialize(),
		Features:    def.Features.Serialize(),
	}
	_, err := client.grpcConn.CreateTrainingSetVariant(ctx, serialized)
	return err
}

func (client *Client) GetTrainingSetVariants(ctx context.Context, ids []NameVariant) ([]*TrainingSetVariant, error) {
	stream, err := client.grpcConn.GetTrainingSetVariants(ctx)
	if err != nil {
		return nil, err
	}
	go func() {
		for _, id := range ids {
			stream.Send(&pb.NameVariant{Name: id.Name, Variant: id.Variant})
		}
	}()
	return client.parseTrainingSetVariantStream(stream)
}

type trainingSetStream interface {
	Recv() (*pb.TrainingSet, error)
}

func (client *Client) parseTrainingSetStream(stream trainingSetStream) ([]*TrainingSet, error) {
	trainingSets := make([]*TrainingSet, 0)
	for {
		serial, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		trainingSets = append(trainingSets, wrapProtoTrainingSet(serial))
	}
	return trainingSets, nil
}

type trainingSetVariantStream interface {
	Recv() (*pb.TrainingSetVariant, error)
}

func (client *Client) parseTrainingSetVariantStream(stream trainingSetVariantStream) ([]*TrainingSetVariant, error) {
	features := make([]*TrainingSetVariant, 0)
	for {
		serial, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		features = append(features, wrapProtoTrainingSetVariant(serial))
	}
	return features, nil
}

func (client *Client) ListSources(ctx context.Context) ([]*Source, error) {
	stream, err := client.grpcConn.ListSources(ctx, &pb.Empty{})
	if err != nil {
		return nil, err
	}
	return client.parseSourceStream(stream)
}

func (client *Client) GetSources(ctx context.Context, sources []string) ([]*Source, error) {
	stream, err := client.grpcConn.GetSources(ctx)
	if err != nil {
		return nil, err
	}
	go func() {
		for _, source := range sources {
			stream.Send(&pb.Name{Name: source})
		}
	}()
	return client.parseSourceStream(stream)
}

type SourceDef struct {
	Name        string
	Variant     string
	Description string
	Type        string
	Owner       string
	Provider    string
}

func (client *Client) CreateSourceVariant(ctx context.Context, def SourceDef) error {
	serialized := &pb.SourceVariant{
		Name:        def.Name,
		Variant:     def.Variant,
		Description: def.Description,
		Type:        def.Type,
		Owner:       def.Owner,
		Provider:    def.Provider,
	}
	_, err := client.grpcConn.CreateSourceVariant(ctx, serialized)
	return err
}

func (client *Client) GetSourceVariants(ctx context.Context, ids []NameVariant) ([]*SourceVariant, error) {
	stream, err := client.grpcConn.GetSourceVariants(ctx)
	if err != nil {
		return nil, err
	}
	go func() {
		for _, id := range ids {
			stream.Send(&pb.NameVariant{Name: id.Name, Variant: id.Variant})
		}
	}()
	return client.parseSourceVariantStream(stream)
}

func (client *Client) GetSourceVariant(ctx context.Context, id NameVariant) (*SourceVariant, error) {
	stream, err := client.grpcConn.GetSourceVariants(ctx)
	if err != nil {
		return nil, err
	}
	go stream.Send(id.Serialize())
	variants, err := client.parseSourceVariantStream(stream)
    if err != nil {
        return nil, err
    }
    return variants[0], nil
}

type sourceStream interface {
	Recv() (*pb.Source, error)
}

func (client *Client) parseSourceStream(stream sourceStream) ([]*Source, error) {
	sources := make([]*Source, 0)
	for {
		serial, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		sources = append(sources, wrapProtoSource(serial))
	}
	return sources, nil
}

type sourceVariantStream interface {
	Recv() (*pb.SourceVariant, error)
}

func (client *Client) parseSourceVariantStream(stream sourceVariantStream) ([]*SourceVariant, error) {
	features := make([]*SourceVariant, 0)
	for {
		serial, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		features = append(features, wrapProtoSourceVariant(serial))
	}
	return features, nil
}

func (client *Client) ListUsers(ctx context.Context) ([]*User, error) {
	stream, err := client.grpcConn.ListUsers(ctx, &pb.Empty{})
	if err != nil {
		return nil, err
	}
	return client.parseUserStream(stream)
}

func (client *Client) GetUsers(ctx context.Context, users []string) ([]*User, error) {
	stream, err := client.grpcConn.GetUsers(ctx)
	if err != nil {
		return nil, err
	}
	go func() {
		for _, user := range users {
			stream.Send(&pb.Name{Name: user})
		}
	}()
	return client.parseUserStream(stream)
}

type UserDef struct {
	Name string
}

func (client *Client) CreateUser(ctx context.Context, def UserDef) error {
	serialized := &pb.User{
		Name: def.Name,
	}
	_, err := client.grpcConn.CreateUser(ctx, serialized)
	return err
}

type userStream interface {
	Recv() (*pb.User, error)
}

func (client *Client) parseUserStream(stream userStream) ([]*User, error) {
	users := make([]*User, 0)
	for {
		serial, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		users = append(users, wrapProtoUser(serial))
	}
	return users, nil
}

func (client *Client) ListProviders(ctx context.Context) ([]*Provider, error) {
	stream, err := client.grpcConn.ListProviders(ctx, &pb.Empty{})
	if err != nil {
		return nil, err
	}
	return client.parseProviderStream(stream)
}

func (client *Client) GetProviders(ctx context.Context, providers []string) ([]*Provider, error) {
	stream, err := client.grpcConn.GetProviders(ctx)
	if err != nil {
		return nil, err
	}
	go func() {
		for _, provider := range providers {
			stream.Send(&pb.Name{Name: provider})
		}
	}()
	return client.parseProviderStream(stream)
}

type ProviderDef struct {
	Name        string
	Description string
	Type        string
	Software    string
	Team        string
}

func (client *Client) CreateProvider(ctx context.Context, def ProviderDef) error {
	serialized := &pb.Provider{
		Name:        def.Name,
		Description: def.Description,
		Type:        def.Type,
		Software:    def.Software,
		Team:        def.Team,
	}
	_, err := client.grpcConn.CreateProvider(ctx, serialized)
	return err
}

type providerStream interface {
	Recv() (*pb.Provider, error)
}

func (client *Client) parseProviderStream(stream providerStream) ([]*Provider, error) {
	providers := make([]*Provider, 0)
	for {
		serial, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		providers = append(providers, wrapProtoProvider(serial))
	}
	return providers, nil
}

func (client *Client) ListEntities(ctx context.Context) ([]*Entity, error) {
	stream, err := client.grpcConn.ListEntities(ctx, &pb.Empty{})
	if err != nil {
		return nil, err
	}
	return client.parseEntityStream(stream)
}

func (client *Client) GetEntities(ctx context.Context, entities []string) ([]*Entity, error) {
	stream, err := client.grpcConn.GetEntities(ctx)
	if err != nil {
		return nil, err
	}
	go func() {
		for _, entity := range entities {
			stream.Send(&pb.Name{Name: entity})
		}
	}()
	return client.parseEntityStream(stream)
}

type EntityDef struct {
	Name        string
	Description string
}

func (client *Client) CreateEntity(ctx context.Context, def EntityDef) error {
	serialized := &pb.Entity{
		Name:        def.Name,
		Description: def.Description,
	}
	_, err := client.grpcConn.CreateEntity(ctx, serialized)
	return err
}

type entityStream interface {
	Recv() (*pb.Entity, error)
}

func (client *Client) parseEntityStream(stream entityStream) ([]*Entity, error) {
	entities := make([]*Entity, 0)
	for {
		serial, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		entities = append(entities, wrapProtoEntity(serial))
	}
	return entities, nil
}

func (client *Client) ListModels(ctx context.Context) ([]*Model, error) {
	stream, err := client.grpcConn.ListModels(ctx, &pb.Empty{})
	if err != nil {
		return nil, err
	}
	return client.parseModelStream(stream)
}

func (client *Client) GetModels(ctx context.Context, models []string) ([]*Model, error) {
	stream, err := client.grpcConn.GetModels(ctx)
	if err != nil {
		return nil, err
	}
	go func() {
		for _, model := range models {
			stream.Send(&pb.Name{Name: model})
		}
	}()
	return client.parseModelStream(stream)
}

type ModelDef struct {
	Name        string
	Description string
}

func (client *Client) CreateModel(ctx context.Context, def ModelDef) error {
	serialized := &pb.Model{
		Name:        def.Name,
		Description: def.Description,
	}
	_, err := client.grpcConn.CreateModel(ctx, serialized)
	return err
}

type modelStream interface {
	Recv() (*pb.Model, error)
}

func (client *Client) parseModelStream(stream modelStream) ([]*Model, error) {
	models := make([]*Model, 0)
	for {
		serial, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		models = append(models, wrapProtoModel(serial))
	}
	return models, nil
}

type protoStringer struct {
	msg proto.Message
}

func (stringer protoStringer) String() string {
	bytes, err := protojson.Marshal(stringer.msg)
	if err != nil {
		return err.Error()
	}
	return string(bytes)
}

type createdGetter interface {
	GetCreated() string
}

type createdFn struct {
	getter createdGetter
}

func (fn createdFn) Created() time.Time {
	t, err := time.Parse(fn.getter.GetCreated(), TIME_FORMAT)
	if err != nil {
		panic(err)
	}
	return t
}

type variantsDescriber interface {
	GetName() string
	GetDefaultVariant() string
	GetVariants() []string
}

type variantsFns struct {
	getter variantsDescriber
}

func (fns variantsFns) Name() string {
	return fns.getter.GetName()
}

func (fns variantsFns) DefaultVariant() string {
	return fns.getter.GetDefaultVariant()
}

func (fns variantsFns) Variants() []string {
	return fns.getter.GetVariants()
}

func (fns variantsFns) NameVariants() []NameVariant {
	name := fns.getter.GetName()
	variants := fns.getter.GetVariants()
	nameVariants := make([]NameVariant, len(variants))
	for i, variant := range variants {
		nameVariants[i] = NameVariant{
			Name:    name,
			Variant: variant,
		}
	}
	return nameVariants
}

type trainingSetsGetter interface {
	GetTrainingsets() []*pb.NameVariant
}

type fetchTrainingSetsFns struct {
	getter trainingSetsGetter
}

func (fn fetchTrainingSetsFns) TrainingSets() []NameVariant {
	return parseNameVariants(fn.getter.GetTrainingsets())
}

func (fn fetchTrainingSetsFns) FetchTrainingSets(client *Client, ctx context.Context) ([]*TrainingSetVariant, error) {
    return client.GetTrainingSetVariants(ctx, fn.TrainingSets())
}

type labelsGetter interface {
	GetLabels() []*pb.NameVariant
}

type fetchLabelsFns struct {
	getter labelsGetter
}

func (fn fetchLabelsFns) Labels() []NameVariant {
	return parseNameVariants(fn.getter.GetLabels())
}

func (fn fetchLabelsFns) FetchLabels(client *Client, ctx context.Context) ([]*LabelVariant, error) {
    return client.GetLabelVariants(ctx, fn.Labels())
}

type featuresGetter interface {
	GetFeatures() []*pb.NameVariant
}

type fetchFeaturesFns struct {
	getter featuresGetter
}

func (fn fetchFeaturesFns) Features() []NameVariant {
	return parseNameVariants(fn.getter.GetFeatures())
}

func (fn fetchFeaturesFns) FetchFeatures(client *Client, ctx context.Context) ([]*FeatureVariant, error) {
    return client.GetFeatureVariants(ctx, fn.Features())
}

type sourcesGetter interface {
	GetSources() []*pb.NameVariant
}

type fetchSourcesFns struct {
	getter sourcesGetter
}

func (fn fetchSourcesFns) Sources() []NameVariant {
	return parseNameVariants(fn.getter.GetSources())
}

func (fn fetchSourcesFns) FetchSources(client *Client, ctx context.Context) ([]*SourceVariant, error) {
    return client.GetSourceVariants(ctx, fn.Sources())
}

type sourceGetter interface {
	GetSource() *pb.NameVariant
}

type fetchSourceFns struct {
	getter sourceGetter
}

func (fn fetchSourceFns) Source() NameVariant {
	return parseNameVariant(fn.getter.GetSource())
}

func (fn fetchSourceFns) FetchSource(client *Client, ctx context.Context) (*SourceVariant, error) {
    return client.GetSourceVariant(ctx, fn.Source())
}

type Feature struct {
	serialized *pb.Feature
	variantsFns
	protoStringer
}

func wrapProtoFeature(serialized *pb.Feature) *Feature {
	return &Feature{
		serialized:    serialized,
		variantsFns:   variantsFns{serialized},
		protoStringer: protoStringer{serialized},
	}
}

func (feature Feature) FetchVariants(client *Client, ctx context.Context) ([]*FeatureVariant, error) {
    return client.GetFeatureVariants(ctx, feature.NameVariants())
}

type FeatureVariant struct {
	serialized *pb.FeatureVariant
	fetchTrainingSetsFns
	createdFn
	protoStringer
}

func wrapProtoFeatureVariant(serialized *pb.FeatureVariant) *FeatureVariant {
	return &FeatureVariant{
		serialized:           serialized,
		fetchTrainingSetsFns: fetchTrainingSetsFns{serialized},
		createdFn:            createdFn{serialized},
		protoStringer:        protoStringer{serialized},
	}
}

func (variant *FeatureVariant) Name() string {
	return variant.serialized.GetName()
}

func (variant *FeatureVariant) Variant() string {
	return variant.serialized.GetVariant()
}

func (variant *FeatureVariant) Source() string {
	return variant.serialized.GetSource()
}

func (variant *FeatureVariant) Type() string {
	return variant.serialized.GetType()
}

func (variant *FeatureVariant) Entity() string {
	return variant.serialized.GetEntity()
}

func (variant *FeatureVariant) Owner() string {
	return variant.serialized.GetOwner()
}

type User struct {
	serialized *pb.User
	fetchTrainingSetsFns
	fetchFeaturesFns
	fetchLabelsFns
	fetchSourcesFns
	protoStringer
}

func wrapProtoUser(serialized *pb.User) *User {
	return &User{
		serialized:           serialized,
		fetchTrainingSetsFns: fetchTrainingSetsFns{serialized},
		fetchFeaturesFns:     fetchFeaturesFns{serialized},
		fetchLabelsFns:       fetchLabelsFns{serialized},
		fetchSourcesFns:      fetchSourcesFns{serialized},
		protoStringer:        protoStringer{serialized},
	}
}

func (user *User) Name() string {
	return user.serialized.GetName()
}

type Provider struct {
	serialized *pb.Provider
	fetchTrainingSetsFns
	fetchFeaturesFns
	fetchLabelsFns
	fetchSourcesFns
	protoStringer
}

func wrapProtoProvider(serialized *pb.Provider) *Provider {
	return &Provider{
		serialized:           serialized,
		fetchTrainingSetsFns: fetchTrainingSetsFns{serialized},
		fetchFeaturesFns:     fetchFeaturesFns{serialized},
		fetchLabelsFns:       fetchLabelsFns{serialized},
		fetchSourcesFns:      fetchSourcesFns{serialized},
		protoStringer:        protoStringer{serialized},
	}
}

func (provider *Provider) Name() string {
	return provider.serialized.GetName()
}

func (provider *Provider) Description() string {
	return provider.serialized.GetDescription()
}

func (provider *Provider) Type() string {
	return provider.serialized.GetType()
}

func (provider *Provider) Software() string {
	return provider.serialized.GetSoftware()
}

func (provider *Provider) Team() string {
	return provider.serialized.GetTeam()
}

type Model struct {
	serialized *pb.Model
	fetchTrainingSetsFns
	fetchFeaturesFns
	fetchLabelsFns
	protoStringer
}

func wrapProtoModel(serialized *pb.Model) *Model {
	return &Model{
		serialized:           serialized,
		fetchTrainingSetsFns: fetchTrainingSetsFns{serialized},
		fetchFeaturesFns:     fetchFeaturesFns{serialized},
		fetchLabelsFns:       fetchLabelsFns{serialized},
		protoStringer:        protoStringer{serialized},
	}
}

func (model *Model) Name() string {
	return model.serialized.GetName()
}

func (model *Model) Description() string {
	return model.serialized.GetDescription()
}

type Label struct {
	serialized *pb.Label
	variantsFns
	protoStringer
}

func wrapProtoLabel(serialized *pb.Label) *Label {
	return &Label{
		serialized:    serialized,
		variantsFns:   variantsFns{serialized},
		protoStringer: protoStringer{serialized},
	}
}

func (label Label) FetchVariants(client *Client, ctx context.Context) ([]*LabelVariant, error) {
    return client.GetLabelVariants(ctx, label.NameVariants())
}

type LabelVariant struct {
	serialized *pb.LabelVariant
	fetchTrainingSetsFns
	createdFn
	protoStringer
}

func wrapProtoLabelVariant(serialized *pb.LabelVariant) *LabelVariant {
	return &LabelVariant{
		serialized:           serialized,
		fetchTrainingSetsFns: fetchTrainingSetsFns{serialized},
		createdFn:            createdFn{serialized},
		protoStringer:        protoStringer{serialized},
	}
}

func (variant *LabelVariant) Name() string {
	return variant.serialized.GetName()
}

func (variant *LabelVariant) Description() string {
	return variant.serialized.GetDescription()
}

func (variant *LabelVariant) Variant() string {
	return variant.serialized.GetVariant()
}

func (variant *LabelVariant) Source() string {
	return variant.serialized.GetSource()
}

func (variant *LabelVariant) Type() string {
	return variant.serialized.GetType()
}

func (variant *LabelVariant) Entity() string {
	return variant.serialized.GetEntity()
}

func (variant *LabelVariant) Owner() string {
	return variant.serialized.GetOwner()
}

type TrainingSet struct {
	serialized *pb.TrainingSet
	variantsFns
	protoStringer
}

func wrapProtoTrainingSet(serialized *pb.TrainingSet) *TrainingSet {
	return &TrainingSet{
		serialized:    serialized,
		variantsFns:   variantsFns{serialized},
		protoStringer: protoStringer{serialized},
	}
}

func (trainingSet TrainingSet) FetchVariants(client *Client, ctx context.Context) ([]*TrainingSetVariant, error) {
    return client.GetTrainingSetVariants(ctx, trainingSet.NameVariants())
}

type TrainingSetVariant struct {
	serialized *pb.TrainingSetVariant
	fetchFeaturesFns
	createdFn
	protoStringer
}

func wrapProtoTrainingSetVariant(serialized *pb.TrainingSetVariant) *TrainingSetVariant {
	return &TrainingSetVariant{
		serialized:       serialized,
		fetchFeaturesFns: fetchFeaturesFns{serialized},
		createdFn:        createdFn{serialized},
		protoStringer:    protoStringer{serialized},
	}
}

func (variant *TrainingSetVariant) Name() string {
	return variant.serialized.GetName()
}

func (variant *TrainingSetVariant) Description() string {
	return variant.serialized.GetDescription()
}

func (variant *TrainingSetVariant) Variant() string {
	return variant.serialized.GetVariant()
}

func (variant *TrainingSetVariant) Provider() string {
	return variant.serialized.GetProvider()
}

func (variant *TrainingSetVariant) Owner() string {
	return variant.serialized.GetOwner()
}

func (variant *TrainingSetVariant) Label() NameVariant {
	return parseNameVariant(variant.serialized.GetLabel())
}

func (variant *TrainingSetVariant) FetchLabel(client *Client, ctx context.Context) (*LabelVariant, error) {
    labelList, err := client.GetLabelVariants(ctx, []NameVariant{variant.Label()})
    if err != nil {
        return nil, err
    }
    return labelList[0], nil
}

type Source struct {
	serialized *pb.Source
	variantsFns
	protoStringer
}

func wrapProtoSource(serialized *pb.Source) *Source {
	return &Source{
		serialized:    serialized,
		variantsFns:   variantsFns{serialized},
		protoStringer: protoStringer{serialized},
	}
}

func (source Source) FetchVariants(client *Client, ctx context.Context) ([]*SourceVariant, error) {
    return client.GetSourceVariants(ctx, source.NameVariants())
}

type SourceVariant struct {
	serialized *pb.SourceVariant
	fetchTrainingSetsFns
	fetchFeaturesFns
	fetchLabelsFns
	createdFn
	protoStringer
}

func wrapProtoSourceVariant(serialized *pb.SourceVariant) *SourceVariant {
	return &SourceVariant{
		serialized:           serialized,
		fetchTrainingSetsFns: fetchTrainingSetsFns{serialized},
		fetchFeaturesFns:     fetchFeaturesFns{serialized},
		fetchLabelsFns:       fetchLabelsFns{serialized},
		createdFn:            createdFn{serialized},
		protoStringer:        protoStringer{serialized},
	}
}

func (variant *SourceVariant) Name() string {
	return variant.serialized.GetName()
}

func (variant *SourceVariant) Variant() string {
	return variant.serialized.GetVariant()
}

func (variant *SourceVariant) Description() string {
	return variant.serialized.GetDescription()
}

func (variant *SourceVariant) Type() string {
	return variant.serialized.GetType()
}

func (variant *SourceVariant) Provider() string {
	return variant.serialized.GetProvider()
}

func (variant *SourceVariant) Owner() string {
	return variant.serialized.GetOwner()
}

type Entity struct {
	serialized *pb.Entity
	fetchTrainingSetsFns
	fetchFeaturesFns
	fetchLabelsFns
	protoStringer
}

func wrapProtoEntity(serialized *pb.Entity) *Entity {
	return &Entity{
		serialized:           serialized,
		fetchTrainingSetsFns: fetchTrainingSetsFns{serialized},
		fetchFeaturesFns:     fetchFeaturesFns{serialized},
		fetchLabelsFns:       fetchLabelsFns{serialized},
		protoStringer:        protoStringer{serialized},
	}
}

func (entity *Entity) Name() string {
	return entity.serialized.GetName()
}

func (entity *Entity) Description() string {
	return entity.serialized.GetDescription()
}

func NewClient(host string, logger *zap.SugaredLogger) (*Client, error) {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	conn, err := grpc.Dial(host, opts...)
	if err != nil {
		return nil, err
	}
	client := pb.NewMetadataClient(conn)
	return &Client{
		Logger:   logger,
		conn:     conn,
		grpcConn: client,
	}, nil
}

func (client *Client) Close() {
	client.conn.Close()
}

func main() {
	logger := zap.NewExample().Sugar()
	client, err := NewClient("localhost:8080", logger)
	if err != nil {
		logger.Panicw("Failed to connect", "Err", err)
	}
	err = client.CreateEntity(context.Background(), EntityDef{
		Name:        "f1",
		Description: "desc",
	})
	if err != nil {
		logger.Panicw("Failed to create entity", "Err", err)
	}
	entities, err := client.ListEntities(context.Background())
	fmt.Printf("%+v\n", entities)
	logger.Infow("Listed Entities", "Entities", entities, "Err", err)
	err = client.CreateUser(context.Background(), UserDef{
		Name: "f1",
	})
	if err != nil {
		logger.Panicw("Failed to create user", "Err", err)
	}
	users, err := client.ListUsers(context.Background())
	fmt.Printf("%+v\n", users)
	logger.Infow("Listed Users", "Users", users, "Err", err)
	err = client.CreateFeatureVariant(context.Background(), FeatureDef{
		Name:        "f1",
		Variant:     "v1",
		Source:      "Users",
		Type:        "int",
		Entity:      "users",
		Owner:       "simba",
		Description: "Our first feature",
	})
	if err != nil {
		logger.Panicw("Failed to create feature", "Err", err)
	}
	features, err := client.ListFeatures(context.Background())
	fmt.Printf("%+v\n", features)
	logger.Infow("Listed features", "Features", features, "Err", err)
}
