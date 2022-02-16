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

func parseNameVariants(protos []*pb.NameVariant) []NameVariant {
	parsed := make([]NameVariant, len(protos))
	for i, serialized := range protos {
		parsed[i] = NameVariant{
			Name:    serialized.Name,
			Variant: serialized.Variant,
		}
	}
	return parsed
}

type Client struct {
	Logger   *zap.SugaredLogger
	conn     *grpc.ClientConn
	grpcConn pb.MetadataClient
}

func (client *Client) ListFeatures(ctx context.Context) ([]Feature, error) {
	stream, err := client.grpcConn.ListFeatures(ctx, &pb.Empty{})
	if err != nil {
		return nil, err
	}
	return client.parseFeatureStream(stream)
}

func (client *Client) GetFeatures(ctx context.Context, features []string) ([]Feature, error) {
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

func (client *Client) parseFeatureStream(stream featureStream) ([]Feature, error) {
	features := make([]Feature, 0)
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

func (client *Client) ListLabels(ctx context.Context) ([]Label, error) {
	stream, err := client.grpcConn.ListLabels(ctx, &pb.Empty{})
	if err != nil {
		return nil, err
	}
	return client.parseLabelStream(stream)
}

func (client *Client) GetLabels(ctx context.Context, labels []string) ([]Label, error) {
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

type labelStream interface {
	Recv() (*pb.Label, error)
}

func (client *Client) parseLabelStream(stream labelStream) ([]Label, error) {
	labels := make([]Label, 0)
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

func (client *Client) ListUsers(ctx context.Context) ([]User, error) {
	stream, err := client.grpcConn.ListUsers(ctx, &pb.Empty{})
	if err != nil {
		return nil, err
	}
	return client.parseUserStream(stream)
}

func (client *Client) GetUsers(ctx context.Context, users []string) ([]User, error) {
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

func (client *Client) parseUserStream(stream userStream) ([]User, error) {
	users := make([]User, 0)
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

func (client *Client) ListEntities(ctx context.Context) ([]Entity, error) {
	stream, err := client.grpcConn.ListEntities(ctx, &pb.Empty{})
	if err != nil {
		return nil, err
	}
	return client.parseEntityStream(stream)
}

func (client *Client) GetEntities(ctx context.Context, entities []string) ([]Entity, error) {
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

func (client *Client) parseEntityStream(stream entityStream) ([]Entity, error) {
	entities := make([]Entity, 0)
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

func (fn createdFn) Created() string {
	t, err := time.Parse(variant.serialized.GetCreated(), TIME_FORMAT)
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

func (fn fetchTrainingSetsFns) FetchTrainingSets() []TrainingSetVariant {
	// TODO
	return nil
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

func (fn fetchLabelsFns) FetchLabels() []LabelVariant {
	// TODO
	return nil
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

func (fn fetchFeaturesFns) FetchFeatures() []FeatureVariant {
	// TODO
	return nil
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

func (fn fetchSourcesFns) FetchSources() []SourceVariant {
	// TODO
	return nil
}

type Feature struct {
	serialized *pb.Feature
	variantsFns
	protoStringer
}

func wrapProtoFeature(serialized *pb.Feature) Feature {
	return Feature{
		serialized:    serialized,
		variantsFns:   variantsFns{serialized},
		protoStringer: protoStringer{serialized},
	}
}

func (feature Feature) FetchVariants() []FeatureVariant {
	// TODO
	return nil
}

type FeatureVariant struct {
	serialized *pb.FeatureVariant
	fetchTrainingSetsFns
	createdFn
	protoStringer
}

func wrapProtoFeatureVariant(serialized *pb.FeatureVariant) FeatureVariant {
	return FeatureVariant{
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

func wrapProtoUser(serialized *pb.User) User {
	return User{
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

type Label struct {
	serialized *pb.Label
	variantsFns
	protoStringer
}

func wrapProtoLabel(serialized *pb.Label) Label {
	return Label{
		serialized:    serialized,
		variantsFns:   variantsFns{serialized},
		protoStringer: protoStringer{serialized},
	}
}

func (label Label) FetchVariants() []LabelVariant {
	// TODO
	return nil
}

type LabelVariant struct {
	serialized *pb.LabelVariant
	fetchTrainingSetsFns
	createdFn
	protoStringer
}

func wrapProtoLabelVariant(serialized *pb.LabelVariant) LabelVariant {
	return LabelVariant{
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
	// TODO
}

type TrainingSetVariant struct {
	// TODO
}

type Source struct {
	// TODO
}

type SourceVariant struct {
	// TODO
}

type Entity struct {
	serialized *pb.Entity
	fetchTrainingSetsFns
	fetchFeaturesFns
	fetchLabelsFns
	protoStringer
}

func wrapProtoEntity(serialized *pb.Entity) Entity {
	return Entity{
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
