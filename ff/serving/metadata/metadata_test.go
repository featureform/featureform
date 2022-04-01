package metadata

import (
	"context"
	"errors"
	pb "github.com/featureform/serving/metadata/proto"
	"go.uber.org/zap"
	grpc "google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"testing"
)

func TestMethodsEmpty(t *testing.T) {
	lookup := make(localResourceLookup)
	testId := ResourceID{
		Name:    "Fraud",
		Variant: "default",
		Type:    ENTITY,
	}
	if _, err := lookup.Lookup(testId); err == nil {
		t.Fatalf("Failed to return a Lookup error because %s", err)
	}
	containsId, err := lookup.Has(testId)
	if err != nil {
		t.Fatalf("Failed to run Has function %s", err)
	}
	if containsId == true {
		t.Fatalf("Failed to return false in Has function for the empty case")
	}
	listAll, err := lookup.List()
	if err != nil {
		t.Fatalf("Failed to list all resources prior to setting a resource %s", err)
	}
	if len(listAll) > 0 {
		t.Fatalf("Failed to get the correct number of results %d instead of 0", len(listAll))
	}
	listByType, errListForType := lookup.ListForType(ENTITY)
	if errListForType != nil {
		t.Fatalf("Failed to list resources (by type) prior to setting a resource %s", errListForType)
	}
	if len(listByType) > 0 {
		t.Fatalf("Failed to get the correct number of results %d instead of 0", len(listByType))
	}
}

type mockResource struct {
	id ResourceID
}

func (res *mockResource) ID() ResourceID {
	return res.id
}

func (res *mockResource) Dependencies(ResourceLookup) (ResourceLookup, error) {
	return nil, nil
}

func (res *mockResource) Notify(ResourceLookup, operation, Resource) error {
	return nil
}

func (res *mockResource) Proto() proto.Message {
	return res.id.Proto()
}

func TestMethodsOneResource(t *testing.T) {
	lookup := make(localResourceLookup)
	testId := ResourceID{
		Name:    "Fraud",
		Variant: "default",
		Type:    SOURCE,
	}
	v := mockResource{
		id: testId,
	}
	if err := lookup.Set(testId, &v); err != nil {
		t.Fatalf("Failed to set resource %s", err)
	}
	if _, err := lookup.Lookup(testId); err != nil {
		t.Fatalf("Failed to return a Lookup error because %s", err)
	}
	containsId, err := lookup.Has(testId)
	if err != nil {
		t.Fatalf("Failed to run Has function %s", err)
	}
	if containsId == false {
		t.Fatalf("Failed to return true in Has function for the 1 resource case")
	}
	listAll, err := lookup.List()
	if err != nil {
		t.Fatalf("Failed to list all resources for the 1 resource case %s", err)
	}
	if len(listAll) != 1 {
		t.Fatalf("Failed to get the correct number of results %d instead of 1", len(listAll))
	}
	listByType, errListForType := lookup.ListForType(SOURCE)
	if errListForType != nil {
		t.Fatalf("Failed to list resources (by type) for the 1 resource case %s", errListForType)
	}
	if len(listByType) != 1 {
		t.Fatalf("Failed to get the correct number of results, %d instead of 1", len(listByType))
	}

}

func TestMethodsThreeResource(t *testing.T) {
	lookup := make(localResourceLookup)
	testIds := []ResourceID{
		{
			Name:    "Fraud",
			Variant: "default",
			Type:    PROVIDER,
		}, {
			Name:    "Time",
			Variant: "second",
			Type:    FEATURE,
		}, {
			Name:    "Ice-Cream",
			Variant: "third-variant",
			Type:    MODEL,
		},
	}
	for _, resource := range testIds {
		if err := lookup.Set(resource, &mockResource{id: resource}); err != nil {
			t.Fatalf("Failed to Set %s", err)
		}
	}
	if _, err := lookup.Lookup(testIds[0]); err != nil {
		t.Fatalf("Failed to return a Lookup error because %s", err)
	}
	containsId, err := lookup.Has(testIds[1])
	if err != nil {
		t.Fatalf("Failed to run Has function %s", err)
	}
	if containsId == false {
		t.Fatalf("Failed to return true in Has function for the 3 resource case")
	}
	listAll, err := lookup.List()
	if err != nil {
		t.Fatalf("Failed to list all resources for the 3 resource case %s", err)
	}
	if len(listAll) != 3 {
		t.Fatalf("Failed to get the correct number of results, %d instead of 3", len(listAll))
	}
	listByType, errListForType := lookup.ListForType(FEATURE)
	if errListForType != nil {
		t.Fatalf("Failed to list resources (by type) for the 1 resource case %s", errListForType)
	}
	if len(listByType) != 1 {
		t.Fatalf("Failed to get the correct number of results %d instead of 1", len(listByType))
	}
	res, errSubmap := lookup.Submap(testIds)
	if errSubmap != nil {
		t.Fatalf("Failed to create a submap %s", errSubmap)
	}
	listsubmap, errList := res.List()
	if errList != nil {
		t.Fatalf("Failed to get list of submap %s", errList)
	}
	if len(listsubmap) != 3 {
		t.Fatalf("Failed to return a correct len of submap, %d instead of 3", len(listsubmap))
	}
	resNotContain, errSubmapNotContain := lookup.Submap([]ResourceID{
		{
			Name:    "Banking",
			Variant: "Secondary",
			Type:    TRANSFORMATION_VARIANT,
		},
	})
	if errSubmapNotContain == nil && resNotContain != nil {
		t.Fatalf("Failed to catch error not found in submap %s", errSubmap)
	}
}

func TestErrorFeatureVariant(t *testing.T) {
	sugar := zap.NewExample().Sugar()
	server := Config{
		Logger: sugar,
	}
	metadataServer, err := NewMetadataServer(&server)
	if err != nil {
		t.Fatalf("Failed to set up metadataserver %s", err)
	}
	variant := pb.FeatureVariant{
		Name:    "Wine",
		Variant: "Second-Variant",
		Source: &pb.NameVariant{
			Name:    "name",
			Variant: "var",
		},
		Type:        "Feature",
		Entity:      "entity",
		Created:     "date",
		Owner:       "bob",
		Description: "thing",
		Provider:    "Snowflake",
		Trainingsets: []*pb.NameVariant{
			{
				Name:    "name",
				Variant: "var",
			}},
	}
	if _, err := metadataServer.CreateFeatureVariant(context.Background(), &variant); err == nil {
		t.Fatalf("Failed to throw error at non existent feature variant %s", err)
	}
}

func TestResourceIDmethods(t *testing.T) {
	resourceId := ResourceID{
		Name:    "Fraud",
		Variant: "default",
		Type:    TRAINING_SET_VARIANT,
	}
	resourceProto := resourceId.Proto()
	if resourceProto.Name != resourceId.Name {
		t.Fatalf("Failed to correctly complete resourceId.Proto (mismatch name)")
	}
	if resourceProto.Variant != resourceId.Variant {
		t.Fatalf("Failed to correctly complete resourceId.Proto (mismatch variant)")
	}
	_, hasParent := resourceId.Parent()
	if hasParent != false {
		t.Fatalf("Failed to correctly set boolean (false) for resourceId.Parent()")
	}
}

func TestResourceIDmethodsParent(t *testing.T) {
	resourceId := ResourceID{
		Name:    "Fraud",
		Variant: "default",
		Type:    TRAINING_SET_VARIANT,
	}
	parentResourceID, hasParent := resourceId.Parent()
	if hasParent != true {
		t.Fatalf("Failed to correctly set boolean (true) for resourceId.Parent()")
	}
	if parentResourceID.Name != resourceId.Name {
		t.Fatalf("Failed to correctly set resource (Name field) for resourceId.Parent()")
	}
}

func makeStreamMock() *UserStreamMock {
	return &UserStreamMock{
		ctx:            context.Background(),
		recvToServer:   make(chan *pb.Name, 10),
		sentFromServer: make(chan *pb.User, 10),
	}
}

type UserStreamMock struct {
	grpc.ServerStream
	ctx            context.Context
	recvToServer   chan *pb.Name
	sentFromServer chan *pb.User
}

func (m *UserStreamMock) Context() context.Context {
	return m.ctx
}
func (m *UserStreamMock) Send(resp *pb.User) error {
	m.sentFromServer <- resp
	return nil
}
func (m *UserStreamMock) Recv() (*pb.Name, error) {
	req, more := <-m.recvToServer
	if !more {
		return nil, errors.New("no more to recieve")
	}
	return req, nil
}

func (m *UserStreamMock) SendFromClient(req *pb.Name) error {
	m.recvToServer <- req
	return nil
}
func (m *UserStreamMock) RecvToClient() (*pb.User, error) {
	response, more := <-m.sentFromServer
	if !more {
		return nil, errors.New("no more to recieve")
	}
	return response, nil
}

func makeStreamMockProvider() *ProviderStreamMock {
	return &ProviderStreamMock{
		ctx:            context.Background(),
		recvToServer:   make(chan *pb.Name, 10),
		sentFromServer: make(chan *pb.Provider, 10),
	}
}

type ProviderStreamMock struct {
	grpc.ServerStream
	ctx            context.Context
	recvToServer   chan *pb.Name
	sentFromServer chan *pb.Provider
}

func (m *ProviderStreamMock) Context() context.Context {
	return m.ctx
}
func (m *ProviderStreamMock) Send(resp *pb.Provider) error {
	m.sentFromServer <- resp
	return nil
}
func (m *ProviderStreamMock) Recv() (*pb.Name, error) {
	req, more := <-m.recvToServer
	if !more {
		return nil, errors.New("no more to recieve")
	}
	return req, nil
}

func (m *ProviderStreamMock) SendFromClient(req *pb.Name) error {
	m.recvToServer <- req
	return nil
}
func (m *ProviderStreamMock) RecvToClient() (*pb.Provider, error) {
	response, more := <-m.sentFromServer
	if !more {
		return nil, errors.New("no more to recieve")
	}
	return response, nil
}
func makeStreamMockEntity() *EntityStreamMock {
	return &EntityStreamMock{
		ctx:            context.Background(),
		recvToServer:   make(chan *pb.Name, 10),
		sentFromServer: make(chan *pb.Entity, 10),
	}
}

type EntityStreamMock struct {
	grpc.ServerStream
	ctx            context.Context
	recvToServer   chan *pb.Name
	sentFromServer chan *pb.Entity
}

func (m *EntityStreamMock) Context() context.Context {
	return m.ctx
}
func (m *EntityStreamMock) Send(resp *pb.Entity) error {
	m.sentFromServer <- resp
	return nil
}
func (m *EntityStreamMock) Recv() (*pb.Name, error) {
	req, more := <-m.recvToServer
	if !more {
		return nil, errors.New("no more to recieve")
	}
	return req, nil
}

func (m *EntityStreamMock) SendFromClient(req *pb.Name) error {
	m.recvToServer <- req
	return nil
}
func (m *EntityStreamMock) RecvToClient() (*pb.Entity, error) {
	response, more := <-m.sentFromServer
	if !more {
		return nil, errors.New("no more to recieve")
	}
	return response, nil
}

func makeStreamMockSourceVar() *SourceVarStreamMock {
	return &SourceVarStreamMock{
		ctx:            context.Background(),
		recvToServer:   make(chan *pb.NameVariant, 10),
		sentFromServer: make(chan *pb.SourceVariant, 10),
	}
}

type SourceVarStreamMock struct {
	grpc.ServerStream
	ctx            context.Context
	recvToServer   chan *pb.NameVariant
	sentFromServer chan *pb.SourceVariant
}

func (m *SourceVarStreamMock) Context() context.Context {
	return m.ctx
}
func (m *SourceVarStreamMock) Send(resp *pb.SourceVariant) error {
	m.sentFromServer <- resp
	return nil
}
func (m *SourceVarStreamMock) Recv() (*pb.NameVariant, error) {
	req, more := <-m.recvToServer
	if !more {
		return nil, errors.New("no more to recieve")
	}
	return req, nil
}

func (m *SourceVarStreamMock) SendFromClient(req *pb.NameVariant) error {
	m.recvToServer <- req
	return nil
}
func (m *SourceVarStreamMock) RecvToClient() (*pb.SourceVariant, error) {
	response, more := <-m.sentFromServer
	if !more {
		return nil, errors.New("no more to recieve")
	}
	return response, nil
}

func makeStreamMockLabelVar() *LabelVarStreamMock {
	return &LabelVarStreamMock{
		ctx:            context.Background(),
		recvToServer:   make(chan *pb.NameVariant, 10),
		sentFromServer: make(chan *pb.LabelVariant, 10),
	}
}

type LabelVarStreamMock struct {
	grpc.ServerStream
	ctx            context.Context
	recvToServer   chan *pb.NameVariant
	sentFromServer chan *pb.LabelVariant
}

func (m *LabelVarStreamMock) Context() context.Context {
	return m.ctx
}
func (m *LabelVarStreamMock) Send(resp *pb.LabelVariant) error {
	m.sentFromServer <- resp
	return nil
}
func (m *LabelVarStreamMock) Recv() (*pb.NameVariant, error) {
	req, more := <-m.recvToServer
	if !more {
		return nil, errors.New("no more to recieve")
	}
	return req, nil
}

func (m *LabelVarStreamMock) SendFromClient(req *pb.NameVariant) error {
	m.recvToServer <- req
	return nil
}
func (m *LabelVarStreamMock) RecvToClient() (*pb.LabelVariant, error) {
	response, more := <-m.sentFromServer
	if !more {
		return nil, errors.New("no more to recieve")
	}
	return response, nil
}

func makeStreamMockModel() *ModelStreamMock {
	return &ModelStreamMock{
		ctx:            context.Background(),
		recvToServer:   make(chan *pb.Name, 10),
		sentFromServer: make(chan *pb.Model, 10),
	}
}

type ModelStreamMock struct {
	grpc.ServerStream
	ctx            context.Context
	recvToServer   chan *pb.Name
	sentFromServer chan *pb.Model
}

func (m *ModelStreamMock) Context() context.Context {
	return m.ctx
}
func (m *ModelStreamMock) Send(resp *pb.Model) error {
	m.sentFromServer <- resp
	return nil
}
func (m *ModelStreamMock) Recv() (*pb.Name, error) {
	req, more := <-m.recvToServer
	if !more {
		return nil, errors.New("no more to recieve")
	}
	return req, nil
}

func (m *ModelStreamMock) SendFromClient(req *pb.Name) error {
	m.recvToServer <- req
	return nil
}
func (m *ModelStreamMock) RecvToClient() (*pb.Model, error) {
	response, more := <-m.sentFromServer
	if !more {
		return nil, errors.New("no more to recieve")
	}
	return response, nil
}

func makeStreamMockTrainingSetVar() *TrainingSetVarStreamMock {
	return &TrainingSetVarStreamMock{
		ctx:            context.Background(),
		recvToServer:   make(chan *pb.NameVariant, 10),
		sentFromServer: make(chan *pb.TrainingSetVariant, 10),
	}
}

type TrainingSetVarStreamMock struct {
	grpc.ServerStream
	ctx            context.Context
	recvToServer   chan *pb.NameVariant
	sentFromServer chan *pb.TrainingSetVariant
}

func (m *TrainingSetVarStreamMock) Context() context.Context {
	return m.ctx
}
func (m *TrainingSetVarStreamMock) Send(resp *pb.TrainingSetVariant) error {
	m.sentFromServer <- resp
	return nil
}
func (m *TrainingSetVarStreamMock) Recv() (*pb.NameVariant, error) {
	req, more := <-m.recvToServer
	if !more {
		return nil, errors.New("no more to recieve")
	}
	return req, nil
}

func (m *TrainingSetVarStreamMock) SendFromClient(req *pb.NameVariant) error {
	m.recvToServer <- req
	return nil
}
func (m *TrainingSetVarStreamMock) RecvToClient() (*pb.TrainingSetVariant, error) {
	response, more := <-m.sentFromServer
	if !more {
		return nil, errors.New("no more to recieve")
	}
	return response, nil
}

func TestCreateMetadataserverMethods(t *testing.T) {
	sugar := zap.NewExample().Sugar()
	server := Config{
		Logger: sugar,
	}
	metadataServer, err := NewMetadataServer(&server)
	if err != nil {
		t.Fatalf("Failed to set up metadataserver %s", err)
	}
	if _, err := metadataServer.CreateUser(context.Background(), &pb.User{Name: "Simba Khadder"}); err != nil {
		t.Fatalf("Failed to create user%s", err)
	}
	stream := makeStreamMock()
	if err := stream.SendFromClient(&pb.Name{Name: "Simba Khadder"}); err != nil {
		t.Fatalf("Failed to send from client user%s", err)
	}
	go func() {
		if err := metadataServer.GetUsers(stream); err != nil {
			t.Fatalf("Failed to get users%s", err)
		}
	}()
	go func() {
		var v pb.Empty
		if err := metadataServer.ListUsers(&v, stream); err != nil {
			t.Fatalf("Failed to list users%s", err)
		}
	}()
	_, err2 := stream.RecvToClient()
	if err2 != nil {
		t.Fatalf("failed to rec")
	}
	if _, err := metadataServer.CreateProvider(context.Background(), &pb.Provider{
		Name:        "demo-s3",
		Description: "local S3 deployment",
		Type:        "Batch",
		Software:    "BigQuery",
		Team:        "",
	}); err != nil {
		t.Fatalf("Failed to create provider %s", err)
	}

	str := makeStreamMockProvider()
	if err := str.SendFromClient(&pb.Name{
		Name: "demo-s3",
	}); err != nil {
		t.Fatalf("Failed to send from client providers%s", err)
	}
	go func() {
		if err := metadataServer.GetProviders(str); err != nil {
			t.Fatalf("Failed to get providers%s", err)
		}
	}()
	go func() {
		var v pb.Empty
		if err := metadataServer.ListProviders(&v, str); err != nil {
			t.Fatalf("Failed to list providers%s", err)
		}
	}()
	_, errRec := str.RecvToClient()
	if errRec != nil {
		t.Fatalf("failed to rec")
	}

	if _, err := metadataServer.CreateEntity(context.Background(), &pb.Entity{
		Name:        "user",
		Description: "user description",
	}); err != nil {
		t.Fatalf("Failed to create entity %s", err)
	}
	streamEntity := makeStreamMockEntity()
	if err := streamEntity.SendFromClient(&pb.Name{
		Name: "user",
	}); err != nil {
		t.Fatalf("Failed to send from client 'Entity'%s", err)
	}
	go func() {
		if err := metadataServer.GetEntities(streamEntity); err != nil {
			t.Fatalf("Failed to get entities%s", err)
		}
	}()
	go func() {
		var v pb.Empty
		if err := metadataServer.ListEntities(&v, streamEntity); err != nil {
			t.Fatalf("Failed to list entities%s", err)
		}
	}()
	_, errRecieveEntity := streamEntity.RecvToClient()
	if errRecieveEntity != nil {
		t.Fatalf("failed to recieve")
	}
	if _, err := metadataServer.CreateSourceVariant(context.Background(), &pb.SourceVariant{
		Name:        "Transactions",
		Variant:     "default",
		Description: "Source of user transactions",
		Type:        "CSV",
		Owner:       "Simba Khadder",
		Provider:    "demo-s3",
	}); err != nil {
		t.Fatalf("Failed to create source var %s", err)
	}
	streamSourceVar := makeStreamMockSourceVar()
	if err := streamSourceVar.SendFromClient(&pb.NameVariant{
		Name:    "Transactions",
		Variant: "default",
	}); err != nil {
		t.Fatalf("Failed to send from client 'NameVariant'%s", err)
	}
	go func() {
		if err := metadataServer.GetSourceVariants(streamSourceVar); err != nil {
			t.Fatalf("Failed to get source variants%s", err)
		}
	}()
	_, errRecieveSourceVariant := streamSourceVar.RecvToClient()
	if errRecieveSourceVariant != nil {
		t.Fatalf("failed to recieve")
	}

	if _, err := metadataServer.CreateLabelVariant(context.Background(), &pb.LabelVariant{
		Name:        "is_fraud",
		Variant:     "default",
		Description: "if a transaction is fraud",
		Type:        "boolean",
		Source:      &pb.NameVariant{Name: "Transactions", Variant: "default"},
		Entity:      "user",
		Owner:       "Simba Khadder",
		Provider:    "demo-s3",
	}); err != nil {
		t.Fatalf("Failed to label variant %s", err)
	}
	streamLabelVar := makeStreamMockLabelVar()
	if err := streamLabelVar.SendFromClient(&pb.NameVariant{
		Name:    "is_fraud",
		Variant: "default",
	}); err != nil {
		t.Fatalf("Failed to send from client 'NameVariant'%s", err)
	}
	go func() {
		if err := metadataServer.GetLabelVariants(streamLabelVar); err != nil {
			t.Fatalf("Failed to get label variants%s", err)
		}
	}()
	_, errRecieveLabelVar := streamLabelVar.RecvToClient()
	if errRecieveLabelVar != nil {
		t.Fatalf("failed to recieve")
	}
	if _, err := metadataServer.CreateModel(context.Background(), &pb.Model{
		Name:        "user_fraud_random_forest",
		Description: "Classifier on whether user commited fraud",
	}); err != nil {
		t.Fatalf("Failed to create model %s", err)
	}
	streamModel := makeStreamMockModel()
	if err := streamModel.SendFromClient(&pb.Name{
		Name: "user_fraud_random_forest",
	}); err != nil {
		t.Fatalf("Failed to send from client 'Models'%s", err)
	}
	go func() {
		if err := metadataServer.GetModels(streamModel); err != nil {
			t.Fatalf("Failed to get models%s", err)
		}
	}()
	go func() {
		var v pb.Empty
		if err := metadataServer.ListModels(&v, streamModel); err != nil {
			t.Fatalf("Failed to get models%s", err)
		}
	}()
	_, errRecieveModel := streamModel.RecvToClient()
	if errRecieveModel != nil {
		t.Fatalf("failed to recieve model")
	}
	if _, err := metadataServer.CreateTrainingSetVariant(context.Background(), &pb.TrainingSetVariant{
		Name:        "is_fraud",
		Variant:     "default",
		Description: "if a transaction is fraud",
		Owner:       "Simba Khadder",
		Provider:    "demo-s3",
		Label:       &pb.NameVariant{Name: "is_fraud", Variant: "default"},
	}); err != nil {
		t.Fatalf("Failed to create trainingset %s", err)
	}
	streamTrainingSetVar := makeStreamMockTrainingSetVar()
	if err := streamTrainingSetVar.SendFromClient(&pb.NameVariant{
		Name:    "is_fraud",
		Variant: "default",
	}); err != nil {
		t.Fatalf("Failed to send from client 'TrainingsetVariant'%s", err)
	}
	go func() {
		if err := metadataServer.GetTrainingSetVariants(streamTrainingSetVar); err != nil {
			t.Fatalf("Failed to get trainingset variants%s", err)
		}
	}()
	_, errRecieveTrainingSetVar := streamTrainingSetVar.RecvToClient()
	if errRecieveTrainingSetVar != nil {
		t.Fatalf("failed to recieve")
	}
}
