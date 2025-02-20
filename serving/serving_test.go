// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package serving

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"reflect"
	"testing"

	"github.com/featureform/scheduling"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/proto"

	"github.com/google/uuid"
	grpcmeta "google.golang.org/grpc/metadata"

	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	"github.com/featureform/metrics"
	pb "github.com/featureform/proto"
	"github.com/featureform/provider"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/provider/types"
)

const PythonFunc = `def average_user_transaction(transactions):
	return transactions.groupby("CustomerID")["TransactionAmount"].mean()`

func simpleFeatureRecords() map[provider.ResourceID][]provider.ResourceRecord {
	featureId := provider.ResourceID{
		Name:    "feature",
		Variant: "variant",
		Type:    provider.Feature,
	}
	featureRecs := []provider.ResourceRecord{
		{Entity: "a", Value: 12.5},
		{Entity: "b", Value: "def"},
	}
	labelId := provider.ResourceID{
		Name:    "label",
		Variant: "variant",
		Type:    provider.Label,
	}
	labelRecs := []provider.ResourceRecord{
		{Entity: "a", Value: true},
		{Entity: "b", Value: false},
	}
	return map[provider.ResourceID][]provider.ResourceRecord{
		featureId: featureRecs,
		labelId:   labelRecs,
	}
}

func onDemandFeatureRecords() map[provider.ResourceID][]provider.ResourceRecord {
	featureId := provider.ResourceID{
		Name:    "feature-od",
		Variant: "on-demand",
		Type:    provider.Feature,
	}
	featureRecs := []provider.ResourceRecord{
		{Value: []byte(PythonFunc)},
	}
	return map[provider.ResourceID][]provider.ResourceRecord{
		featureId: featureRecs,
	}
}

func invalidFeatureRecords() map[provider.ResourceID][]provider.ResourceRecord {
	featureId := provider.ResourceID{
		Name:    "feature",
		Variant: "variant",
		Type:    provider.Feature,
	}
	featureRecs := []provider.ResourceRecord{
		{Entity: "a", Value: make([]string, 0)},
	}
	labelId := provider.ResourceID{
		Name:    "label",
		Variant: "variant",
		Type:    provider.Label,
	}
	labelRecs := []provider.ResourceRecord{
		{Entity: "a", Value: true},
	}
	return map[provider.ResourceID][]provider.ResourceRecord{
		featureId: featureRecs,
		labelId:   labelRecs,
	}
}

func invalidLabelRecords() map[provider.ResourceID][]provider.ResourceRecord {
	featureId := provider.ResourceID{
		Name:    "feature",
		Variant: "variant",
		Type:    provider.Feature,
	}
	featureRecs := []provider.ResourceRecord{
		{Entity: "a", Value: 12.5},
	}
	labelId := provider.ResourceID{
		Name:    "label",
		Variant: "variant",
		Type:    provider.Label,
	}
	labelRecs := []provider.ResourceRecord{
		{Entity: "a", Value: make([]string, 0)},
	}
	return map[provider.ResourceID][]provider.ResourceRecord{
		featureId: featureRecs,
		labelId:   labelRecs,
	}
}

func invalidTypeFeatureRecords() map[provider.ResourceID][]provider.ResourceRecord {
	id := provider.ResourceID{
		Name:    "feature",
		Variant: "variant",
		Type:    provider.Feature,
	}
	recs := []provider.ResourceRecord{
		{Entity: "a", Value: make([]string, 0)},
	}
	return map[provider.ResourceID][]provider.ResourceRecord{
		id: recs,
	}
}

func allTypesFeatureRecords() map[provider.ResourceID][]provider.ResourceRecord {
	idToVal := map[provider.ResourceID]interface{}{
		{
			Name:    "feature",
			Variant: "double",
		}: 12.5,
		{
			Name:    "feature",
			Variant: "float",
		}: float32(2.3),
		{
			Name:    "feature",
			Variant: "str",
		}: "abc",
		{
			Name:    "feature",
			Variant: "int",
		}: 5,
		{
			Name:    "feature",
			Variant: "smallint",
		}: int32(4),
		{
			Name:    "feature",
			Variant: "bigint",
		}: int64(3),
		{
			Name:    "feature",
			Variant: "bool",
		}: true,
		{
			Name:    "feature",
			Variant: "proto",
		}: &pb.Value{
			Value: &pb.Value_StrValue{StrValue: "proto"},
		},
	}
	recs := make(map[provider.ResourceID][]provider.ResourceRecord)
	for id, val := range idToVal {
		id.Type = provider.Feature
		recs[id] = []provider.ResourceRecord{
			{Entity: "a", Value: val},
		}
	}
	return recs
}

func allTypesResourceDefsFn(providerType string) []metadata.ResourceDef {
	return []metadata.ResourceDef{
		metadata.UserDef{
			Name: "Featureform",
		},
		metadata.ProviderDef{
			Name: "mockOnline",
			Type: providerType,
		},
		metadata.EntityDef{
			Name: "mockEntity",
		},
		metadata.SourceDef{
			Name:     "mockSource",
			Variant:  "var",
			Owner:    "Featureform",
			Provider: "mockOnline",
			Definition: metadata.PrimaryDataSource{
				Location: metadata.SQLTable{
					Name: "mockPrimary",
				},
			},
		},
		metadata.FeatureDef{
			Name:     "feature",
			Variant:  "double",
			Provider: "mockOnline",
			Entity:   "mockEntity",
			Source:   metadata.NameVariant{Name: "mockSource", Variant: "var"},
			Owner:    "Featureform",
			Location: metadata.ResourceVariantColumns{
				Entity: "col1",
				Value:  "col2",
				TS:     "col3",
			},
			Mode:       metadata.PRECOMPUTED,
			IsOnDemand: false,
		},
		metadata.FeatureDef{
			Name:     "feature",
			Variant:  "float",
			Provider: "mockOnline",
			Entity:   "mockEntity",
			Source:   metadata.NameVariant{Name: "mockSource", Variant: "var"},
			Owner:    "Featureform",
			Location: metadata.ResourceVariantColumns{
				Entity: "col1",
				Value:  "col2",
				TS:     "col3",
			},
			Mode:       metadata.PRECOMPUTED,
			IsOnDemand: false,
		},
		metadata.FeatureDef{
			Name:     "feature",
			Variant:  "str",
			Provider: "mockOnline",
			Entity:   "mockEntity",
			Source:   metadata.NameVariant{Name: "mockSource", Variant: "var"},
			Owner:    "Featureform",
			Location: metadata.ResourceVariantColumns{
				Entity: "col1",
				Value:  "col2",
				TS:     "col3",
			},
			Mode:       metadata.PRECOMPUTED,
			IsOnDemand: false,
		},
		metadata.FeatureDef{
			Name:     "feature",
			Variant:  "int",
			Provider: "mockOnline",
			Entity:   "mockEntity",
			Source:   metadata.NameVariant{Name: "mockSource", Variant: "var"},
			Owner:    "Featureform",
			Location: metadata.ResourceVariantColumns{
				Entity: "col1",
				Value:  "col2",
				TS:     "col3",
			},
			Mode:       metadata.PRECOMPUTED,
			IsOnDemand: false,
		},
		metadata.FeatureDef{
			Name:     "feature",
			Variant:  "smallint",
			Provider: "mockOnline",
			Entity:   "mockEntity",
			Source:   metadata.NameVariant{Name: "mockSource", Variant: "var"},
			Owner:    "Featureform",
			Location: metadata.ResourceVariantColumns{
				Entity: "col1",
				Value:  "col2",
				TS:     "col3",
			},
			Mode:       metadata.PRECOMPUTED,
			IsOnDemand: false,
		},
		metadata.FeatureDef{
			Name:     "feature",
			Variant:  "bigint",
			Provider: "mockOnline",
			Entity:   "mockEntity",
			Source:   metadata.NameVariant{Name: "mockSource", Variant: "var"},
			Owner:    "Featureform",
			Location: metadata.ResourceVariantColumns{
				Entity: "col1",
				Value:  "col2",
				TS:     "col3",
			},
			Mode:       metadata.PRECOMPUTED,
			IsOnDemand: false,
		},
		metadata.FeatureDef{
			Name:     "feature",
			Variant:  "bool",
			Provider: "mockOnline",
			Entity:   "mockEntity",
			Source:   metadata.NameVariant{Name: "mockSource", Variant: "var"},
			Owner:    "Featureform",
			Location: metadata.ResourceVariantColumns{
				Entity: "col1",
				Value:  "col2",
				TS:     "col3",
			},
			Mode:       metadata.PRECOMPUTED,
			IsOnDemand: false,
		},
		metadata.FeatureDef{
			Name:     "feature",
			Variant:  "proto",
			Provider: "mockOnline",
			Entity:   "mockEntity",
			Source:   metadata.NameVariant{Name: "mockSource", Variant: "var"},
			Owner:    "Featureform",
			Location: metadata.ResourceVariantColumns{
				Entity: "col1",
				Value:  "col2",
				TS:     "col3",
			},
			Mode:       metadata.PRECOMPUTED,
			IsOnDemand: false,
		},
	}
}

func simpleResourceDefsFn(providerType string) []metadata.ResourceDef {
	return []metadata.ResourceDef{
		metadata.UserDef{
			Name: "Featureform",
		},
		metadata.ProviderDef{
			Name: "mockOnline",
			Type: providerType,
		},
		metadata.EntityDef{
			Name: "mockEntity",
		},
		metadata.SourceDef{
			Name:     "mockSource",
			Variant:  "var",
			Owner:    "Featureform",
			Provider: "mockOnline",
			Definition: metadata.PrimaryDataSource{
				Location: metadata.SQLTable{
					Name: "mockPrimary",
				},
			},
		},
		metadata.FeatureDef{
			Name:     "feature",
			Variant:  "variant",
			Provider: "mockOnline",
			Entity:   "mockEntity",
			Source:   metadata.NameVariant{Name: "mockSource", Variant: "var"},
			Owner:    "Featureform",
			Location: metadata.ResourceVariantColumns{
				Entity: "col1",
				Value:  "col2",
				TS:     "col3",
			},
			Mode:       metadata.PRECOMPUTED,
			IsOnDemand: false,
		},
		metadata.FeatureDef{
			Name:     "feature",
			Variant:  "variant2",
			Provider: "mockOnline",
			Entity:   "mockEntity",
			Source:   metadata.NameVariant{Name: "mockSource", Variant: "var"},
			Owner:    "Featureform",
			Location: metadata.ResourceVariantColumns{
				Entity: "col1",
				Value:  "col2",
				TS:     "col3",
			},
			Mode:       metadata.PRECOMPUTED,
			IsOnDemand: false,
		},
		metadata.LabelDef{
			Name:     "label",
			Variant:  "variant",
			Provider: "mockOnline",
			Entity:   "mockEntity",
			Source:   metadata.NameVariant{Name: "mockSource", Variant: "var"},
			Owner:    "Featureform",
			Type:     types.String,
			Location: metadata.ResourceVariantColumns{
				Entity: "col1",
				Value:  "col2",
				TS:     "col3",
			},
		},
		metadata.TrainingSetDef{
			Name:     "training-set",
			Variant:  "variant",
			Provider: "mockOnline",
			Label:    metadata.NameVariant{Name: "label", Variant: "variant"},
			Features: metadata.NameVariants{{Name: "feature", Variant: "variant"}},
			Owner:    "Featureform",
		},
	}
}

func simpleTrainingSetDefs() []provider.TrainingSetDef {
	return []provider.TrainingSetDef{
		{
			ID: provider.ResourceID{
				Name:    "training-set",
				Variant: "variant",
			},
			Label: provider.ResourceID{
				Name:    "label",
				Variant: "variant",
			},
			Features: []provider.ResourceID{
				{
					Name:    "feature",
					Variant: "variant",
				},
			},
		},
	}
}

func onDemandResourceDefsFn(providerType string) []metadata.ResourceDef {
	return []metadata.ResourceDef{
		metadata.UserDef{
			Name: "Featureform",
		},
		metadata.FeatureDef{
			Name:    "feature-od",
			Variant: "on-demand",
			Owner:   "Featureform",
			Location: metadata.PythonFunction{
				Query: []byte(PythonFunc),
			},
			Mode:       metadata.CLIENT_COMPUTED,
			IsOnDemand: true,
		},
	}
}

type resourceDefsFn func(providerType string) []metadata.ResourceDef

type onlineTestContext struct {
	ResourceDefsFn resourceDefsFn
	FactoryFn      provider.Factory
	metaServ       *metadata.MetadataServer
	context.Context
	logger logging.Logger
}

func (ctx *onlineTestContext) Create(t *testing.T) *FeatureServer {
	var addr string
	ctx.Context, ctx.logger = logging.NewTestContextAndLogger(t)
	ctx.metaServ, addr = startMetadata(t, ctx, ctx.logger)
	providerType := uuid.NewString()
	if ctx.FactoryFn != nil {
		if err := provider.RegisterFactory(pt.Type(providerType), ctx.FactoryFn); err != nil {
			t.Fatalf("Failed to register factory: %s", err)
		}
	}
	meta := metadataClient(t, ctx, ctx.logger, addr)
	if ctx.ResourceDefsFn != nil {
		defs := ctx.ResourceDefsFn(providerType)
		if err := meta.CreateAll(ctx, defs); err != nil {
			t.Fatalf("Failed to create metadata entries: %s", err)
		}
	}
	serv, err := NewFeatureServer(meta, metrics.NewMetrics(randomMetricsId()), ctx.logger)
	if err != nil {
		t.Fatalf("Failed to create feature server: %s", err)
	}
	return serv
}

func (ctx *onlineTestContext) Destroy() {
	ctx.metaServ.Stop()
}

// Metrics can't have numbers in it, so we can't just use a UUID.
func randomMetricsId() string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	id := make([]rune, 24)
	for i := range id {
		id[i] = letters[rand.Intn(len(letters))]
	}
	return string(id)
}

func createMockOnlineStoreFactory(recsMap map[provider.ResourceID][]provider.ResourceRecord) provider.Factory {
	return func(cfg pc.SerializedConfig) (provider.Provider, error) {
		store := provider.NewLocalOnlineStore()
		for id, recs := range recsMap {
			if id.Type != provider.Feature {
				continue
			}
			table, err := store.CreateTable(id.Name, id.Variant, types.String)
			if err != nil {
				panic(err)
			}
			for _, rec := range recs {
				if err := table.Set(rec.Entity, rec.Value); err != nil {
					panic(err)
				}
			}
		}
		return store, nil
	}
}

func createMockOfflineStoreFactory(recsMap map[provider.ResourceID][]provider.ResourceRecord, defs []provider.TrainingSetDef) provider.Factory {
	return func(cfg pc.SerializedConfig) (provider.Provider, error) {
		store := provider.NewMemoryOfflineStore()
		for id, recs := range recsMap {
			table, err := store.CreateResourceTable(id, provider.TableSchema{})
			if err != nil {
				panic(err)
			}
			for _, rec := range recs {
				if err := table.Write(rec); err != nil {
					panic(err)
				}
			}
		}
		for _, def := range defs {
			if err := store.CreateTrainingSet(def); err != nil {
				panic(err)
			}
		}
		return store, nil
	}
}

func onlineStoreNoTables(cfg pc.SerializedConfig) (provider.Provider, error) {
	store := provider.NewLocalOnlineStore()
	return store, nil
}

func startMetadata(t *testing.T, ctx context.Context, logger logging.Logger) (*metadata.MetadataServer, string) {
	manager, err := scheduling.NewMemoryTaskMetadataManager(ctx)
	if err != nil {
		panic(err)
	}
	config := &metadata.Config{
		Logger:      logger,
		TaskManager: manager,
	}
	serv, err := metadata.NewMetadataServer(ctx, config)
	if err != nil {
		panic(err)
	}
	// listen on a random port
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	go func() {
		if err := serv.ServeOnListener(lis); err != nil {
			panic(err)
		}
	}()
	return serv, lis.Addr().String()
}

func metadataClient(t *testing.T, ctx context.Context, logger logging.Logger, addr string) *metadata.Client {
	client, err := metadata.NewClient(addr, logger)
	if err != nil {
		t.Fatalf("Failed to create client: %s", err)
	}
	return client
}

func unwrapVal(val *pb.Value) interface{} {
	switch casted := val.Value.(type) {
	case *pb.Value_DoubleValue:
		return casted.DoubleValue
	case *pb.Value_FloatValue:
		return casted.FloatValue
	case *pb.Value_StrValue:
		return casted.StrValue
	case *pb.Value_IntValue:
		return int(casted.IntValue)
	case *pb.Value_Int32Value:
		return casted.Int32Value
	case *pb.Value_Int64Value:
		return casted.Int64Value
	case *pb.Value_BoolValue:
		return casted.BoolValue
	case *pb.Value_OnDemandFunction:
		return casted.OnDemandFunction
	default:
		panic(fmt.Sprintf("Unable to unwrap value: %T", val.Value))
	}
}

func TestFeatureServe(t *testing.T) {
	ctx := onlineTestContext{
		ResourceDefsFn: simpleResourceDefsFn,
		FactoryFn:      createMockOnlineStoreFactory(simpleFeatureRecords()),
	}
	serv := ctx.Create(t)
	defer ctx.Destroy()
	req := &pb.FeatureServeRequest{
		Features: []*pb.FeatureID{
			{
				Name:    "feature",
				Version: "variant",
			},
		},
		Entities: []*pb.Entity{
			{
				Name:   "mockEntity",
				Values: []string{"a"},
			},
		},
	}
	resp, err := serv.FeatureServe(ctx, req)
	if err != nil {
		t.Fatalf("Failed to serve feature: %s", err)
	}
	vals := resp.ValueLists
	if len(vals) != len(req.Features) {
		t.Fatalf("Wrong number of values: %d\nExpected: %d", len(vals), len(req.Features))
	}
	var dblVal interface{}
	for _, val := range vals {
		dblVal = unwrapVal(val.Values[0])
	}
	if dblVal != 12.5 {
		t.Fatalf("Wrong feature value: %v\nExpected: %v", dblVal, 12.5)
	}
}

func TestFeatureServeMultipleEntities(t *testing.T) {
	ctx := onlineTestContext{
		ResourceDefsFn: simpleResourceDefsFn,
		FactoryFn:      createMockOnlineStoreFactory(simpleFeatureRecords()),
	}
	serv := ctx.Create(t)
	defer ctx.Destroy()
	req := &pb.FeatureServeRequest{
		Features: []*pb.FeatureID{
			{
				Name:    "feature",
				Version: "variant",
			},
		},
		Entities: []*pb.Entity{
			&pb.Entity{
				Name:   "mockEntity",
				Values: []string{"a", "b"},
			},
		},
	}
	resp, err := serv.FeatureServe(ctx, req)
	if err != nil {
		t.Fatalf("Failed to serve feature: %s", err)
	}
	vals := resp.ValueLists
	if len(vals) != len(req.Features) {
		t.Fatalf("Wrong number of values: %d\nExpected: %d", len(vals), len(req.Features))
	}
	var features []interface{}
	for _, val := range vals {
		var entities []interface{}
		for _, v := range val.Values {
			entities = append(entities, unwrapVal(v))
		}
		features = append(features, entities)
	}
	expectedValues := []interface{}{[]interface{}{12.5, "def"}}
	if !reflect.DeepEqual(features, expectedValues) {
		t.Fatalf("Wrong feature values: %v\nExpected: %v", features, expectedValues)
	}
}

// todo: should be able to delete
type mockBatchServingStream struct {
	RowChan    chan *pb.BatchFeatureRow
	ShouldFail bool
}

func newMockBatchServingStream() *mockBatchServingStream {
	return &mockBatchServingStream{
		RowChan: make(chan *pb.BatchFeatureRow),
	}
}

func (stream *mockBatchServingStream) Send(row *pb.BatchFeatureRow) error {
	if stream.ShouldFail {
		return fmt.Errorf("Mock Failure")
	}
	stream.RowChan <- row
	return nil
}

func (stream *mockBatchServingStream) Context() context.Context {
	return context.Background()
}

func (stream *mockBatchServingStream) SetHeader(grpcmeta.MD) error {
	return nil
}

func (stream *mockBatchServingStream) SendHeader(grpcmeta.MD) error {
	return nil
}

func (stream *mockBatchServingStream) SetTrailer(grpcmeta.MD) {
}

func (stream *mockBatchServingStream) SendMsg(interface{}) error {
	return nil
}

func (stream *mockBatchServingStream) RecvMsg(interface{}) error {
	return nil
}

// func TestBatchFeatureServe(t *testing.T) {
// 	ctx := onlineTestContext{
// 		ResourceDefsFn: simpleResourceDefsFn,
// 		FactoryFn:      createMockOfflineStoreFactory(simpleFeatureRecords(), simpleTrainingSetDefs()),
// 	}
// 	serv := ctx.Create(t)
// 	defer ctx.Destroy()
// 	req := &pb.BatchFeatureServeRequest{
// 		Features: []*pb.FeatureID{
// 			{
// 				Name:    "feature",
// 				Version: "variant",
// 			},
// 		},
// 	}
// 	stream := newMockBatchServingStream()
// 	errChan := make(chan error)
// 	go func() {
// 		if err := serv.BatchFeatureServe(req, stream); err != nil {
// 			errChan <- err
// 		}
// 		close(errChan)
// 	}()
// 	type Row struct {
// 		Entity   interface{}
// 		Features interface{}
// 	}
// 	// We use a map since the order is not guaranteed.
// 	expectedRows := map[interface{}]Row{
// 		'a': {"a", 1},
// 		'b': {"b", 2},
// 	}
// 	actualRows := make(map[interface{}]Row)
// 	moreVals := true
// 	for moreVals {
// 		select {
// 		case row := <-stream.RowChan:
// 			actualRows[unwrapVal(row.Entity)] = Row{unwrapVal(row.Entity), unwrapVal(row.Features[0])}
// 		case err := <-errChan:
// 			if err != nil {
// 				t.Fatalf("Failed to serve batch data: %s", err)
// 			}
// 			moreVals = false
// 		}
// 	}
// 	if !reflect.DeepEqual(expectedRows, actualRows) {
// 		t.Fatalf("Rows arent equal: %v\n%v", expectedRows, actualRows)
// 	}
// }

func TestFeatureNotFound(t *testing.T) {
	ctx := onlineTestContext{
		ResourceDefsFn: simpleResourceDefsFn,
		FactoryFn:      createMockOnlineStoreFactory(simpleFeatureRecords()),
	}
	serv := ctx.Create(t)
	defer ctx.Destroy()
	req := &pb.FeatureServeRequest{
		Features: []*pb.FeatureID{
			{
				Name:    "nonexistantFeature",
				Version: "variant",
			},
		},
		Entities: []*pb.Entity{
			{
				Name:   "mockEntity",
				Values: []string{"a"},
			},
		},
	}
	if _, err := serv.FeatureServe(ctx, req); err == nil {
		t.Fatalf("Succeeded in serving non-existant feature")
	}
}

func TestProviderNotRegistered(t *testing.T) {
	ctx := onlineTestContext{
		ResourceDefsFn: simpleResourceDefsFn,
		FactoryFn:      nil,
	}
	serv := ctx.Create(t)
	defer ctx.Destroy()
	req := &pb.FeatureServeRequest{
		Features: []*pb.FeatureID{
			{
				Name:    "feature",
				Version: "variant",
			},
		},
		Entities: []*pb.Entity{
			{
				Name:   "mockEntity",
				Values: []string{"a"},
			},
		},
	}
	if _, err := serv.FeatureServe(ctx, req); err == nil {
		t.Fatalf("Succeeded in serving feature with no registered provider factory")
	}
}

func TestOfflineStoreAsOnlineStore(t *testing.T) {
	ctx := onlineTestContext{
		ResourceDefsFn: simpleResourceDefsFn,
		FactoryFn:      createMockOfflineStoreFactory(simpleFeatureRecords(), nil),
	}
	serv := ctx.Create(t)
	defer ctx.Destroy()
	req := &pb.FeatureServeRequest{
		Features: []*pb.FeatureID{
			{
				Name:    "feature",
				Version: "variant",
			},
		},
		Entities: []*pb.Entity{
			{
				Name:   "mockEntity",
				Values: []string{"a"},
			},
		},
	}
	if _, err := serv.FeatureServe(ctx, req); err == nil {
		t.Fatalf("Succeeded in serving feature stored on OfflineStore")
	}
}

func TestTableNotFoundInOnlineStore(t *testing.T) {
	ctx := onlineTestContext{
		ResourceDefsFn: simpleResourceDefsFn,
		FactoryFn:      onlineStoreNoTables,
	}
	serv := ctx.Create(t)
	defer ctx.Destroy()
	req := &pb.FeatureServeRequest{
		Features: []*pb.FeatureID{
			{
				Name:    "feature",
				Version: "variant",
			},
		},
		Entities: []*pb.Entity{
			{
				Name:   "mockEntity",
				Values: []string{"a"},
			},
		},
	}
	if _, err := serv.FeatureServe(ctx, req); err == nil {
		t.Fatalf("Succeeded in serving feature in an online store without a valid table")
	}
}

func TestEntityNotFoundInOnlineStore(t *testing.T) {
	ctx := onlineTestContext{
		ResourceDefsFn: simpleResourceDefsFn,
		FactoryFn:      createMockOnlineStoreFactory(simpleFeatureRecords()),
	}
	serv := ctx.Create(t)
	defer ctx.Destroy()
	req := &pb.FeatureServeRequest{
		Features: []*pb.FeatureID{
			{
				Name:    "feature",
				Version: "variant",
			},
		},
		Entities: []*pb.Entity{
			{
				Name:   "mockEntity",
				Values: []string{"NonExistantEntity"},
			},
		},
	}
	if _, err := serv.FeatureServe(ctx, req); err == nil {
		t.Fatalf("Succeeded in serving feature with non-existant entity")
	}
}

func TestEntityNotInRequest(t *testing.T) {
	ctx := onlineTestContext{
		ResourceDefsFn: simpleResourceDefsFn,
		FactoryFn:      createMockOnlineStoreFactory(simpleFeatureRecords()),
	}
	serv := ctx.Create(t)
	defer ctx.Destroy()
	req := &pb.FeatureServeRequest{
		Features: []*pb.FeatureID{
			{
				Name:    "feature",
				Version: "variant",
			},
		},
		Entities: []*pb.Entity{
			{
				Name:   "wrongEntity",
				Values: []string{"a"},
			},
		},
	}
	if _, err := serv.FeatureServe(ctx, req); err == nil {
		t.Fatalf("Succeeded in serving feature without the right entity set")
	}
}

func TestInvalidFeatureType(t *testing.T) {
	ctx := onlineTestContext{
		ResourceDefsFn: simpleResourceDefsFn,
		FactoryFn:      createMockOnlineStoreFactory(invalidTypeFeatureRecords()),
	}
	serv := ctx.Create(t)
	defer ctx.Destroy()
	req := &pb.FeatureServeRequest{
		Features: []*pb.FeatureID{
			{
				Name:    "feature",
				Version: "variant",
			},
		},
		Entities: []*pb.Entity{
			{
				Name:   "mockEntity",
				Values: []string{"a"},
			},
		},
	}
	if _, err := serv.FeatureServe(ctx, req); err == nil {
		t.Fatalf("Succeeded in serving feature with invalid type")
	}
}

func TestAllFeatureTypes(t *testing.T) {
	ctx := onlineTestContext{
		ResourceDefsFn: allTypesResourceDefsFn,
		FactoryFn:      createMockOnlineStoreFactory(allTypesFeatureRecords()),
	}
	serv := ctx.Create(t)
	defer ctx.Destroy()
	req := &pb.FeatureServeRequest{
		Features: []*pb.FeatureID{
			{
				Name:    "feature",
				Version: "double",
			},
			{
				Name:    "feature",
				Version: "float",
			},
			{
				Name:    "feature",
				Version: "str",
			},
			{
				Name:    "feature",
				Version: "int",
			},
			{
				Name:    "feature",
				Version: "smallint",
			},
			{
				Name:    "feature",
				Version: "bigint",
			},
			{
				Name:    "feature",
				Version: "bool",
			},
			{
				Name:    "feature",
				Version: "proto",
			},
		},
		Entities: []*pb.Entity{
			{
				Name:   "mockEntity",
				Values: []string{"a"},
			},
		},
	}
	resp, err := serv.FeatureServe(ctx, req)
	if err != nil {
		t.Fatalf("Failed to get multiple features with all types: %s", err)
	}
	expected := []interface{}{
		12.5, float32(2.3), "abc", 5, int32(4), int64(3), true, "proto",
	}
	vals := resp.ValueLists
	if len(vals) != len(req.Features) {
		t.Fatalf("Wrong number of values: %d\nExpected: %d", len(vals), len(req.Features))
	}
	for i, exp := range expected {
		v := vals[i]
		unwrapped := unwrapVal(v.Values[0])
		if unwrapped != exp {
			t.Fatalf("Values not equal %v %v", vals, expected)
		}
	}
}

func TestSimpleModelRegistrationFeatureServe(t *testing.T) {
	ctx := onlineTestContext{
		ResourceDefsFn: simpleResourceDefsFn,
		FactoryFn:      createMockOnlineStoreFactory(simpleFeatureRecords()),
	}
	serv := ctx.Create(t)
	defer ctx.Destroy()
	modelName := "model"
	feature := &pb.FeatureID{
		Name:    "feature",
		Version: "variant",
	}
	req := &pb.FeatureServeRequest{
		Features: []*pb.FeatureID{
			feature,
		},
		Entities: []*pb.Entity{
			{
				Name:   "mockEntity",
				Values: []string{"a"},
			},
		},
		Model: &pb.Model{
			Name: modelName,
		},
	}
	resp, err := serv.FeatureServe(ctx, req)
	if err != nil {
		t.Fatalf("Failed to serve feature: %s", err)
	}
	vals := resp.ValueLists
	if len(vals) != len(req.Features) {
		t.Fatalf("Wrong number of values: %d\nExpected: %d", len(vals), len(req.Features))
	}

	for _, v := range vals[0].Values {
		gotVal := unwrapVal(v)
		if gotVal != 12.5 {
			t.Fatalf("Wrong feature value: %v\nExpected: %v", gotVal, 12.5)
		}
	}
	modelResp, err := serv.Metadata.GetModel(ctx, modelName)
	if err != nil {
		t.Fatalf("Failed to get model: %s", err)
	}
	if len(modelResp.Features()) != 1 {
		t.Fatalf("Failed to associate model with feature")
	}
	modelFeature := modelResp.Features()[0]
	if !(modelFeature.Name == feature.Name && modelFeature.Variant == feature.Version) {
		t.Fatalf("Wrong feature associated with registered model: %v\nExpected %v", modelFeature, feature)
	}
}

func TestOnDemandFeatureServe(t *testing.T) {
	ctx := onlineTestContext{
		ResourceDefsFn: onDemandResourceDefsFn,
		FactoryFn:      createMockOnlineStoreFactory(onDemandFeatureRecords()),
	}
	serv := ctx.Create(t)
	defer ctx.Destroy()
	req := &pb.FeatureServeRequest{
		Features: []*pb.FeatureID{
			{
				Name:    "feature-od",
				Version: "on-demand",
			},
		},
	}
	resp, err := serv.FeatureServe(ctx, req)
	if err != nil {
		t.Fatalf("Failed to serve feature: %s", err)
	}
	vals := resp.ValueLists
	if len(vals) != len(req.Features) {
		t.Fatalf("Wrong number of values: %d\nExpected: %d", len(vals), len(req.Features))
	}
	var dblVal []interface{}
	for _, val := range vals[0].Values {
		dblVal = append(dblVal, unwrapVal(val))
	}

	expected := []byte(PythonFunc)
	for _, val := range dblVal {
		areBytesEqual := bytes.Equal(val.([]byte), expected)
		if !areBytesEqual {
			t.Fatalf("Wrong feature value: %v\nExpected: %v", dblVal, string(expected))
		}
	}
}

type mockTrainingStream struct {
	RowChan    chan *pb.TrainingDataRows
	ShouldFail bool
}

func newMockTrainingStream() *mockTrainingStream {
	return &mockTrainingStream{
		RowChan: make(chan *pb.TrainingDataRows),
	}
}

func (stream *mockTrainingStream) Send(rows *pb.TrainingDataRows) error {
	if stream.ShouldFail {
		return fmt.Errorf("Mock Failure")
	}
	stream.RowChan <- rows
	return nil
}

func (stream *mockTrainingStream) Context() context.Context {
	return context.Background()
}

func (stream *mockTrainingStream) SetHeader(grpcmeta.MD) error {
	return nil
}

func (stream *mockTrainingStream) SendHeader(grpcmeta.MD) error {
	return nil
}

func (stream *mockTrainingStream) SetTrailer(grpcmeta.MD) {
}

func (stream *mockTrainingStream) SendMsg(interface{}) error {
	return nil
}

func (stream *mockTrainingStream) RecvMsg(interface{}) error {
	return nil
}

func TestSimpleTrainingSetServe(t *testing.T) {
	ctx := onlineTestContext{
		ResourceDefsFn: simpleResourceDefsFn,
		FactoryFn:      createMockOfflineStoreFactory(simpleFeatureRecords(), simpleTrainingSetDefs()),
	}
	serv := ctx.Create(t)
	defer ctx.Destroy()
	req := &pb.TrainingDataRequest{
		Id: &pb.TrainingDataID{
			Name:    "training-set",
			Version: "variant",
		},
	}
	stream := newMockTrainingStream()
	errChan := make(chan error)
	go func() {
		if err := serv.TrainingData(req, stream); err != nil {
			errChan <- err
		}
		close(errChan)
	}()
	type Row struct {
		Feature interface{}
		Label   interface{}
	}
	// We use a map since the order is not guaranteed.
	expectedRows := map[Row]bool{
		{12.5, true}:   true,
		{"def", false}: true,
	}
	actualRows := make(map[Row]bool)
	moreVals := true
	for moreVals {
		select {
		case rows := <-stream.RowChan:
			for _, row := range rows.Rows {
				if len(row.Features) != 1 {
					t.Fatalf("Row has too many features: %v", row)
				}
				actualRows[Row{
					Feature: unwrapVal(row.Features[0]),
					Label:   unwrapVal(row.Label),
				}] = true
			}
		case err := <-errChan:
			if err != nil {
				t.Fatalf("Failed to get training data: %s", err)
			}
			moreVals = false
		}
	}
	if !reflect.DeepEqual(expectedRows, actualRows) {
		t.Fatalf("Rows arent equal: %v\n%v", expectedRows, actualRows)
	}
}

func TestTrainingSetNotFound(t *testing.T) {
	ctx := onlineTestContext{
		ResourceDefsFn: simpleResourceDefsFn,
		FactoryFn:      createMockOfflineStoreFactory(simpleFeatureRecords(), simpleTrainingSetDefs()),
	}
	serv := ctx.Create(t)
	defer ctx.Destroy()
	req := &pb.TrainingDataRequest{
		Id: &pb.TrainingDataID{
			Name:    "nonexistant-training-set",
			Version: "variant",
		},
	}
	stream := newMockTrainingStream()
	errChan := make(chan error)
	go func() {
		if err := serv.TrainingData(req, stream); err != nil {
			errChan <- err
		}
		close(errChan)
	}()
	if err := <-errChan; err == nil {
		t.Fatalf("Succeeded in serving non-existant training data: %s", err)
	}
}

func TestTrainingSetNoProviderFactory(t *testing.T) {
	ctx := onlineTestContext{
		ResourceDefsFn: simpleResourceDefsFn,
		FactoryFn:      nil,
	}
	serv := ctx.Create(t)
	defer ctx.Destroy()
	req := &pb.TrainingDataRequest{
		Id: &pb.TrainingDataID{
			Name:    "training-set",
			Version: "variant",
		},
	}
	stream := newMockTrainingStream()
	errChan := make(chan error)
	go func() {
		if err := serv.TrainingData(req, stream); err != nil {
			errChan <- err
		}
		close(errChan)
	}()
	if err := <-errChan; err == nil {
		t.Fatalf("Succeeded in serving with no provider: %s", err)
	}
}

func TestTrainingSetInOnlineStore(t *testing.T) {
	ctx := onlineTestContext{
		ResourceDefsFn: simpleResourceDefsFn,
		FactoryFn:      createMockOnlineStoreFactory(simpleFeatureRecords()),
	}
	serv := ctx.Create(t)
	defer ctx.Destroy()
	req := &pb.TrainingDataRequest{
		Id: &pb.TrainingDataID{
			Name:    "training-set",
			Version: "variant",
		},
	}
	stream := newMockTrainingStream()
	errChan := make(chan error)
	go func() {
		if err := serv.TrainingData(req, stream); err != nil {
			errChan <- err
		}
		close(errChan)
	}()
	if err := <-errChan; err == nil {
		t.Fatalf("Succeeded in serving with online store provider: %s", err)
	}
}

func TestTrainingSetStreamFailure(t *testing.T) {
	ctx := onlineTestContext{
		ResourceDefsFn: simpleResourceDefsFn,
		FactoryFn:      createMockOfflineStoreFactory(simpleFeatureRecords(), simpleTrainingSetDefs()),
	}
	serv := ctx.Create(t)
	defer ctx.Destroy()
	req := &pb.TrainingDataRequest{
		Id: &pb.TrainingDataID{
			Name:    "training-set",
			Version: "variant",
		},
	}
	stream := newMockTrainingStream()
	stream.ShouldFail = true
	errChan := make(chan error)
	go func() {
		if err := serv.TrainingData(req, stream); err != nil {
			errChan <- err
		}
		close(errChan)
	}()
	if err := <-errChan; err == nil {
		t.Fatalf("Succeeded in serving on broken stream: %s", err)
	}
}

func TestTrainingSetInvalidLabel(t *testing.T) {
	ctx := onlineTestContext{
		ResourceDefsFn: simpleResourceDefsFn,
		FactoryFn:      createMockOfflineStoreFactory(invalidLabelRecords(), simpleTrainingSetDefs()),
	}
	serv := ctx.Create(t)
	defer ctx.Destroy()
	req := &pb.TrainingDataRequest{
		Id: &pb.TrainingDataID{
			Name:    "training-set",
			Version: "variant",
		},
	}
	stream := newMockTrainingStream()
	stream.ShouldFail = true
	errChan := make(chan error)
	go func() {
		if err := serv.TrainingData(req, stream); err != nil {
			errChan <- err
		}
		close(errChan)
	}()
	if err := <-errChan; err == nil {
		t.Fatalf("Succeeded in serving invalid label: %s", err)
	}
}

func TestTrainingSetInvalidFeature(t *testing.T) {
	ctx := onlineTestContext{
		ResourceDefsFn: simpleResourceDefsFn,
		FactoryFn:      createMockOfflineStoreFactory(invalidFeatureRecords(), simpleTrainingSetDefs()),
	}
	serv := ctx.Create(t)
	defer ctx.Destroy()
	req := &pb.TrainingDataRequest{
		Id: &pb.TrainingDataID{
			Name:    "training-set",
			Version: "variant",
		},
	}
	stream := newMockTrainingStream()
	errChan := make(chan error)
	go func() {
		if err := serv.TrainingData(req, stream); err != nil {
			errChan <- err
		}
		close(errChan)
	}()
	if err := <-errChan; err == nil {
		t.Fatalf("Succeeded in serving invalid feature: %s", err)
	}
}

func TestSimpleModelRegistrationTrainingSetServe(t *testing.T) {
	ctx := onlineTestContext{
		ResourceDefsFn: simpleResourceDefsFn,
		FactoryFn:      createMockOfflineStoreFactory(simpleFeatureRecords(), simpleTrainingSetDefs()),
	}
	serv := ctx.Create(t)
	defer ctx.Destroy()
	modelName := "model"
	trainingData := &pb.TrainingDataID{
		Name:    "training-set",
		Version: "variant",
	}
	req := &pb.TrainingDataRequest{
		Id: trainingData,
		Model: &pb.Model{
			Name: modelName,
		},
	}
	stream := newMockTrainingStream()
	errChan := make(chan error)
	go func() {
		if err := serv.TrainingData(req, stream); err != nil {
			errChan <- err
		}
		close(errChan)
	}()
	type Row struct {
		Feature interface{}
		Label   interface{}
	}
	// We use a map since the order is not guaranteed.
	expectedRows := map[Row]bool{
		{12.5, true}:   true,
		{"def", false}: true,
	}
	actualRows := make(map[Row]bool)
	moreVals := true
	for moreVals {
		select {
		case rows := <-stream.RowChan:
			for _, row := range rows.Rows {
				if len(row.Features) != 1 {
					t.Fatalf("Row has too many features: %v", row)
				}
				actualRows[Row{
					Feature: unwrapVal(row.Features[0]),
					Label:   unwrapVal(row.Label),
				}] = true
			}
		case err := <-errChan:
			if err != nil {
				t.Fatalf("Failed to get training data: %s", err)
			}
			moreVals = false
		}
	}
	if !reflect.DeepEqual(expectedRows, actualRows) {
		t.Fatalf("Rows aren't equal: %v\n%v", expectedRows, actualRows)
	}
	modelResp, err := serv.Metadata.GetModel(ctx, modelName)
	if err != nil {
		t.Fatalf("Failed to get model: %s", err)
	}
	if len(modelResp.TrainingSets()) != 1 {
		t.Fatalf("Failed to associate model with feature")
	}
	modelTrainingSet := modelResp.TrainingSets()[0]
	if !(modelTrainingSet.Name == trainingData.Name && modelTrainingSet.Variant == trainingData.Version) {
		t.Fatalf("Wrong training set associated with registered model: %v\nExpected %v", modelTrainingSet, trainingData)
	}
}

func TestTrainingDataColumns(t *testing.T) {
	ctx := onlineTestContext{
		ResourceDefsFn: simpleResourceDefsFn,
		FactoryFn:      createMockOfflineStoreFactory(simpleFeatureRecords(), simpleTrainingSetDefs()),
	}
	serv := ctx.Create(t)
	defer ctx.Destroy()
	req := &pb.TrainingDataColumnsRequest{
		Id: &pb.TrainingDataID{
			Name:    "training-set",
			Version: "variant",
		},
	}
	expectedColumns := &pb.TrainingColumns{
		Features: []string{"feature__feature__variant"},
		Label:    "label__label__variant",
	}
	resp, err := serv.TrainingDataColumns(ctx, req)
	if err != nil {
		t.Fatalf("Failed to get training data columns: %s", err)
	}
	if !reflect.DeepEqual(expectedColumns, resp) {
		t.Fatalf("Columns aren't equal: %v\n%v", expectedColumns, resp)
	}
}

// Test Train Test Split

type MockFeature_TrainTestSplitServer struct {
	mock.Mock
	pb.Feature_TrainTestSplitServer
	Responses []*pb.BatchTrainTestSplitResponse
}

func (m *MockFeature_TrainTestSplitServer) Send(response *pb.BatchTrainTestSplitResponse) error {
	args := m.Called(response)
	m.Responses = append(m.Responses, response)
	return args.Error(0)
}

func (m *MockFeature_TrainTestSplitServer) Recv() (*pb.TrainTestSplitRequest, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*pb.TrainTestSplitRequest), nil
}

func TestTrainTestSplit_Initialize(t *testing.T) {
	ctx := onlineTestContext{
		ResourceDefsFn: simpleResourceDefsFn,
		FactoryFn:      createMockOfflineStoreFactory(simpleFeatureRecords(), simpleTrainingSetDefs()),
	}
	serv := ctx.Create(t)
	defer ctx.Destroy()

	initRequest := &pb.TrainTestSplitRequest{
		Id:          &pb.TrainingDataID{Name: "training-set", Version: "variant"},
		Model:       nil,
		TestSize:    .5,
		TrainSize:   .5,
		Shuffle:     false,
		RandomState: 0,
		RequestType: pb.RequestType_INITIALIZE,
		BatchSize:   1,
	}

	mockTrainTestSplitServer := new(MockFeature_TrainTestSplitServer)

	mockTrainTestSplitServer.On("Recv").Return(initRequest, nil).Once()
	mockTrainTestSplitServer.On("Recv").Return(nil, io.EOF).Once()
	mockTrainTestSplitServer.On("Send", mock.Anything).Return(nil).Maybe()

	done := make(chan bool)

	go func() {
		err := serv.TrainTestSplit(mockTrainTestSplitServer)
		assert.NoError(t, err)
		close(done)
	}()

	<-done

	assert.NotEmpty(t, mockTrainTestSplitServer.Responses)
	assert.Equal(t, pb.RequestType_INITIALIZE, mockTrainTestSplitServer.Responses[0].RequestType)
}

func TestTrainTestSplit_DataRequest(t *testing.T) {
	ctx := onlineTestContext{
		ResourceDefsFn: simpleResourceDefsFn,
		FactoryFn:      createMockOfflineStoreFactory(simpleFeatureRecords(), simpleTrainingSetDefs()),
	}
	serv := ctx.Create(t)
	defer ctx.Destroy()

	initRequest := &pb.TrainTestSplitRequest{
		Id:          &pb.TrainingDataID{Name: "training-set", Version: "variant"},
		Model:       nil,
		TestSize:    .5,
		TrainSize:   .5,
		Shuffle:     false,
		RandomState: 0,
		RequestType: pb.RequestType_INITIALIZE,
		BatchSize:   1,
	}

	dataRequest := proto.Clone(initRequest).(*pb.TrainTestSplitRequest)
	dataRequest.RequestType = pb.RequestType_TRAINING

	mockTrainTestSplitServer := new(MockFeature_TrainTestSplitServer)

	// Setup mock to return initRequest, then dataRequest, then simulate stream end with io.EOF
	mockTrainTestSplitServer.On("Recv").Return(initRequest, nil).Once()
	mockTrainTestSplitServer.On("Recv").Return(dataRequest, nil).Once()
	mockTrainTestSplitServer.On("Recv").Return(nil, io.EOF) // This will be returned for all subsequent calls

	mockTrainTestSplitServer.On("Send", mock.Anything).Return(nil)

	done := make(chan bool)

	go func() {
		err := serv.TrainTestSplit(mockTrainTestSplitServer)
		assert.NoError(t, err)
		close(done)
	}()

	<-done

	assert.NotEmpty(t, mockTrainTestSplitServer.Responses)
	assert.Equal(t, pb.RequestType_INITIALIZE, mockTrainTestSplitServer.Responses[0].RequestType)
}
