// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package api

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/featureform/logging"
	pb "github.com/featureform/metadata/proto"
	"github.com/featureform/proto"
	srv "github.com/featureform/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type mockFeatureClient struct{}
type testKey string

func (m *mockFeatureClient) TrainingData(ctx context.Context, in *srv.TrainingDataRequest, opts ...grpc.CallOption) (srv.Feature_TrainingDataClient, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (m *mockFeatureClient) TrainingDataColumns(ctx context.Context, in *srv.TrainingDataColumnsRequest, opts ...grpc.CallOption) (*srv.TrainingColumns, error) {
	return &srv.TrainingColumns{}, nil
}

func (m *mockFeatureClient) FeatureServe(ctx context.Context, in *srv.FeatureServeRequest, opts ...grpc.CallOption) (*srv.FeatureRow, error) {
	return &srv.FeatureRow{}, nil
}

func (m *mockFeatureClient) SourceData(ctx context.Context, in *srv.SourceDataRequest, opts ...grpc.CallOption) (srv.Feature_SourceDataClient, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (m *mockFeatureClient) SourceColumns(ctx context.Context, in *srv.SourceColumnRequest, opts ...grpc.CallOption) (*srv.SourceDataColumns, error) {
	return &srv.SourceDataColumns{}, nil
}

func (m *mockFeatureClient) Nearest(ctx context.Context, in *srv.NearestRequest, opts ...grpc.CallOption) (*srv.NearestResponse, error) {
	return &srv.NearestResponse{}, nil // Nearest was the method we aimed to mock for positive response in the test.
}

func (m *mockFeatureClient) BatchFeatureServe(ctx context.Context, in *srv.BatchFeatureServeRequest, opts ...grpc.CallOption) (srv.Feature_BatchFeatureServeClient, error) {
	return nil, nil
}
func (m *mockFeatureClient) ResourceLocation(ctx context.Context, in *srv.TrainingDataRequest, opts ...grpc.CallOption) (*srv.ResourceLocation, error) {
	return &srv.ResourceLocation{}, nil
}

func (m *mockFeatureClient) GetResourceLocation(ctx context.Context, in *srv.ResourceIdRequest, opts ...grpc.CallOption) (*srv.ResourceLocation, error) {
	return &srv.ResourceLocation{}, nil
}

func (m *mockFeatureClient) TrainTestSplit(ctx context.Context, opts ...grpc.CallOption) (srv.Feature_TrainTestSplitClient, error) {
	return nil, nil
}

func TestOnlineServerNearest(t *testing.T) {
	type fields struct {
		Logger  logging.Logger
		address string
		client  proto.FeatureClient
	}
	type args struct {
		ctx context.Context
		req *srv.NearestRequest
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *srv.NearestResponse
		wantErr bool
	}{
		{
			name: "Test Nearest",
			fields: fields{
				Logger:  logging.WrapZapLogger(zap.NewExample().Sugar()),
				address: "localhost:50051",
				client:  &mockFeatureClient{},
			},
			args: args{
				ctx: context.WithValue(context.Background(), testKey("tk"), "test_user"),
				req: &srv.NearestRequest{Id: &srv.FeatureID{Name: "f1", Version: "v1"}},
			},
			want:    &srv.NearestResponse{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			serv := &OnlineServer{
				Logger:  tt.fields.Logger,
				address: tt.fields.address,
				client:  tt.fields.client,
			}
			got, err := serv.Nearest(tt.args.ctx, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("Nearest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Nearest() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOnlineServerSourceColumns(t *testing.T) {
	type fields struct {
		Logger  logging.Logger
		address string
		client  proto.FeatureClient
	}
	type args struct {
		ctx context.Context
		req *srv.SourceColumnRequest
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *srv.SourceDataColumns
		wantErr bool
	}{
		{
			name: "Test SourceColumns",
			fields: fields{
				Logger:  logging.WrapZapLogger(zap.NewExample().Sugar()),
				address: "localhost:50051",
				client:  &mockFeatureClient{},
			},
			args: args{
				ctx: context.WithValue(context.Background(), testKey("tk"), "test_user"),
				req: &srv.SourceColumnRequest{Id: &srv.SourceID{Name: "f1", Version: "v1"}},
			},
			want:    &srv.SourceDataColumns{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			serv := &OnlineServer{
				Logger:  tt.fields.Logger,
				address: tt.fields.address,
				client:  tt.fields.client,
			}

			got, err := serv.SourceColumns(tt.args.ctx, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("SourceColumns() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SourceColumns() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOnlineServerFeatureServe(t *testing.T) {
	type fields struct {
		Logger  logging.Logger
		address string
		client  proto.FeatureClient
	}
	type args struct {
		ctx context.Context
		req *srv.FeatureServeRequest
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *srv.FeatureRow
		wantErr bool
	}{
		{
			name: "Test FeatureServe",
			fields: fields{
				Logger:  logging.WrapZapLogger(zap.NewExample().Sugar()),
				address: "localhost:50051",
				client:  &mockFeatureClient{},
			},
			args: args{
				ctx: context.WithValue(context.Background(), testKey("tk"), "test_user"),
				req: &srv.FeatureServeRequest{
					Features: []*srv.FeatureID{
						&srv.FeatureID{Name: "f1", Version: "v1"},
					},
					Entities: []*srv.Entity{
						&srv.Entity{Name: "e1", Value: "v1"},
					},
				},
			},
			want:    &srv.FeatureRow{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			serv := &OnlineServer{
				Logger:  tt.fields.Logger,
				address: tt.fields.address,
				client:  tt.fields.client,
			}
			got, err := serv.FeatureServe(tt.args.ctx, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("FeatureServe() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("FeatureServe() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOnlineServerTrainingDataColumns(t *testing.T) {
	type fields struct {
		Logger  logging.Logger
		address string
		client  proto.FeatureClient
	}
	type args struct {
		ctx context.Context
		req *srv.TrainingDataColumnsRequest
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *srv.TrainingColumns
		wantErr bool
	}{
		{
			name: "Test Training Data Columns",
			fields: fields{
				Logger:  logging.WrapZapLogger(zap.NewExample().Sugar()),
				address: "localhost:50051",
				client:  &mockFeatureClient{},
			},
			args: args{
				ctx: context.WithValue(context.Background(), testKey(""), "test_user"),
				req: &srv.TrainingDataColumnsRequest{Id: &srv.TrainingDataID{Name: "f1", Version: "v1"}},
			},
			want:    &srv.TrainingColumns{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			serv := &OnlineServer{
				Logger:  tt.fields.Logger,
				address: tt.fields.address,
				client:  tt.fields.client,
			}
			got, err := serv.TrainingDataColumns(tt.args.ctx, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("TrainingDataColumns() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("TrainingDataColumns() got = %v, want %v", got, tt.want)
			}
		})
	}
}

type mockAPIClient struct{}

func (m *mockAPIClient) CreateUser(ctx context.Context, in *pb.User, opts ...grpc.CallOption) (*pb.Empty, error) {
	return &pb.Empty{}, nil
}

func (m *mockAPIClient) CreateProvider(ctx context.Context, in *pb.Provider, opts ...grpc.CallOption) (*pb.Empty, error) {
	return &pb.Empty{}, nil
}

func (m *mockAPIClient) CreateSourceVariant(ctx context.Context, in *pb.SourceVariant, opts ...grpc.CallOption) (*pb.Empty, error) {
	return &pb.Empty{}, nil
}

func (m *mockAPIClient) CreateEntity(ctx context.Context, in *pb.Entity, opts ...grpc.CallOption) (*pb.Empty, error) {
	return &pb.Empty{}, nil
}

func (m *mockAPIClient) CreateFeatureVariant(ctx context.Context, in *pb.FeatureVariant, opts ...grpc.CallOption) (*pb.Empty, error) {
	return &pb.Empty{}, nil
}

func (m *mockAPIClient) CreateLabelVariant(ctx context.Context, in *pb.LabelVariant, opts ...grpc.CallOption) (*pb.Empty, error) {
	return &pb.Empty{}, nil
}

func (m *mockAPIClient) CreateTrainingSetVariant(ctx context.Context, in *pb.TrainingSetVariant, opts ...grpc.CallOption) (*pb.Empty, error) {
	return &pb.Empty{}, nil
}

func (m *mockAPIClient) CreateModel(ctx context.Context, in *pb.Model, opts ...grpc.CallOption) (*pb.Empty, error) {
	return &pb.Empty{}, nil
}

func (m *mockAPIClient) RequestScheduleChange(ctx context.Context, in *pb.ScheduleChangeRequest, opts ...grpc.CallOption) (*pb.Empty, error) {
	return &pb.Empty{}, nil
}

func (m *mockAPIClient) GetUsers(ctx context.Context, opts ...grpc.CallOption) (pb.Api_GetUsersClient, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (m *mockAPIClient) GetFeatures(ctx context.Context, opts ...grpc.CallOption) (pb.Api_GetFeaturesClient, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (m *mockAPIClient) GetFeatureVariants(ctx context.Context, opts ...grpc.CallOption) (pb.Api_GetFeatureVariantsClient, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (m *mockAPIClient) GetLabels(ctx context.Context, opts ...grpc.CallOption) (pb.Api_GetLabelsClient, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (m *mockAPIClient) GetLabelVariants(ctx context.Context, opts ...grpc.CallOption) (pb.Api_GetLabelVariantsClient, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (m *mockAPIClient) GetTrainingSets(ctx context.Context, opts ...grpc.CallOption) (pb.Api_GetTrainingSetsClient, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (m *mockAPIClient) GetTrainingSetVariants(ctx context.Context, opts ...grpc.CallOption) (pb.Api_GetTrainingSetVariantsClient, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (m *mockAPIClient) GetSources(ctx context.Context, opts ...grpc.CallOption) (pb.Api_GetSourcesClient, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (m *mockAPIClient) GetSourceVariants(ctx context.Context, opts ...grpc.CallOption) (pb.Api_GetSourceVariantsClient, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (m *mockAPIClient) GetProviders(ctx context.Context, opts ...grpc.CallOption) (pb.Api_GetProvidersClient, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (m *mockAPIClient) GetEntities(ctx context.Context, opts ...grpc.CallOption) (pb.Api_GetEntitiesClient, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (m *mockAPIClient) GetModels(ctx context.Context, opts ...grpc.CallOption) (pb.Api_GetModelsClient, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (m *mockAPIClient) ListFeatures(ctx context.Context, in *pb.Empty, opts ...grpc.CallOption) (pb.Api_ListFeaturesClient, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (m *mockAPIClient) ListLabels(ctx context.Context, in *pb.Empty, opts ...grpc.CallOption) (pb.Api_ListLabelsClient, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (m *mockAPIClient) ListTrainingSets(ctx context.Context, in *pb.Empty, opts ...grpc.CallOption) (pb.Api_ListTrainingSetsClient, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (m *mockAPIClient) ListSources(ctx context.Context, in *pb.Empty, opts ...grpc.CallOption) (pb.Api_ListSourcesClient, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (m *mockAPIClient) ListUsers(ctx context.Context, in *pb.Empty, opts ...grpc.CallOption) (pb.Api_ListUsersClient, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (m *mockAPIClient) ListProviders(ctx context.Context, in *pb.Empty, opts ...grpc.CallOption) (pb.Api_ListProvidersClient, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (m *mockAPIClient) ListEntities(ctx context.Context, in *pb.Empty, opts ...grpc.CallOption) (pb.Api_ListEntitiesClient, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (m *mockAPIClient) ListModels(ctx context.Context, in *pb.Empty, opts ...grpc.CallOption) (pb.Api_ListModelsClient, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (m *mockAPIClient) WriteFeatures(ctx context.Context, opts ...grpc.CallOption) (pb.Api_WriteFeaturesClient, error) {
	return nil, fmt.Errorf("Not implemented")
}
