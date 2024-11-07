// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package metadata

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/featureform/metadata/proto"
	pc "github.com/featureform/provider/provider_config"
	"github.com/featureform/provider/types"
)

func TestSourceVariant_IsTransformation(t *testing.T) {
	type fields struct {
		serialized           *pb.SourceVariant
		fetchTrainingSetsFns fetchTrainingSetsFns
		fetchFeaturesFns     fetchFeaturesFns
		fetchLabelsFns       fetchLabelsFns
		fetchProviderFns     fetchProviderFns
		createdFn            createdFn
		protoStringer        protoStringer
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{name: "test", fields: fields{
			serialized:           &pb.SourceVariant{Definition: &pb.SourceVariant_Transformation{}},
			fetchFeaturesFns:     fetchFeaturesFns{},
			fetchLabelsFns:       fetchLabelsFns{},
			fetchProviderFns:     fetchProviderFns{},
			fetchTrainingSetsFns: fetchTrainingSetsFns{},
			createdFn:            createdFn{},
			protoStringer:        protoStringer{}}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			variant := &SourceVariant{
				serialized:           tt.fields.serialized,
				fetchTrainingSetsFns: tt.fields.fetchTrainingSetsFns,
				fetchFeaturesFns:     tt.fields.fetchFeaturesFns,
				fetchLabelsFns:       tt.fields.fetchLabelsFns,
				fetchProviderFns:     tt.fields.fetchProviderFns,
				createdFn:            tt.fields.createdFn,
				protoStringer:        tt.fields.protoStringer,
			}
			if got := variant.IsTransformation(); got != tt.want {
				t.Errorf("IsTransformation() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSourceVariant_TransformationArgs(t *testing.T) {
	type fields struct {
		serialized           *pb.SourceVariant
		fetchTrainingSetsFns fetchTrainingSetsFns
		fetchFeaturesFns     fetchFeaturesFns
		fetchLabelsFns       fetchLabelsFns
		fetchProviderFns     fetchProviderFns
		createdFn            createdFn
		lastUpdatedFn        lastUpdatedFn
		protoStringer        protoStringer
	}
	tests := []struct {
		name   string
		fields fields
		want   TransformationArgs
	}{
		{
			"Kubernetes Proto",
			fields{
				serialized: &pb.SourceVariant{
					Definition: &pb.SourceVariant_Transformation{
						Transformation: &pb.Transformation{
							Args: &pb.Transformation_KubernetesArgs{
								KubernetesArgs: &pb.KubernetesArgs{
									DockerImage: "",
									Specs: &pb.KubernetesResourceSpecs{
										CpuLimit:      "1",
										CpuRequest:    "0.5",
										MemoryLimit:   "500M",
										MemoryRequest: "1G",
									},
								},
							},
						},
					},
				},
			},
			KubernetesArgs{
				DockerImage: "",
				Specs: KubernetesResourceSpecs{
					CPULimit:      "1",
					CPURequest:    "0.5",
					MemoryLimit:   "500M",
					MemoryRequest: "1G",
				},
			},
		},
		{
			"Nil Proto",
			fields{
				serialized: &pb.SourceVariant{
					Definition: &pb.SourceVariant_Transformation{
						Transformation: &pb.Transformation{
							Args: nil,
						},
					},
				},
			},
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			variant := &SourceVariant{
				serialized:           tt.fields.serialized,
				fetchTrainingSetsFns: tt.fields.fetchTrainingSetsFns,
				fetchFeaturesFns:     tt.fields.fetchFeaturesFns,
				fetchLabelsFns:       tt.fields.fetchLabelsFns,
				fetchProviderFns:     tt.fields.fetchProviderFns,
				createdFn:            tt.fields.createdFn,
				lastUpdatedFn:        tt.fields.lastUpdatedFn,
				protoStringer:        tt.fields.protoStringer,
			}
			if got := variant.TransformationArgs(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("TransformationArgs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func createJson(t *testing.T, m map[string]string) []byte {
	marshal, err := json.Marshal(m)
	if err != nil {
		t.Errorf("could not marshal test case: %s", err.Error())
	}
	return marshal
}

func TestKubernetesArgs_Format(t *testing.T) {
	type fields struct {
		DockerImage string
		Specs       KubernetesResourceSpecs
	}
	tests := []struct {
		name    string
		fields  fields
		want    map[string]string
		wantErr bool
	}{
		{"Empty", fields{},
			map[string]string{"Docker Image": "", "CPU Request": "", "CPU Limit": "", "Memory Request": "", "Memory Limit": ""}, false},
		{"With Image", fields{
			DockerImage: "my/test:image"},
			map[string]string{"Docker Image": "my/test:image", "CPU Request": "", "CPU Limit": "", "Memory Request": "", "Memory Limit": ""}, false},
		{"With Specs", fields{
			Specs: KubernetesResourceSpecs{"1", "2", "3", "4"}},
			map[string]string{"Docker Image": "", "CPU Request": "1", "CPU Limit": "2", "Memory Request": "3", "Memory Limit": "4"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			arg := KubernetesArgs{
				DockerImage: tt.fields.DockerImage,
				Specs:       tt.fields.Specs,
			}
			got := arg.Format()
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Format() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSourceVariant_HasKubernetesArgs(t *testing.T) {
	type fields struct {
		serialized *pb.SourceVariant
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{"Has Kubernetes Args",
			fields{
				serialized: &pb.SourceVariant{
					Definition: &pb.SourceVariant_Transformation{
						Transformation: &pb.Transformation{
							Args: &pb.Transformation_KubernetesArgs{
								KubernetesArgs: &pb.KubernetesArgs{
									DockerImage: "",
								},
							},
						},
					},
				},
			},
			true,
		},
		{"Does Not Have Kubernetes Args",
			fields{
				serialized: &pb.SourceVariant{
					Definition: &pb.SourceVariant_Transformation{
						Transformation: &pb.Transformation{},
					},
				},
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			variant := &SourceVariant{
				serialized: tt.fields.serialized,
			}
			if got := variant.HasKubernetesArgs(); got != tt.want {
				t.Errorf("HasKubernetesArgs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEmptyKubernetesArgsSpecs(t *testing.T) {
	type fields struct {
		serialized *pb.SourceVariant
	}
	tests := []struct {
		name   string
		fields fields
		want   KubernetesArgs
	}{
		{"Empty", fields{
			serialized: &pb.SourceVariant{},
		}, KubernetesArgs{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			variant := &SourceVariant{
				serialized: tt.fields.serialized,
			}
			if got := variant.parseKubernetesArgs(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseKubernetesArgs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestVectorValueType(t *testing.T) {
	valType := types.VectorType{
		ScalarType:  types.Float32,
		IsEmbedding: true,
		Dimension:   384,
	}
	fv := &pb.FeatureVariant{
		Name:    "vector",
		Variant: "vector_variant",
		Source: &pb.NameVariant{
			Name:    "vector_source",
			Variant: "vector_source_variant",
		},
		Type:     valType.ToProto(),
		Entity:   "vector_entity",
		Owner:    "vector_owner",
		Provider: "vector_provider",
		Location: &pb.FeatureVariant_Columns{
			Columns: &pb.Columns{
				Entity: "vector_entity",
				Value:  "vector_value",
			},
		},
	}

	wfc := WrapProtoFeatureVariant(fv)

	if wfc.Dimension() != 384 {
		t.Errorf("expected dimension to be 384, got %d", wfc.Dimension())
	}
	if wfc.IsEmbedding() != true {
		t.Errorf("expected embedding to be true, got %v", wfc.IsEmbedding())
	}
}

func TestFeatureVariant_ToShallowMap(t *testing.T) {
	valType := types.VectorType{
		ScalarType: types.Float32,
		Dimension:  64,
	}
	tests := []struct {
		name       string
		serialized *pb.FeatureVariant
		want       FeatureVariantResource
	}{
		{"Precomputed", &pb.FeatureVariant{
			Name:    "name",
			Variant: "variant",
			Source: &pb.NameVariant{
				Name:    "source name",
				Variant: "source variant",
			},
			Type:    valType.ToProto(),
			Entity:  "entity",
			Created: &timestamppb.Timestamp{},
			Owner:   "owner",
			Location: &pb.FeatureVariant_Columns{
				Columns: &pb.Columns{
					Entity: "entity",
					Value:  "value",
					Ts:     "ts",
				},
			},
			Description: "description",
			Provider:    "provider",
			Status: &pb.ResourceStatus{
				Status:       pb.ResourceStatus_NO_STATUS,
				ErrorMessage: "error",
				ErrorStatus:  &pb.ErrorStatus{},
			},
			Trainingsets:         []*pb.NameVariant{},
			LastUpdated:          &timestamppb.Timestamp{},
			Schedule:             "* * * * *",
			Tags:                 &pb.Tags{},
			Properties:           &pb.Properties{},
			Mode:                 pb.ComputationMode_PRECOMPUTED,
			AdditionalParameters: &pb.FeatureParameters{},
		}, FeatureVariantResource{
			Created:     time.UnixMilli(0).UTC(),
			Description: "description",
			Entity:      "entity",
			Name:        "name",
			Variant:     "variant",
			Owner:       "owner",
			Provider:    "provider",
			DataType:    "float32[64](embedding=false)",
			Status:      "NO_STATUS",
			Error:       "error",
			Location: map[string]string{
				"Entity": "entity",
				"Source": "",
				"TS":     "ts",
				"Value":  "value",
			},
			Source: NameVariant{
				Name:    "source name",
				Variant: "source variant",
			},
			TrainingSets: nil,
			Tags:         Tags{},
			Properties:   Properties{},
			Mode:         "PRECOMPUTED",
			IsOnDemand:   false,
			Definition:   "",
		}},
		{"ClientComputed", &pb.FeatureVariant{
			Name:    "name",
			Variant: "variant",
			Type:    valType.ToProto(),
			Entity:  "entity",
			Created: &timestamppb.Timestamp{},
			Owner:   "owner",
			Location: &pb.FeatureVariant_Function{
				Function: &pb.PythonFunction{
					Query: []byte("abcd"),
				},
			},
			Description: "description",
			Provider:    "provider",
			Status: &pb.ResourceStatus{
				Status:       pb.ResourceStatus_NO_STATUS,
				ErrorMessage: "error",
				ErrorStatus:  &pb.ErrorStatus{},
			},
			Trainingsets:         []*pb.NameVariant{},
			LastUpdated:          &timestamppb.Timestamp{},
			Schedule:             "* * * * *",
			Tags:                 &pb.Tags{},
			Properties:           &pb.Properties{},
			Mode:                 pb.ComputationMode_CLIENT_COMPUTED,
			AdditionalParameters: &pb.FeatureParameters{},
		}, FeatureVariantResource{
			Created:     time.UnixMilli(0).UTC(),
			Description: "description",
			Name:        "name",
			Variant:     "variant",
			Owner:       "owner",
			Status:      "NO_STATUS",
			Error:       "error",
			Location: map[string]string{
				"query": "abcd",
			},
			Source: NameVariant{
				Name:    "",
				Variant: "",
			},
			TrainingSets: nil,
			Tags:         Tags{},
			Properties:   Properties{},
			Mode:         "CLIENT_COMPUTED",
			IsOnDemand:   true,
			Definition:   "",
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			variant := &FeatureVariant{
				serialized:           tt.serialized,
				fetchTrainingSetsFns: fetchTrainingSetsFns{tt.serialized},
				fetchProviderFns:     fetchProviderFns{tt.serialized},
				fetchSourceFns:       fetchSourceFns{tt.serialized},
				createdFn:            createdFn{tt.serialized},
				lastUpdatedFn:        lastUpdatedFn{tt.serialized},
				protoStringer:        protoStringer{tt.serialized},
				fetchTagsFn:          fetchTagsFn{tt.serialized},
				fetchPropertiesFn:    fetchPropertiesFn{tt.serialized},
			}
			if got := variant.ToShallowMap(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ToShallowMap() = \n%#v, \nwant \n%#v", got, tt.want)
			}
		})
	}
}

func TestLabelVariant_ToShallowMap(t *testing.T) {
	tests := []struct {
		name       string
		serialized *pb.LabelVariant
		want       LabelVariantResource
	}{
		{
			"Simple",
			&pb.LabelVariant{
				Name:    "name",
				Variant: "variant",
				Source: &pb.NameVariant{
					Name:    "source name",
					Variant: "source variant",
				},
				Type:    types.Float32.ToProto(),
				Entity:  "entity",
				Created: &timestamppb.Timestamp{},
				Owner:   "owner",
				Location: &pb.LabelVariant_Columns{
					Columns: &pb.Columns{
						Entity: "entity",
						Value:  "value",
						Ts:     "ts",
					},
				},
				Description: "description",
				Provider:    "provider",
				Status: &pb.ResourceStatus{
					Status:       pb.ResourceStatus_NO_STATUS,
					ErrorMessage: "error",
					ErrorStatus:  &pb.ErrorStatus{},
				},
				Trainingsets: []*pb.NameVariant{},

				Tags:       &pb.Tags{},
				Properties: &pb.Properties{},
			}, LabelVariantResource{
				Created:     time.UnixMilli(0).UTC(),
				Description: "description",
				Entity:      "entity",
				Name:        "name",
				Variant:     "variant",
				Owner:       "owner",
				Provider:    "provider",
				DataType:    "float32",
				Status:      "NO_STATUS",
				Error:       "error",
				Location: map[string]string{
					"Entity": "entity",
					"Source": "",
					"TS":     "ts",
					"Value":  "value",
				},
				Source: NameVariant{
					Name:    "source name",
					Variant: "source variant",
				},
				TrainingSets: nil,
				Tags:         Tags{},
				Properties:   Properties{},
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			variant := &LabelVariant{
				serialized:           tt.serialized,
				fetchTrainingSetsFns: fetchTrainingSetsFns{tt.serialized},
				fetchProviderFns:     fetchProviderFns{tt.serialized},
				fetchSourceFns:       fetchSourceFns{tt.serialized},
				createdFn:            createdFn{tt.serialized},
				protoStringer:        protoStringer{tt.serialized},
				fetchTagsFn:          fetchTagsFn{tt.serialized},
				fetchPropertiesFn:    fetchPropertiesFn{tt.serialized},
			}
			if got := variant.ToShallowMap(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ToShallowMap() = %v, want %v", got, tt.want)
			}
		})
	}
}
func TestSourceVariant_ToShallowMap(t *testing.T) {
	tests := []struct {
		name       string
		serialized *pb.SourceVariant
		want       SourceVariantResource
	}{
		{
			"Simple",
			&pb.SourceVariant{
				Name:        "name",
				Variant:     "variant",
				Created:     &timestamppb.Timestamp{},
				Owner:       "owner",
				Description: "description",
				Provider:    "provider",
				Definition: &pb.SourceVariant_PrimaryData{
					PrimaryData: &pb.PrimaryData{
						Location: &pb.PrimaryData_Table{
							Table: &pb.SQLTable{
								Name: "table",
							},
						},
					},
				},
				Status: &pb.ResourceStatus{
					Status:       pb.ResourceStatus_NO_STATUS,
					ErrorMessage: "error",
					ErrorStatus:  &pb.ErrorStatus{},
				},
				Trainingsets: []*pb.NameVariant{},
				LastUpdated:  &timestamppb.Timestamp{},
				Schedule:     "* * * * *",
				Tags:         &pb.Tags{},
				Properties:   &pb.Properties{},
			},
			SourceVariantResource{
				Created:        time.UnixMilli(0).UTC(),
				Description:    "description",
				Name:           "name",
				Variant:        "variant",
				Owner:          "owner",
				Provider:       "provider",
				Status:         "NO_STATUS",
				Error:          "error",
				TrainingSets:   nil,
				Tags:           Tags{},
				Properties:     Properties{},
				Definition:     "table",
				LastUpdated:    time.UnixMilli(0).UTC(),
				Schedule:       "* * * * *",
				SourceType:     "Primary Table",
				Specifications: map[string]string{},
				Inputs:         NameVariants{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			variant := &SourceVariant{
				serialized:           tt.serialized,
				fetchTrainingSetsFns: fetchTrainingSetsFns{tt.serialized},
				fetchProviderFns:     fetchProviderFns{tt.serialized},
				createdFn:            createdFn{tt.serialized},
				lastUpdatedFn:        lastUpdatedFn{tt.serialized},
				protoStringer:        protoStringer{tt.serialized},
				fetchTagsFn:          fetchTagsFn{tt.serialized},
				fetchPropertiesFn:    fetchPropertiesFn{tt.serialized},
			}
			if got := variant.ToShallowMap(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ToShallowMap() = \n%#v, \nwant \n%#v", got, tt.want)
			}
		})
	}
}

func TestTrainingSetVariant_ToShallowMap(t *testing.T) {
	tests := []struct {
		name       string
		serialized *pb.TrainingSetVariant
		want       TrainingSetVariantResource
	}{
		{
			"Simple",
			&pb.TrainingSetVariant{
				Name:        "name",
				Variant:     "variant",
				Created:     &timestamppb.Timestamp{},
				Owner:       "owner",
				Description: "description",
				Provider:    "provider",
				Label: &pb.NameVariant{
					Name:    "label name",
					Variant: "label variant",
				},
				Features: []*pb.NameVariant{
					{Name: "feature name", Variant: "feature variant"},
				},
				Status: &pb.ResourceStatus{
					Status:       pb.ResourceStatus_NO_STATUS,
					ErrorMessage: "error",
					ErrorStatus:  &pb.ErrorStatus{},
				},
				LastUpdated: &timestamppb.Timestamp{},
				Schedule:    "* * * * *",
				Tags:        &pb.Tags{},
				Properties:  &pb.Properties{},
			},
			TrainingSetVariantResource{
				Created:     time.UnixMilli(0).UTC(),
				Description: "description",
				Name:        "name",
				Variant:     "variant",
				Owner:       "owner",
				Provider:    "provider",
				Label:       NameVariant{Name: "label name", Variant: "label variant"},
				Status:      "NO_STATUS",
				Error:       "error",
				Tags:        Tags{},
				Properties:  Properties{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			variant := &TrainingSetVariant{
				serialized:        tt.serialized,
				fetchProviderFns:  fetchProviderFns{tt.serialized},
				createdFn:         createdFn{tt.serialized},
				lastUpdatedFn:     lastUpdatedFn{tt.serialized},
				protoStringer:     protoStringer{tt.serialized},
				fetchTagsFn:       fetchTagsFn{tt.serialized},
				fetchPropertiesFn: fetchPropertiesFn{tt.serialized},
			}
			if got := variant.ToShallowMap(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ToShallowMap() = \n%#v, \nwant \n%#v", got, tt.want)
			}
		})
	}
}

func TestSnowflakeDynamicTableConfig(t *testing.T) {
	tests := []struct {
		name        string
		serialized  *pb.SourceVariant
		want        *ResourceSnowflakeConfig
		expectError bool
	}{
		{
			"Without Config",
			getSQLTransformationSourceVariant(nil),
			nil,
			false,
		},
		{
			"With Config",
			getSQLTransformationSourceVariant(&pb.ResourceSnowflakeConfig{
				DynamicTableConfig: &pb.SnowflakeDynamicTableConfig{
					TargetLag:   "1 days",
					RefreshMode: pb.RefreshMode_REFRESH_MODE_AUTO,
					Initialize:  pb.Initialize_INITIALIZE_ON_CREATE,
				},
			}),
			&ResourceSnowflakeConfig{
				DynamicTableConfig: &SnowflakeDynamicTableConfig{
					TargetLag:   "1 days",
					RefreshMode: "AUTO",
					Initialize:  "ON_CREATE",
				},
			},
			false,
		},
		{
			"Unspecified Refresh Mode",
			getSQLTransformationSourceVariant(&pb.ResourceSnowflakeConfig{
				DynamicTableConfig: &pb.SnowflakeDynamicTableConfig{
					TargetLag:   "1 days",
					RefreshMode: pb.RefreshMode_REFRESH_MODE_UNSPECIFIED,
					Initialize:  pb.Initialize_INITIALIZE_ON_CREATE,
				},
			}),
			nil,
			true,
		},
		{
			"Unspecified Initialize",
			getSQLTransformationSourceVariant(&pb.ResourceSnowflakeConfig{
				DynamicTableConfig: &pb.SnowflakeDynamicTableConfig{
					TargetLag:   "1 days",
					RefreshMode: pb.RefreshMode_REFRESH_MODE_AUTO,
					Initialize:  pb.Initialize_INITIALIZE_UNSPECIFIED,
				},
			}),
			nil,
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			variant := &SourceVariant{
				serialized: tt.serialized,
			}
			got, err := variant.ResourceSnowflakeConfig()
			if (err != nil) != tt.expectError {
				t.Errorf("SnowflakeDynamicTableConfig() error = %v, wantErr %v", err, tt.expectError)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SnowflakeDynamicTableConfig() = %v, want %v", got, tt.want)
			}
		})
	}
}

func getSQLTransformationSourceVariant(resourceSnowflakeConfig *pb.ResourceSnowflakeConfig) *pb.SourceVariant {
	return &pb.SourceVariant{
		Name:    "name",
		Variant: "variant",
		Definition: &pb.SourceVariant_Transformation{
			Transformation: &pb.Transformation{
				Type: &pb.Transformation_SQLTransformation{
					SQLTransformation: &pb.SQLTransformation{
						Query:                   "SELECT * FROM table",
						Source:                  []*pb.NameVariant{{Name: "source", Variant: "source_variant"}},
						ResourceSnowflakeConfig: resourceSnowflakeConfig,
					},
				},
				Location: &pb.Transformation_Catalog{},
			},
		},
	}
}

func TestRefreshMode(t *testing.T) {
	tests := []struct {
		name        string
		proto       pb.RefreshMode
		want        RefreshMode
		expectError bool
	}{
		{"Unspecified", pb.RefreshMode_REFRESH_MODE_UNSPECIFIED, "", true},
		{"Auto", pb.RefreshMode_REFRESH_MODE_AUTO, AutoRefresh, false},
		{"FULL", pb.RefreshMode_REFRESH_MODE_FULL, FullRefresh, false},
		{"Incremental", pb.RefreshMode_REFRESH_MODE_INCREMENTAL, IncrementalRefresh, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rm, err := RefreshModeFromProto(tt.proto)
			if (err != nil) != tt.expectError {
				t.Errorf("RefreshMode() error = %v, wantErr %v", err, tt.expectError)
			}
			if !tt.expectError && !reflect.DeepEqual(rm, tt.want) {
				t.Errorf("RefreshMode() = %v, want %v", rm, tt.want)
			}
		})
	}
}

func TestInitialize(t *testing.T) {
	tests := []struct {
		name        string
		proto       pb.Initialize
		want        Initialize
		expectError bool
	}{
		{"Unspecified", pb.Initialize_INITIALIZE_UNSPECIFIED, "", true},
		{"OnCreate", pb.Initialize_INITIALIZE_ON_CREATE, InitializeOnCreate, false},
		{"OnSchedule", pb.Initialize_INITIALIZE_ON_SCHEDULE, InitializeOnSchedule, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i, err := InitializeFromProto(tt.proto)
			if (err != nil) != tt.expectError {
				t.Errorf("Initialize() error = %v, wantErr %v", err, tt.expectError)
			}
			if !tt.expectError && !reflect.DeepEqual(i, tt.want) {
				t.Errorf("Initialize() = %v, want %v", i, tt.want)
			}
		})
	}
}

func TestSnowflakeDynamicTableConfigMerge(t *testing.T) {
	tests := []struct {
		name      string
		resConfig *ResourceSnowflakeConfig
		config    *pc.SnowflakeConfig
		want      *ResourceSnowflakeConfig
		expectErr bool
	}{
		{
			"Empty Catalog",
			&ResourceSnowflakeConfig{
				DynamicTableConfig: &SnowflakeDynamicTableConfig{
					TargetLag:   "1 days",
					RefreshMode: AutoRefresh,
					Initialize:  InitializeOnCreate,
				},
			},
			&pc.SnowflakeConfig{},
			&ResourceSnowflakeConfig{
				DynamicTableConfig: &SnowflakeDynamicTableConfig{
					TargetLag:   "1 days",
					RefreshMode: AutoRefresh,
					Initialize:  InitializeOnCreate,
				},
			},
			true,
		},
		{
			"Empty Catalog And Config",
			&ResourceSnowflakeConfig{},
			&pc.SnowflakeConfig{},
			&ResourceSnowflakeConfig{
				DynamicTableConfig: &SnowflakeDynamicTableConfig{},
			},
			true,
		},
		{
			"No Snowflake Config Overrides",
			&ResourceSnowflakeConfig{
				DynamicTableConfig: &SnowflakeDynamicTableConfig{
					TargetLag:   "1 days",
					RefreshMode: AutoRefresh,
					Initialize:  InitializeOnCreate,
				},
				Warehouse: "resource-level-warehouse",
			},
			&pc.SnowflakeConfig{
				Catalog: &pc.SnowflakeCatalogConfig{
					ExternalVolume: "external_volume",
					BaseLocation:   "base_location",
				},
				Warehouse: "provider-warehouse",
			},
			&ResourceSnowflakeConfig{
				DynamicTableConfig: &SnowflakeDynamicTableConfig{
					ExternalVolume: "external_volume",
					BaseLocation:   "base_location",
					TargetLag:      "1 days",
					RefreshMode:    AutoRefresh,
					Initialize:     InitializeOnCreate,
				},
				Warehouse: "resource-level-warehouse",
			},
			false,
		},
		{
			"Snowflake Config Overrides",
			&ResourceSnowflakeConfig{
				DynamicTableConfig: &SnowflakeDynamicTableConfig{},
			},
			&pc.SnowflakeConfig{
				Catalog: &pc.SnowflakeCatalogConfig{
					ExternalVolume: "external_volume",
					BaseLocation:   "base_location",
					TableConfig: pc.SnowflakeTableConfig{
						TargetLag:   "2 days",
						RefreshMode: "FULL",
						Initialize:  "ON_SCHEDULE",
					},
				},
				Warehouse: "warehouse",
			},
			&ResourceSnowflakeConfig{
				DynamicTableConfig: &SnowflakeDynamicTableConfig{
					ExternalVolume: "external_volume",
					BaseLocation:   "base_location",
					TargetLag:      "2 days",
					RefreshMode:    FullRefresh,
					Initialize:     InitializeOnSchedule,
				},
				Warehouse: "warehouse",
			},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.resConfig.Merge(tt.config)
			if (err != nil) != tt.expectErr {
				t.Errorf("Merge() error = %v", err)
			}
			if !reflect.DeepEqual(tt.resConfig, tt.want) {
				t.Errorf("Merge() = %v, want %v", tt.config, tt.want)
			}
		})
	}
}

func TestResourceSnowflakeConfigValidation(t *testing.T) {
	tests := []struct {
		name      string
		config    *ResourceSnowflakeConfig
		expectErr bool
	}{
		{
			"Valid Config",
			&ResourceSnowflakeConfig{
				DynamicTableConfig: &SnowflakeDynamicTableConfig{
					ExternalVolume: "external_volume",
					BaseLocation:   "base_location",
					TargetLag:      "1 days",
					RefreshMode:    AutoRefresh,
					Initialize:     InitializeOnCreate,
				},
				Warehouse: "warehouse",
			},
			false,
		},
		{
			"Valid Config with minimum lag",
			&ResourceSnowflakeConfig{
				DynamicTableConfig: &SnowflakeDynamicTableConfig{
					ExternalVolume: "external_volume",
					BaseLocation:   "base_location",
					TargetLag:      "1 minutes",
					RefreshMode:    AutoRefresh,
					Initialize:     InitializeOnCreate,
				},
				Warehouse: "warehouse",
			},
			false,
		},
		{
			"Empty Table Config",
			&ResourceSnowflakeConfig{
				DynamicTableConfig: &SnowflakeDynamicTableConfig{},
			},
			true,
		},
		{
			"Invalid Target Lag (units must be plural)",
			&ResourceSnowflakeConfig{
				DynamicTableConfig: &SnowflakeDynamicTableConfig{
					TargetLag:   "1 day",
					RefreshMode: AutoRefresh,
					Initialize:  InitializeOnCreate,
				},
			},
			true,
		},
		{
			"Invalid Target Lag (minimum lag is 1 minutes)",
			&ResourceSnowflakeConfig{
				DynamicTableConfig: &SnowflakeDynamicTableConfig{
					TargetLag:   "59 seconds",
					RefreshMode: AutoRefresh,
					Initialize:  InitializeOnCreate,
				},
			},
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.expectErr {
				t.Errorf("Validate() error = %v", err)
			}
		})
	}
}
