package metadata

import (
	"encoding/json"
	"google.golang.org/protobuf/types/known/timestamppb"
	"reflect"
	"testing"
	"time"

	pb "github.com/featureform/metadata/proto"
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
	fv := &pb.FeatureVariant{
		Name:    "vector",
		Variant: "vector_variant",
		Source: &pb.NameVariant{
			Name:    "vector_source",
			Variant: "vector_source_variant",
		},
		Type:     "float32",
		Entity:   "vector_entity",
		Owner:    "vector_owner",
		Provider: "vector_provider",
		Location: &pb.FeatureVariant_Columns{
			Columns: &pb.Columns{
				Entity: "vector_entity",
				Value:  "vector_value",
			},
		},
		IsEmbedding: true,
		Dimension:   384,
	}

	wfc := wrapProtoFeatureVariant(fv)

	if wfc.Dimension() != 384 {
		t.Errorf("expected dimension to be 384, got %d", wfc.Dimension())
	}
	if wfc.IsEmbedding() != true {
		t.Errorf("expected embedding to be true, got %v", wfc.IsEmbedding())
	}
}

func TestFeatureVariant_ToShallowMap(t *testing.T) {
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
			Type:    "datatype",
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
			IsEmbedding:          false,
			Dimension:            64,
			AdditionalParameters: &pb.FeatureParameters{},
		}, FeatureVariantResource{
			Created:     time.UnixMilli(0).UTC(),
			Description: "description",
			Entity:      "entity",
			Name:        "name",
			Variant:     "variant",
			Owner:       "owner",
			Provider:    "provider",
			DataType:    "datatype",
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
			Type:    "datatype",
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
			IsEmbedding:          false,
			Dimension:            64,
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
				fetchIsEmbeddingFn:   fetchIsEmbeddingFn{tt.serialized},
				fetchDimensionFn:     fetchDimensionFn{tt.serialized},
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
				Type:    "datatype",
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
				DataType:    "datatype",
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
							Table: &pb.PrimarySQLTable{
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
