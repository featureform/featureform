package metadata

import (
	"encoding/json"
	pb "github.com/featureform/metadata/proto"
	"reflect"
	"testing"
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
									Spec: &pb.KubernetesResourceSpecs{
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
