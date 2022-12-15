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
								},
							},
						},
					},
				},
			},
			KubernetesArgs{
				DockerImage: "",
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
	}
	tests := []struct {
		name    string
		fields  fields
		want    []byte
		wantErr bool
	}{
		{"Empty", fields{""}, createJson(t, map[string]string{"Docker Image": ""}), false},
		{"With Image", fields{"my/test:image"}, createJson(t, map[string]string{"Docker Image": "my/test:image"}), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			arg := KubernetesArgs{
				DockerImage: tt.fields.DockerImage,
			}
			got, err := arg.Format()
			if (err != nil) != tt.wantErr {
				t.Errorf("Format() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				var gotMap map[string]string
				var wantMap map[string]string
				err := json.Unmarshal(got, &gotMap)
				if err != nil {
					t.Fatalf("could not unmarshal gotten bytes: %s", err.Error())
				}
				err = json.Unmarshal(tt.want, &wantMap)
				if err != nil {
					t.Fatalf("could not unmarshal gotten bytes: %s", err.Error())
				}
				t.Errorf("Format() got = %v, want %v", gotMap, wantMap)
			}
		})
	}
}
