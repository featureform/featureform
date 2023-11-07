package helpers

import "testing"

func TestBuildDashboardUrl(t *testing.T) {
	type args struct {
		host            string
		resourceType    string
		resourceName    string
		resourceVariant string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "Feature Variant",
			args: args{
				host:            "localhost",
				resourceType:    "FEATURE_VARIANT",
				resourceName:    "test",
				resourceVariant: "featureVariant",
			},
			want:    "http://localhost/features/test?variant=featureVariant",
			wantErr: false,
		},
		{
			name: "Source Variant",
			args: args{
				host:            "localhost",
				resourceType:    "SOURCE_VARIANT",
				resourceName:    "test",
				resourceVariant: "sourceVariant",
			},
			want:    "http://localhost/sources/test?variant=sourceVariant",
			wantErr: false,
		},
		{
			name: "Label Variant",
			args: args{
				host:            "localhost",
				resourceType:    "LABEL_VARIANT",
				resourceName:    "test",
				resourceVariant: "labelVariant",
			},
			want:    "http://localhost/labels/test?variant=labelVariant",
			wantErr: false,
		},
		{
			name: "Training Set Variant",
			args: args{
				host:            "gcp.featureform.com",
				resourceType:    "TRAINING_SET_VARIANT",
				resourceName:    "test",
				resourceVariant: "trainingSetVariant",
			},
			want:    "https://gcp.featureform.com/training-sets/test?variant=trainingSetVariant",
			wantErr: false,
		},
		{
			name: "Training Set No Variant",
			args: args{
				host:            "gcp.featureform.com",
				resourceType:    "TRAINING_SET_VARIANT",
				resourceName:    "test",
				resourceVariant: "",
			},
			want:    "https://gcp.featureform.com/training-sets/test",
			wantErr: false,
		},
		{
			name: "Invalid Resource Type",
			args: args{
				host:            "gcp.featureform.com",
				resourceType:    "MODEL",
				resourceName:    "test",
				resourceVariant: "variant",
			},
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				got, err := BuildDashboardUrl(
					tt.args.host,
					tt.args.resourceType,
					tt.args.resourceName,
					tt.args.resourceVariant,
				)
				if (err != nil) && !tt.wantErr {
					t.Errorf("BuildDashboardUrl() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if got != tt.want {
					t.Errorf("BuildDashboardUrl() got = %v, want %v", got, tt.want)
				}
			},
		)
	}
}
