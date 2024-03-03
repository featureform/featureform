package provider_config

import (
	"reflect"
	"testing"

	ss "github.com/featureform/helpers/string_set"
)

// TODO!

func TestIKVConfigDifferingFields(t *testing.T) {
	type args struct {
		a IKVConfig
		b IKVConfig
	}

	tests := []struct {
		name     string
		args     args
		expected ss.StringSet
	}{
		{"No Differing Fields", args{
			a: IKVConfig{
				StoreName:      "foo",
				AccountId:      "foo-account-id",
				AccountPasskey: "foo-account-passkey",
				MountDirectory: "/foo",
			},
			b: IKVConfig{
				StoreName:      "foo",
				AccountId:      "foo-account-id",
				AccountPasskey: "foo-account-passkey",
				MountDirectory: "/foo",
			},
		}, ss.StringSet{}},
		{"Differing Fields", args{
			a: IKVConfig{
				StoreName:      "foo",
				AccountId:      "foo-account-id",
				AccountPasskey: "foo-account-passkey",
				MountDirectory: "/foo",
			},
			b: IKVConfig{
				StoreName:      "bar",
				AccountId:      "foo-account-id",
				AccountPasskey: "foo-account-passkey",
				MountDirectory: "/foo",
			},
		}, ss.StringSet{
			"StoreName": true,
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := tt.args.a.DifferingFields(tt.args.b)

			if err != nil {
				t.Errorf("Failed to get differing fields due to error: %v", err)
			}

			if !reflect.DeepEqual(actual, tt.expected) {
				t.Errorf("Expected %v, but instead found %v", tt.expected, actual)
			}

		})
	}

}
