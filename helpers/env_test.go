package helpers

import (
	"os"
	"testing"
)

func TestGetEnv(t *testing.T) {
	type args struct {
		key      string
		fallback string
	}
	type testKey struct {
		key   string
		value string
	}
	tests := []struct {
		name   string
		args   args
		setKey testKey
		want   string
	}{
		{name: "Test Fallback", args: args{"INVALID_ENV_VAR", "8888"}, setKey: testKey{"", ""}, want: "8888"},
		{name: "Test Fallback", args: args{"VALID_ENV_VAR", "8888"}, setKey: testKey{"VALID_ENV_VAR", "1234"}, want: "1234"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setKey.key != "" {
				err := os.Setenv(tt.setKey.key, tt.setKey.value)
				if err != nil {
					t.Errorf("Could not set key: %v", err)
				}
			}
			if got := GetEnv(tt.args.key, tt.args.fallback); got != tt.want {
				t.Errorf("GetEnv() = %v, want %v", got, tt.want)
			}
			os.Clearenv()
		})
	}
}
