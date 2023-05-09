package helpers

import (
	"os"
	"testing"
)

type getEnvFn interface{}

func TestGetEnv(t *testing.T) {
	type args struct {
		key      string
		fallback interface{}
	}
	type testKey struct {
		key   string
		value string
	}
	tests := []struct {
		name   string
		args   args
		setKey testKey
		want   interface{}
		testFn getEnvFn
	}{
		{name: "Test GetEnv Fallback", args: args{"INVALID_ENV_VAR", "8888"}, setKey: testKey{"", ""}, want: "8888", testFn: GetEnv},
		{name: "Test GetEnv", args: args{"VALID_ENV_VAR", "8888"}, setKey: testKey{"VALID_ENV_VAR", "1234"}, want: "1234", testFn: GetEnv},
		{name: "Test GetEnvInt Fallback", args: args{"INVALID_ENV_VAR", 8888}, setKey: testKey{"", ""}, want: 8888, testFn: GetEnvInt},
		{name: "Test GetEnvInt", args: args{"VALID_ENV_VAR", 8888}, setKey: testKey{"VALID_ENV_VAR", "1234"}, want: 1234, testFn: GetEnvInt},
		{name: "Test GetEnvInt32 Fallback", args: args{"INVALID_ENV_VAR", int32(8888)}, setKey: testKey{"", ""}, want: int32(8888), testFn: GetEnvInt32},
		{name: "Test GetEnvInt32", args: args{"VALID_ENV_VAR", int32(8888)}, setKey: testKey{"VALID_ENV_VAR", "1234"}, want: int32(1234), testFn: GetEnvInt32},
		{name: "Test GetEnvBool Fallback", args: args{"INVALID_ENV_VAR", true}, setKey: testKey{"", ""}, want: true, testFn: GetEnvBool},
		{name: "Test GetEnvBool", args: args{"VALID_ENV_VAR", true}, setKey: testKey{"VALID_ENV_VAR", "false"}, want: false, testFn: GetEnvBool},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setKey.key != "" {
				err := os.Setenv(tt.setKey.key, tt.setKey.value)
				if err != nil {
					t.Errorf("Could not set key: %v", err)
				}
			}
			var got interface{}
			switch fn := tt.testFn.(type) {
			case func(string, string) string:
				got = fn(tt.args.key, tt.args.fallback.(string))
			case func(string, int) int:
				got = fn(tt.args.key, tt.args.fallback.(int))
			case func(string, bool) bool:
				got = fn(tt.args.key, tt.args.fallback.(bool))
			}
			if got != tt.want {
				t.Errorf("%T() = %v, want %v", tt.testFn, got, tt.want)
			}
			os.Clearenv()
		})
	}
}
