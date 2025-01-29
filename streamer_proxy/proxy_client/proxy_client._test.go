package proxy_client

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetProxyClient_Validations(t *testing.T) {
	tests := []struct {
		name        string
		source      string
		variant     string
		limit       int
		envVars     map[string]string
		expectedMsg string
	}{
		{
			name:        "missing ICEBERG_PROXY_HOST",
			source:      "someSource",
			variant:     "someVariant",
			limit:       10,
			envVars:     map[string]string{"ICEBERG_PROXY_HOST": "", "ICEBERG_PROXY_PORT": "8086"},
			expectedMsg: "missing ICEBERG_PROXY_HOST env variable",
		},
		{
			name:        "missing ICEBERG_PROXY_PORT",
			source:      "someSource",
			variant:     "someVariant",
			limit:       10,
			envVars:     map[string]string{"ICEBERG_PROXY_HOST": "localhost", "ICEBERG_PROXY_PORT": ""},
			expectedMsg: "missing ICEBERG_PROXY_PORT env variable",
		},
		{
			name:        "missing source",
			source:      "",
			variant:     "someVariant",
			limit:       10,
			envVars:     map[string]string{"ICEBERG_PROXY_HOST": "localhost", "ICEBERG_PROXY_PORT": "8086"},
			expectedMsg: "missing 'source' param value",
		},
		{
			name:        "missing variant",
			source:      "someSource",
			variant:     "",
			limit:       10,
			envVars:     map[string]string{"ICEBERG_PROXY_HOST": "localhost", "ICEBERG_PROXY_PORT": "8086"},
			expectedMsg: "missing 'variant' param value",
		},
		{
			name:        "limit less than 0",
			source:      "someSource",
			variant:     "someVariant",
			limit:       -1,
			envVars:     map[string]string{"ICEBERG_PROXY_HOST": "localhost", "ICEBERG_PROXY_PORT": "8086"},
			expectedMsg: "limit value (-1) is less than 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for key, value := range tt.envVars {
				t.Setenv(key, value)
			}

			client, err := GetStreamProxyClient(context.Background(), tt.source, tt.variant, tt.limit)

			assert.Nil(t, client, "Expected client to be nil")
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectedMsg, fmt.Sprintf("Expected error to contain: %s", tt.expectedMsg))
		})
	}
}
