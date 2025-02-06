package proxy_client

import (
	"fmt"
	"testing"

	"github.com/featureform/logging"
	"github.com/stretchr/testify/assert"
)

func TestGetProxyClient_Validations(t *testing.T) {
	tests := []struct {
		name        string
		source      string
		variant     string
		host        string
		port        string
		limit       int
		envVars     map[string]string
		expectedMsg string
	}{
		{
			name:        "missing host",
			source:      "someSource",
			variant:     "someVariant",
			host:        "",
			port:        "8086",
			limit:       10,
			expectedMsg: "missing 'host' param value",
		},
		{
			name:        "missing port",
			source:      "someSource",
			variant:     "someVariant",
			host:        "localhost",
			port:        "",
			limit:       10,
			expectedMsg: "missing 'port' param value",
		},
		{
			name:        "missing source",
			source:      "",
			variant:     "someVariant",
			host:        "localhost",
			port:        "8086",
			limit:       10,
			expectedMsg: "missing 'source' param value",
		},
		{
			name:        "missing variant",
			source:      "someSource",
			variant:     "",
			host:        "localhost",
			port:        "8086",
			limit:       10,
			expectedMsg: "missing 'variant' param value",
		},
		{
			name:        "limit less than 0",
			source:      "someSource",
			variant:     "someVariant",
			host:        "localhost",
			port:        "8086",
			limit:       -1,
			expectedMsg: "limit value (-1) is less than 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for key, value := range tt.envVars {
				t.Setenv(key, value)
			}

			params := ProxyRequest{
				Query: ProxyQuery{
					Source:  tt.source,
					Variant: tt.variant,
					Limit:   tt.limit,
				},
				Config: ProxyConfig{
					Host: tt.host,
					Port: tt.port,
				},
			}

			ctx := logging.NewTestContext(t)
			client, err := GetStreamProxyClient(ctx, params)

			assert.Nil(t, client, "Expected client to be nil")
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectedMsg, fmt.Sprintf("Expected error to contain: %s", tt.expectedMsg))
		})
	}
}
