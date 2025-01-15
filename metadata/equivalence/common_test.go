package equivalence

import (
	pb "github.com/featureform/metadata/proto"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestResourceSnowflakeConfigFromProto(t *testing.T) {
	tests := []struct {
		name     string
		input    *pb.ResourceSnowflakeConfig
		expected resourceSnowflakeConfig
	}{
		{
			name:     "nil input",
			input:    nil,
			expected: resourceSnowflakeConfig{},
		},
		{
			name: "warehouse only",
			input: &pb.ResourceSnowflakeConfig{
				Warehouse: "test_warehouse",
			},
			expected: resourceSnowflakeConfig{
				Warehouse: "test_warehouse",
			},
		},
		{
			name: "full config",
			input: &pb.ResourceSnowflakeConfig{
				Warehouse: "test_warehouse",
				DynamicTableConfig: &pb.SnowflakeDynamicTableConfig{
					TargetLag:   "1h",
					RefreshMode: pb.RefreshMode_REFRESH_MODE_FULL,
					Initialize:  pb.Initialize_INITIALIZE_ON_SCHEDULE,
				},
			},
			expected: resourceSnowflakeConfig{
				Warehouse: "test_warehouse",
				DynamicTableConfig: snowflakeDynamicTableConfig{
					TargetLag:   "1h",
					RefreshMode: "REFRESH_MODE_FULL",
					Initialize:  "INITIALIZE_ON_SCHEDULE",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := resourceSnowflakeConfigFromProto(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
