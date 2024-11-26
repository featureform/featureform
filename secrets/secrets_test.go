package secrets

import (
	"encoding/json"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func setEnvVariable(t *testing.T, key, value string) {
	err := os.Setenv(key, value)
	if err != nil {
		t.Fatalf("Failed to set environment variable: %v", err)
	}
	t.Cleanup(func() {
		os.Unsetenv(key)
	})
}

func testEnvironmentSecret[T SupportedTypes](t *testing.T, expectedValue T, envValue string) {
	key := uuid.New().String()

	setEnvVariable(t, key, envValue)

	secret := &EnvironmentSecret[T]{
		Key: key,
	}

	value, err := secret.get()
	assert.NoError(t, err)
	assert.Equal(t, expectedValue, value)
}

func TestEnvironmentSecretString(t *testing.T) {
	testEnvironmentSecret[string](t, "env-value", "env-value")
}

func TestEnvironmentSecret(t *testing.T) {
	testEnvironmentSecret[int](t, 42, "42")
}

func TestEnvironmentSecretBool(t *testing.T) {
	testEnvironmentSecret[bool](t, true, "true")
}

func TestEnvironmentValueRetrieverMissingEnv(t *testing.T) {
	key := uuid.New().String()

	secret := &EnvironmentSecret[string]{
		Key: key,
	}

	_, err := secret.get()
	assert.Error(t, err)
}

func TestEnvironmentSecret_Serde(t *testing.T) {
	key := uuid.New().String()
	retriever := &EnvironmentSecret[string]{
		Key: key,
	}

	data, err := retriever.Serialize()
	assert.NoError(t, err)

	deser := &EnvironmentSecret[string]{}
	err = deser.Deserialize(data)
	assert.NoError(t, err)

	assert.Equal(t, retriever, deser)
}

func TestStaticSecret_Serde(t *testing.T) {
	value := "test-value"
	retriever := NewStaticSecret(value)

	data, err := retriever.Serialize()
	assert.NoError(t, err)

	deser := &StaticSecret[string]{}
	err = deser.Deserialize(data)
	assert.NoError(t, err)

	assert.Equal(t, retriever, deser)
}

func TestDeserializeSecret(t *testing.T) {
	t.Run("static secret fallback", func(t *testing.T) {
		input := []byte(`"test-value"`)
		secret, err := DeserializeSecret[string](input)
		assert.NoError(t, err)
		assert.Equal(t, NewStaticSecret("test-value"), secret)
	})

	t.Run("static secret", func(t *testing.T) {
		input := NewStaticSecret("test-value")
		serialized, err := input.Serialize()
		assert.NoError(t, err)

		secret, err := DeserializeSecret[string](serialized)
		assert.NoError(t, err)

		assert.Equal(t, input, secret)
	})

	t.Run("environment secret", func(t *testing.T) {
		input := EnvironmentSecret[string]{
			baseSecret: baseSecret{Type: TypeEnvironment},
			Key:        "TEST_ENV_VAR",
		}

		data, err := json.Marshal(input)
		assert.NoError(t, err)

		secret, err := DeserializeSecret[string](data)
		assert.NoError(t, err)

		assert.Equal(t, &input, secret)
	})

	t.Run("error cases", func(t *testing.T) {
		tests := []struct {
			name    string
			input   []byte
			wantErr string
		}{
			{
				name:    "empty data",
				input:   []byte{},
				wantErr: "empty data provided",
			},
			{
				name:    "invalid json",
				input:   []byte(`{"type": "INVALID"`),
				wantErr: "failed to determine secret type",
			},
			{
				name:    "unknown secret type",
				input:   []byte(`{"type": "UNKNOWN"}`),
				wantErr: "unknown secret type",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, err := DeserializeSecret[string](tt.input)
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			})
		}
	})
}
