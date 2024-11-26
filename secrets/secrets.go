package secrets

import (
	"encoding/json"
	"fmt"
	"github.com/featureform/fferr"
	"os"
)

type Provider interface {
	SerializedConfig() []byte
}

type Manager interface {
	GetSecretProvider(providerName string) (Provider, error)
}

type SecretType string

const (
	TypeStatic      SecretType = "STATIC"
	TypeEnvironment SecretType = "ENVIRONMENT"
)

// Secret defines the interface for all secret types
type Secret[T SupportedTypes] interface {
	Serialize() ([]byte, error)
	Deserialize([]byte) error
	Resolve(secretManager Manager) (T, error)
	get() (T, error)
}

type baseSecret struct {
	Type SecretType `json:"type"`
}

// StaticSecret implements Secret for static values and will ensure backward compatibility
type StaticSecret[T SupportedTypes] struct {
	baseSecret
	Value T `json:"value"`
}

func NewStaticSecret[T SupportedTypes](value T) Secret[T] {
	return &StaticSecret[T]{
		baseSecret: baseSecret{Type: TypeStatic},
		Value:      value,
	}
}

func (s *StaticSecret[T]) get() (T, error) {
	return s.Value, nil
}

func (s *StaticSecret[T]) Serialize() ([]byte, error) {
	return json.Marshal(s)
}

func (s *StaticSecret[T]) Deserialize(data []byte) error {
	return json.Unmarshal(data, s)
}

func (s *StaticSecret[T]) Resolve(_ Manager) (T, error) {
	return s.Value, nil
}

// EnvironmentSecret implements Secret for environment variables
type EnvironmentSecret[T SupportedTypes] struct {
	baseSecret
	Key string `json:"key"`
}

func (e *EnvironmentSecret[T]) get() (T, error) {
	val, exists := os.LookupEnv(e.Key)
	if !exists {
		return *new(T), fferr.NewInternalErrorf("environment variable %s not set", e.Key)
	}
	return convertStringToType[T](val)
}

func (e *EnvironmentSecret[T]) Serialize() ([]byte, error) {
	return json.Marshal(e)
}

func (e *EnvironmentSecret[T]) Deserialize(data []byte) error {
	return json.Unmarshal(data, e)
}

func (e *EnvironmentSecret[T]) Resolve(_ Manager) (T, error) {
	return e.get()
}

// DeserializeSecret creates a new Secret instance from serialized data
func DeserializeSecret[T SupportedTypes](message json.RawMessage) (Secret[T], error) {
	if message == nil {
		return NewStaticSecret[T](*new(T)), nil
	}

	if len(message) == 0 {
		return nil, fferr.NewInternalErrorf("empty data provided")
	}

	var base baseSecret
	if err := json.Unmarshal(message, &base); err != nil {
		// This ensures backward compatibility with static secrets!
		var value T
		if err := json.Unmarshal(message, &value); err == nil {
			return NewStaticSecret(value), nil
		}
		return nil, fmt.Errorf("failed to determine secret type: %w", err)
	}

	var secret Secret[T]
	switch base.Type {
	case TypeStatic:
		secret = &StaticSecret[T]{}
	case TypeEnvironment:
		secret = &EnvironmentSecret[T]{}
	default:
		return nil, fferr.NewInternalErrorf("unknown secret type: %s", base.Type)
	}

	if err := secret.Deserialize(message); err != nil {
		return nil, fmt.Errorf("failed to deserialize secret: %w", err)
	}

	return secret, nil
}

type ErrorGuard struct {
	err error
}

func (d *ErrorGuard) Err() error {
	return d.err
}

func TryDeserializeSecret[T SupportedTypes](ea *ErrorGuard, message json.RawMessage) Secret[T] {
	if ea.err != nil {
		return nil
	}

	secret, err := DeserializeSecret[T](message)
	if err != nil {
		return nil
	}

	return secret
}

func TryResolveSecret[T SupportedTypes](ea *ErrorGuard, secret Secret[T], secretManager Manager) T {
	if ea.err != nil {
		return *new(T)
	}

	value, err := secret.Resolve(secretManager)
	if err != nil {
		ea.err = err
		return *new(T)
	}

	return value
}
