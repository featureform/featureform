package provider

import (
	"encoding/json"
	"fmt"
)

type SerializedConfig []byte

type RedisConfig struct {
	Prefix   string `default:"featureform_table__"`
	Addr     string
	Password string
	DB       int
}

func (r RedisConfig) Serialized() SerializedConfig {
	config, err := json.Marshal(r)
	if err != nil {
		panic(err)
	}
	return config
}

func (r *RedisConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, r)
	if err != nil {
		return err
	}
	return nil
}

type Provider interface {
	AsOnlineStore() (OnlineStore, error)
}

type Factory func(SerializedConfig) (Provider, error)

type Type string

var factories map[Type]Factory = make(map[Type]Factory)

func RegisterFactory(t Type, f Factory) error {
	if _, has := factories[t]; has {
		return fmt.Errorf("%s provider factory already exists", t)
	}
	factories[t] = f
	return nil
}

func Get(t Type, config SerializedConfig) (Provider, error) {
	f, has := factories[t]
	if !has {
		return nil, fmt.Errorf("no provider of type: %s", t)
	}
	return f(config)
}
