package provider

import (
	"fmt"
)

type SerializedConfig []byte

type Provider interface {
	AsOnlineStore() (OnlineStore, error)
}

type Factory func(SerializedConfig) (Provider, error)

type Type string

var factories map[Type]Factory = make(map[Type]Factory)

func RegisterFactory(t Type, f Factory) error {
	if _, has := factories[t]; has {
		return fmt.Errorf("%s provider factory already exists.", t)
	}
	factories[t] = f
	return nil
}

func Get(t Type, config SerializedConfig) (Provider, error) {
	f, has := factories[t]
	if !has {
		return nil, fmt.Errorf("No provider of type: %s", t)
	}
	return f(config)
}
