package provider_config

import (
	ss "github.com/featureform/helpers/string_set"
)

type RedisConfig struct {
	DefaultProviderConfig
	Prefix   string
	Addr     string
	Password string
	DB       int
}

func (r RedisConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Password": true,
	}
}
