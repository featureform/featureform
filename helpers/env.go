// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package helpers

import (
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"
)

// GetEnv Takes a environment variable key and returns the value if it exists.
// Otherwise, returns the fallback value provided
func GetEnv(key, fallback string) string {
	value, has := os.LookupEnv(key)
	if !has {
		return fallback
	}
	return value
}

func MustGetTestingEnv(t *testing.T, key string) string {
	value, has := os.LookupEnv(key)
	if !has {
		t.Fatalf("ENV %s not set", key)
	}
	return value
}

type EnvNotFound struct {
	Key string
}

func (err *EnvNotFound) Error() string {
	return fmt.Sprintf("ENV %s not found", err.Key)
}

func LookupEnvDuration(key string) (time.Duration, error) {
	value, has := os.LookupEnv(key)
	if !has {
		return time.Duration(0), &EnvNotFound{key}
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		return time.Duration(0), err
	}
	return duration, nil
}

func getEnvGeneric(key string, fallback interface{}, converter func(string) (interface{}, error)) interface{} {
	value, exists := os.LookupEnv(key)
	if !exists {
		return fallback
	}

	parsedValue, err := converter(value)
	if err != nil {
		fmt.Printf("Error parsing environment variable %s: %s\n", key, err.Error())
		return fallback
	}
	return parsedValue
}

func GetEnvInt(key string, fallback int) int {
	return getEnvGeneric(key, fallback, func(val string) (interface{}, error) {
		parsedValue, err := strconv.Atoi(val)
		return parsedValue, err
	}).(int)
}

func GetEnvInt32(key string, fallback int32) int32 {
	return getEnvGeneric(key, fallback, func(val string) (interface{}, error) {
		parsedValue, err := strconv.ParseInt(val, 10, 32)
		return int32(parsedValue), err
	}).(int32)
}

func GetEnvUInt16(key string, fallback uint16) uint16 {
	return getEnvGeneric(key, fallback, func(val string) (interface{}, error) {
		parsedValue, err := strconv.ParseUint(val, 10, 16)
		return uint16(parsedValue), err
	}).(uint16)
}

func GetEnvBool(key string, fallback bool) bool {
	return getEnvGeneric(key, fallback, func(val string) (interface{}, error) {
		parsedValue, err := strconv.ParseBool(val)
		return parsedValue, err
	}).(bool)
}

func IsDebugEnv() bool {
	return GetEnvBool("DEBUG", false)
}
