package helpers

import (
	"fmt"
	"os"
	"strconv"
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

func GetEnvBool(key string, fallback bool) bool {
	return getEnvGeneric(key, fallback, func(val string) (interface{}, error) {
		parsedValue, err := strconv.ParseBool(val)
		return parsedValue, err
	}).(bool)
}

func IsDebugEnv() bool {
	return GetEnvBool("DEBUG", false)
}
