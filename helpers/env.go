package helpers

import "os"

// GetEnv Takes a environment variable key and returns the value if it exists.
// Otherwise returns the fallback value provided
func GetEnv(key, fallback string) string {
	value, has := os.LookupEnv(key)
	if !has {
		return fallback
	}
	return value
}
