package config

import "os"

// Get returns the env var value for key, or fallback if unset.
func Get(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
