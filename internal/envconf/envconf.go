// Package envconf reads optional configuration from environment variables,
// falling back to a caller-supplied default whenever the variable is unset or
// cannot be parsed.
//
// Reading a single value out of the process environment is all this package
// does; loading a .env file from disk into that environment is
// nexus/internal/envutil.
package envconf

import (
	"os"
	"strconv"
	"strings"
)

// String returns the raw value of key, or fallback when the variable is unset
// or empty. Unlike the typed readers below it deliberately does not trim
// whitespace: a value of " " is a value, not an absence.
func String(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// Bool parses key with strconv.ParseBool, returning fallback when the variable
// is unset, blank, or unparseable. Like the other typed readers it trims
// surrounding whitespace first; String is the one reader that does not.
func Bool(key string, fallback bool) bool {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		return fallback
	}
	return v
}

// Int parses key with strconv.Atoi, returning fallback when the variable is
// unset, blank, or unparseable. The value is trimmed before parsing.
func Int(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return v
}

// Int64 parses key as a base-10 64-bit integer, returning fallback when the
// variable is unset, blank, or unparseable. The value is trimmed before parsing.
func Int64(key string, fallback int64) int64 {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return fallback
	}
	return v
}

// Float64 parses key with strconv.ParseFloat, returning fallback when the
// variable is unset, blank, or unparseable. The value is trimmed before
// parsing. Note that ParseFloat accepts "NaN" and "Inf", so those come back
// verbatim rather than as fallback.
func Float64(key string, fallback float64) float64 {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return fallback
	}
	return v
}
