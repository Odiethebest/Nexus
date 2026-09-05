// Package envutil loads a .env file from disk into the process environment.
// Reading an individual environment variable back out, with a typed fallback,
// is nexus/internal/envconf.
package envutil

import (
	"errors"
	"fmt"
	"os"

	"github.com/joho/godotenv"
)

var defaultDotEnvCandidates = []string{
	".env",
	"../.env",
	"../../.env",
}

// LoadDotEnvIfPresent loads the first .env file found in common working
// directories. Existing process env vars are not overridden.
//
// Returns the loaded file path, or an empty string when no .env file exists.
func LoadDotEnvIfPresent() (string, error) {
	for _, path := range defaultDotEnvCandidates {
		if _, err := os.Stat(path); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return "", fmt.Errorf("envutil: stat %s: %w", path, err)
		}

		if err := godotenv.Load(path); err != nil {
			return "", fmt.Errorf("envutil: load %s: %w", path, err)
		}
		return path, nil
	}

	return "", nil
}
