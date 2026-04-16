package testutil

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/joho/godotenv"
)

const timeZoneEnvKey = "TZ"

var loadedEnv sync.Once

func forceTimeZone(tzString string) error {
	location, err := time.LoadLocation(tzString)
	if err != nil {
		return err
	}
	time.Local = location
	return nil
}

// LoadEnv walks up from the current directory until the project root
// is found and loads the .env file if it exists.
// After the first call, subsequent calls to LoadEnv are no-ops.
func LoadEnv() {
	loadedEnv.Do(func() {
		loadEnvOnce()
	})
}

func loadEnvOnce() {
	ctx := context.Background()
	dir, err := os.Getwd()
	if err != nil {
		slog.ErrorContext(ctx, "LoadEnv: failed to get working directory", "error", err)
		return
	}

	slog.InfoContext(ctx, "LoadEnv: starting search", "cwd", dir)

	var lastVisited string

	rootReached := false

	for !rootReached {
		envPath := filepath.Join(dir, ".env")
		if _, err := os.Stat(envPath); err == nil && filepath.Base(lastVisited) == "flow" {
			slog.InfoContext(ctx, "LoadEnv: found .env", "path", envPath)
			if err := godotenv.Load(envPath); err != nil {
				slog.ErrorContext(ctx, "LoadEnv: failed to load .env", "path", envPath, "error", err)
			}

			maybeForcedTZ := os.Getenv(timeZoneEnvKey)

			if maybeForcedTZ != "" {
				slog.InfoContext(ctx, "LoadEnv: attempting to force time zone from environment variable",
					"key", timeZoneEnvKey, "forced_value", maybeForcedTZ)
				if err := forceTimeZone(maybeForcedTZ); err != nil {
					slog.ErrorContext(ctx, "LoadEnv: failed to force time zone", "error", err)
				}
			}

			return
		}

		lastVisited = dir

		parent := filepath.Dir(dir)
		if parent == dir {
			rootReached = true
		}
		dir = parent
	}

	slog.WarnContext(ctx, "LoadEnv: no .env file found above flow directory")
}
