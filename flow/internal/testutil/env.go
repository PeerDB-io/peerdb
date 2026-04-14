package testutil

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/joho/godotenv"
)

// LoadEnv walks up from the current directory until the project root
// is found and loads the .env file if it exists
func LoadEnv() {
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
