package testutil

import (
	"log/slog"
	"os"
	"path/filepath"

	"github.com/joho/godotenv"
)

// LoadEnv walks up from the current directory until the project root
// is found and loads the .env file if it exists
func LoadEnv() {
	dir, err := os.Getwd()
	if err != nil {
		slog.Error("LoadEnv: failed to get working directory", "error", err)
		return
	}

	slog.Info("LoadEnv: starting search", "cwd", dir)

	var lastVisited string

	rootReached := false

	for !rootReached {
		envPath := filepath.Join(dir, ".env")
		if _, err := os.Stat(envPath); err == nil && filepath.Base(lastVisited) == "flow" {
			slog.Info("LoadEnv: found .env", "path", envPath)
			godotenv.Load(envPath)
			return
		}

		lastVisited = dir

		parent := filepath.Dir(dir)
		if parent == dir {
			rootReached = true
		}
		dir = parent
	}

	slog.Warn("LoadEnv: no .env file found above flow directory")
}
