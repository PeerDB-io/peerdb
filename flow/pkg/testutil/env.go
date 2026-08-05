package testutil

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/joho/godotenv"
	"github.com/stretchr/testify/require"
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

func ClickHouseTestHost() string {
	host, ok := os.LookupEnv("CI_CLICKHOUSE_HOST")
	if !ok {
		return "localhost"
	}
	return host
}

func ClickHouseTestPort() uint32 {
	portString, ok := os.LookupEnv("CI_CLICKHOUSE_NATIVE_PORT")
	if !ok {
		return 9000
	}
	port, err := strconv.ParseUint(portString, 10, 16)
	if err != nil {
		return 9000
	}
	return uint32(port)
}

type MongoTestCredentials struct {
	URI      string
	Username string
	Password string
}

func MongoAdminTestCredentials(t *testing.T) MongoTestCredentials {
	t.Helper()
	creds := MongoTestCredentials{
		URI:      os.Getenv("CI_MONGO_ADMIN_URI"),
		Username: os.Getenv("CI_MONGO_ADMIN_USERNAME"),
		Password: os.Getenv("CI_MONGO_ADMIN_PASSWORD"),
	}
	require.NotEmpty(t, creds.URI, "missing CI_MONGO_ADMIN_URI env var")
	require.NotEmpty(t, creds.Username, "missing CI_MONGO_ADMIN_USERNAME env var")
	require.NotEmpty(t, creds.Password, "missing CI_MONGO_ADMIN_PASSWORD env var")
	return creds
}

func MongoUserTestCredentials(t *testing.T) MongoTestCredentials {
	t.Helper()
	creds := MongoTestCredentials{
		URI:      os.Getenv("CI_MONGO_URI"),
		Username: os.Getenv("CI_MONGO_USERNAME"),
		Password: os.Getenv("CI_MONGO_PASSWORD"),
	}
	require.NotEmpty(t, creds.URI, "missing CI_MONGO_URI env var")
	require.NotEmpty(t, creds.Username, "missing CI_MONGO_USERNAME env var")
	require.NotEmpty(t, creds.Password, "missing CI_MONGO_PASSWORD env var")
	return creds
}
