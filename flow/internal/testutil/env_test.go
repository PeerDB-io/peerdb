package testutil

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadEnv_FindsEnvAboveFlow(t *testing.T) {
	// Create: tmp/project/.env and tmp/project/flow/e2e/
	// Run from flow/e2e/, expect .env at project/ to be loaded
	tmp := t.TempDir()
	projectDir := filepath.Join(tmp, "project")
	flowDir := filepath.Join(projectDir, "flow")
	e2eDir := filepath.Join(flowDir, "e2e")
	require.NoError(t, os.MkdirAll(e2eDir, 0o755))

	envFile := filepath.Join(projectDir, ".env")
	require.NoError(t, os.WriteFile(envFile, []byte("TEST_LOAD_ENV=found_above_flow\n"), 0o600))

	t.Chdir(e2eDir)

	os.Unsetenv("TEST_LOAD_ENV")
	loadedEnv.Store(false)
	LoadEnv()

	require.Equal(t, "found_above_flow", os.Getenv("TEST_LOAD_ENV"))
}

func TestLoadEnv_IgnoresEnvNotAboveFlow(t *testing.T) {
	// Create: tmp/.env and tmp/notflow/sub/
	// Run from notflow/sub/, expect .env NOT to be loaded (no "flow" ancestor)
	tmp := t.TempDir()
	notflowDir := filepath.Join(tmp, "notflow")
	subDir := filepath.Join(notflowDir, "sub")
	require.NoError(t, os.MkdirAll(subDir, 0o755))

	envFile := filepath.Join(tmp, ".env")
	require.NoError(t, os.WriteFile(envFile, []byte("TEST_LOAD_ENV_IGNORE=should_not_load\n"), 0o600))

	t.Chdir(subDir)

	os.Unsetenv("TEST_LOAD_ENV_IGNORE")
	loadedEnv.Store(false)
	LoadEnv()

	require.Empty(t, os.Getenv("TEST_LOAD_ENV_IGNORE"))
}

func TestLoadEnv_NoEnvFile(t *testing.T) {
	// Create: tmp/project/flow/e2e/ with no .env anywhere
	// Should complete without error
	tmp := t.TempDir()
	e2eDir := filepath.Join(tmp, "project", "flow", "e2e")
	require.NoError(t, os.MkdirAll(e2eDir, 0o755))

	t.Chdir(e2eDir)

	os.Unsetenv("TEST_LOAD_ENV_NONE")
	loadedEnv.Store(false)
	LoadEnv()

	require.Empty(t, os.Getenv("TEST_LOAD_ENV_NONE"))
}

func TestLoadEnv_EnvInsideFlowIgnored(t *testing.T) {
	// Create: tmp/project/flow/.env and tmp/project/flow/e2e/
	// .env is inside flow, not above it — should NOT be loaded
	tmp := t.TempDir()
	projectDir := filepath.Join(tmp, "project")
	flowDir := filepath.Join(projectDir, "flow")
	e2eDir := filepath.Join(flowDir, "e2e")
	require.NoError(t, os.MkdirAll(e2eDir, 0o755))

	envFile := filepath.Join(flowDir, ".env")
	require.NoError(t, os.WriteFile(envFile, []byte("TEST_LOAD_ENV_INSIDE=inside_flow\n"), 0o600))

	t.Chdir(e2eDir)

	os.Unsetenv("TEST_LOAD_ENV_INSIDE")
	loadedEnv.Store(false)
	LoadEnv()

	require.Empty(t, os.Getenv("TEST_LOAD_ENV_INSIDE"))
}

func TestLoadEnv_FromConnectorsSubdir(t *testing.T) {
	// Create: tmp/project/.env and tmp/project/flow/connectors/
	// Run from flow/connectors/, expect .env at project/ to be loaded
	tmp := t.TempDir()
	projectDir := filepath.Join(tmp, "project")
	connectorsDir := filepath.Join(projectDir, "flow", "connectors")
	require.NoError(t, os.MkdirAll(connectorsDir, 0o755))

	envFile := filepath.Join(projectDir, ".env")
	require.NoError(t, os.WriteFile(envFile, []byte("TEST_LOAD_ENV_CONNECTORS=from_connectors\n"), 0o600))

	t.Chdir(connectorsDir)

	os.Unsetenv("TEST_LOAD_ENV_CONNECTORS")
	loadedEnv.Store(false)
	LoadEnv()

	require.Equal(t, "from_connectors", os.Getenv("TEST_LOAD_ENV_CONNECTORS"))
}

func TestLoadEnv_RunFromFlowDirectly(t *testing.T) {
	// Create: tmp/project/.env and tmp/project/flow/
	// Run from flow/ itself, expect .env at project/ to be loaded
	tmp := t.TempDir()
	projectDir := filepath.Join(tmp, "project")
	flowDir := filepath.Join(projectDir, "flow")
	require.NoError(t, os.MkdirAll(flowDir, 0o755))

	envFile := filepath.Join(projectDir, ".env")
	require.NoError(t, os.WriteFile(envFile, []byte("TEST_LOAD_ENV_FROM_FLOW=from_flow\n"), 0o600))

	t.Chdir(flowDir)

	os.Unsetenv("TEST_LOAD_ENV_FROM_FLOW")
	loadedEnv.Store(false)
	LoadEnv()

	require.Equal(t, "from_flow", os.Getenv("TEST_LOAD_ENV_FROM_FLOW"))
}
