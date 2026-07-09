package connpostgres

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

// requireUnix skips the test on platforms without the shell utilities used here.
func requireUnix(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("requires unix shell utilities")
	}
}

func TestRunPipeline_HappyPath(t *testing.T) {
	requireUnix(t)
	ctx := t.Context()

	src := exec.CommandContext(ctx, "sh", "-c", "printf 'hello world'")
	var dstOut bytes.Buffer
	dst := exec.CommandContext(ctx, "cat")
	dst.Stdout = &dstOut

	if err := runPipeline(ctx, src, dst, "src", "dst", nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := dstOut.String(); got != "hello world" {
		t.Fatalf("dst stdout = %q, want %q", got, "hello world")
	}
}

func TestRunPipeline_SrcStartFails(t *testing.T) {
	ctx := t.Context()

	src := exec.CommandContext(ctx, "/nonexistent/peerdb-test-binary")
	dst := exec.CommandContext(ctx, "cat")

	err := runPipeline(ctx, src, dst, "src", "dst", nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "start src") {
		t.Fatalf("error %q does not mention src start failure", err)
	}
	// dst should have been killed and reaped; ProcessState should be set.
	if dst.ProcessState == nil {
		t.Fatal("dst was not reaped after src start failure")
	}
}

func TestRunPipeline_DstStartFails(t *testing.T) {
	ctx := t.Context()

	src := exec.CommandContext(ctx, "echo", "hi")
	dst := exec.CommandContext(ctx, "/nonexistent/peerdb-test-binary")

	err := runPipeline(ctx, src, dst, "src", "dst", nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "start dst") {
		t.Fatalf("error %q does not mention dst start failure", err)
	}
	// src must not have been started.
	if src.ProcessState != nil {
		t.Fatal("src should not have been started when dst failed to start")
	}
}

func TestRunPipeline_SrcExitsNonZero(t *testing.T) {
	requireUnix(t)
	ctx := t.Context()

	// write some output then exit with error
	src := exec.CommandContext(ctx, "sh", "-c", "echo partial; exit 7")
	dst := exec.CommandContext(ctx, "cat")
	dst.Stdout = &bytes.Buffer{}

	err := runPipeline(ctx, src, dst, "src", "dst", nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "src failed") {
		t.Fatalf("error %q does not mention src failure", err)
	}
}

func TestRunPipeline_DstExitsNonZero(t *testing.T) {
	requireUnix(t)
	ctx := t.Context()

	src := exec.CommandContext(ctx, "true")
	// exit 3 immediately, ignoring stdin
	dst := exec.CommandContext(ctx, "sh", "-c", "exit 3")

	err := runPipeline(ctx, src, dst, "src", "dst", nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	// src succeeded so error must be from dst
	if !strings.Contains(err.Error(), "dst failed") {
		t.Fatalf("error %q does not mention dst failure", err)
	}
}

// TestRunPipeline_SrcFailsWhileDstSlow verifies the deadlock-prevention fix:
// if src exits non-zero while dst is still reading slowly, dst is killed so
// runPipeline returns promptly instead of waiting for dst to finish its work.
func TestRunPipeline_SrcFailsWhileDstSlow(t *testing.T) {
	requireUnix(t)
	ctx := t.Context()

	// src writes a small amount (fits in pipe buffer, no blocking) then exits non-zero.
	src := exec.CommandContext(ctx, "sh", "-c", "echo hi; exit 9")
	// dst is a single process (no shell-spawned children) that doesn't read stdin
	// and won't exit on its own. We expect runPipeline to kill it after src fails.
	// Note: we deliberately avoid `sh -c "sleep 30; cat"` here -- when sh forks a
	// child, that child inherits sh's stderr fd, and Go's exec.Wait blocks
	// draining stderr until the inherited fd is closed (i.e. for the full sleep).
	// psql doesn't fork children, so this matches real behavior.
	dst := exec.CommandContext(ctx, "sleep", "30")

	start := time.Now()
	done := make(chan error, 1)
	go func() { done <- runPipeline(ctx, src, dst, "src", "dst", nil) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected error from src failure")
		}
		if !strings.Contains(err.Error(), "src failed") {
			t.Fatalf("expected src failure, got %v", err)
		}
		if elapsed := time.Since(start); elapsed > 5*time.Second {
			t.Fatalf("runPipeline took %v -- dst was not killed promptly after src failure", elapsed)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("runPipeline hung -- dst was not killed after src failure")
	}
}

// TestRunPipeline_DstExitsWhileSrcWriting verifies the inverse: if dst exits
// early while src is producing lots of data, src is killed so it doesn't hang
// forever blocked on a write to a closed pipe (would normally get SIGPIPE,
// but we explicitly kill to be safe / to surface the failure quickly).
func TestRunPipeline_DstExitsWhileSrcWriting(t *testing.T) {
	requireUnix(t)
	ctx := t.Context()

	// src tries to stream a lot of data
	src := exec.CommandContext(ctx, "sh", "-c", "yes peerdb | head -c 10000000")
	// dst exits immediately without reading
	dst := exec.CommandContext(ctx, "sh", "-c", "exit 2")

	start := time.Now()
	done := make(chan error, 1)
	go func() { done <- runPipeline(ctx, src, dst, "src", "dst", nil) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected error from dst failure")
		}
		// We prefer dst's error since src's failure is just a downstream symptom.
		if !strings.Contains(err.Error(), "dst failed") {
			t.Fatalf("expected dst failure, got %v", err)
		}
		if elapsed := time.Since(start); elapsed > 5*time.Second {
			t.Fatalf("runPipeline took %v -- src was not killed promptly after dst exit", elapsed)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("runPipeline hung -- src was not killed after dst exited")
	}
}

// TestRunPipeline_LargeStream verifies that streaming more than the kernel
// pipe buffer (typically 64KB on Linux) works without deadlock.
func TestRunPipeline_LargeStream(t *testing.T) {
	requireUnix(t)
	ctx := t.Context()

	const size = 2 * 1024 * 1024 // 2 MiB
	// #nosec G204 -- test-only, constant arguments
	src := exec.CommandContext(ctx, "sh", "-c", "yes a | head -c 2097152")
	var out bytes.Buffer
	dst := exec.CommandContext(ctx, "cat")
	dst.Stdout = &out

	if err := runPipeline(ctx, src, dst, "src", "dst", nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Len() != size {
		t.Fatalf("dst received %d bytes, want %d", out.Len(), size)
	}
}

func TestRunPipeline_ContextCancel(t *testing.T) {
	requireUnix(t)
	ctx, cancel := context.WithCancel(t.Context())

	// Use exec'd binaries directly (not `sh -c "..."`). When sh is run with
	// a single argument, many shells fork a child for the command rather than
	// exec-replacing themselves. That child inherits sh's stderr fd, and Go's
	// exec.Wait blocks draining stderr until every fd holder closes it -- so
	// CommandContext killing sh isn't enough; the child keeps stderr open and
	// Wait hangs. Using a single-process command avoids the inheritance.
	src := exec.CommandContext(ctx, "sleep", "30")
	dst := exec.CommandContext(ctx, "cat")
	dst.Stdout = &bytes.Buffer{}

	done := make(chan error, 1)
	go func() { done <- runPipeline(ctx, src, dst, "src", "dst", nil) }()

	// give them a moment to start
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected error after context cancel")
		}
		// CommandContext kills the process; just ensure we got back.
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) && !strings.Contains(err.Error(), "killed") &&
			!strings.Contains(err.Error(), "signal") {
			// any non-nil error is acceptable here; we're mostly checking we don't hang
			t.Logf("got error after cancel: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("runPipeline did not return after context cancel")
	}
}

// TestRunPipeline_FilterStripsLines verifies the filter goroutine drops
// matching lines and forwards the rest. Covers SET transaction_timeout (PG17+)
// and \restrict / \unrestrict psql meta-commands (pg_dump 17.6+).
func TestRunPipeline_FilterStripsLines(t *testing.T) {
	requireUnix(t)
	ctx := t.Context()

	input := "SELECT 1;\n" +
		"SET transaction_timeout = 0;\n" +
		"\\restrict abc123\n" +
		"CREATE TABLE t(id int);\n" +
		"\\unrestrict abc123\n" +
		"SELECT 2;\n"
	src := exec.CommandContext(ctx, "printf", "%s", input)
	var out bytes.Buffer
	dst := exec.CommandContext(ctx, "cat")
	dst.Stdout = &out

	if err := runPipeline(ctx, src, dst, "src", "dst", filterIncompatibleLines); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := out.String()
	want := "SELECT 1;\nCREATE TABLE t(id int);\nSELECT 2;\n"
	if got != want {
		t.Fatalf("filtered output = %q, want %q", got, want)
	}
}

func strPtr(s string) *string { return &s }
func boolPtr(b bool) *bool    { return &b }

func TestBuildPgDumpArgs(t *testing.T) {
	tests := []struct {
		name     string
		config   *protos.PostgresConfig
		wantArgs []string
	}{
		{
			name: "basic config",
			config: &protos.PostgresConfig{
				Host:     "src-host",
				Port:     5432,
				Database: "mydb",
				User:     "admin",
			},
			wantArgs: []string{
				"--schema-only", "--no-owner", "--no-privileges",
				"-h", "src-host", "-p", "5432", "-d", "mydb", "-U", "admin",
			},
		},
		{
			name: "default port when zero",
			config: &protos.PostgresConfig{
				Host:     "localhost",
				Port:     0,
				Database: "test",
			},
			wantArgs: []string{
				"--schema-only", "--no-owner", "--no-privileges",
				"-h", "localhost", "-p", "5432", "-d", "test",
			},
		},
		{
			name: "custom port no user",
			config: &protos.PostgresConfig{
				Host:     "remote",
				Port:     6543,
				Database: "prod",
			},
			wantArgs: []string{
				"--schema-only", "--no-owner", "--no-privileges",
				"-h", "remote", "-p", "6543", "-d", "prod",
			},
		},
		{
			name: "TlsHost overrides -h",
			config: &protos.PostgresConfig{
				Host:     "10.0.0.1",
				Port:     5432,
				Database: "mydb",
				User:     "admin",
				TlsHost:  "db.example.com",
			},
			wantArgs: []string{
				"--schema-only", "--no-owner", "--no-privileges",
				"-h", "db.example.com", "-p", "5432", "-d", "mydb", "-U", "admin",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildPgDumpArgs(tt.config)
			if !slices.Equal(got, tt.wantArgs) {
				t.Fatalf("buildPgDumpArgs = %v, want %v", got, tt.wantArgs)
			}
		})
	}
}

func TestBuildPsqlArgs(t *testing.T) {
	tests := []struct {
		name     string
		config   *protos.PostgresConfig
		wantArgs []string
	}{
		{
			name: "with user",
			config: &protos.PostgresConfig{
				Host:     "dst-host",
				Port:     5433,
				Database: "target",
				User:     "writer",
			},
			wantArgs: []string{
				"-h", "dst-host", "-p", "5433", "-d", "target",
				"--single-transaction",
				"-v", "ON_ERROR_STOP=1",
				"--quiet",
				"-U", "writer",
			},
		},
		{
			name: "default port no user",
			config: &protos.PostgresConfig{
				Host:     "localhost",
				Port:     0,
				Database: "db",
			},
			wantArgs: []string{
				"-h", "localhost", "-p", "5432", "-d", "db",
				"--single-transaction",
				"-v", "ON_ERROR_STOP=1",
				"--quiet",
			},
		},
		{
			name: "TlsHost overrides -h",
			config: &protos.PostgresConfig{
				Host:     "10.0.0.1",
				Port:     5432,
				Database: "db",
				User:     "u",
				TlsHost:  "db.example.com",
			},
			wantArgs: []string{
				"-h", "db.example.com", "-p", "5432", "-d", "db",
				"--single-transaction",
				"-v", "ON_ERROR_STOP=1",
				"--quiet",
				"-U", "u",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildPsqlArgs(tt.config)
			if !slices.Equal(got, tt.wantArgs) {
				t.Fatalf("buildPsqlArgs = %v, want %v", got, tt.wantArgs)
			}
		})
	}
}

// envValue returns the last value for key in cmd.Env, or "" if absent.
func envValue(cmd *exec.Cmd, key string) string {
	prefix := key + "="
	for i := len(cmd.Env) - 1; i >= 0; i-- {
		if strings.HasPrefix(cmd.Env[i], prefix) {
			return strings.TrimPrefix(cmd.Env[i], prefix)
		}
	}
	return ""
}

func TestAppendTLSEnv(t *testing.T) {
	ctx := t.Context()

	tests := []struct {
		name            string
		config          *protos.PostgresConfig
		wantSSLMode     string
		wantRootCertSet bool
	}{
		{
			name: "no TLS when not required and no root CA",
			config: &protos.PostgresConfig{
				Host: "h", Port: 5432, Database: "d",
			},
			wantSSLMode: "",
		},
		{
			name: "require TLS via RequireTls flag",
			config: &protos.PostgresConfig{
				Host: "h", Port: 5432, Database: "d",
				RequireTls: true,
			},
			wantSSLMode: "require",
		},
		{
			name: "require TLS via DisableTls=false",
			config: &protos.PostgresConfig{
				Host: "h", Port: 5432, Database: "d",
				DisableTls: boolPtr(false),
			},
			wantSSLMode: "require",
		},
		{
			name: "no TLS when DisableTls=true",
			config: &protos.PostgresConfig{
				Host: "h", Port: 5432, Database: "d",
				DisableTls: boolPtr(true),
			},
			wantSSLMode: "",
		},
		{
			name: "require with skip cert verification",
			config: &protos.PostgresConfig{
				Host: "h", Port: 5432, Database: "d",
				RequireTls:           true,
				SkipCertVerification: true,
			},
			wantSSLMode: "require",
		},
		{
			name: "verify-ca when root CA provided with RequireTls",
			config: &protos.PostgresConfig{
				Host: "h", Port: 5432, Database: "d",
				RequireTls: true,
				RootCa:     strPtr("-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----"),
			},
			wantSSLMode:     "verify-ca",
			wantRootCertSet: true,
		},
		{
			name: "root CA alone triggers TLS with verify-ca",
			config: &protos.PostgresConfig{
				Host: "h", Port: 5432, Database: "d",
				RootCa: strPtr("-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----"),
			},
			wantSSLMode:     "verify-ca",
			wantRootCertSet: true,
		},
		{
			name: "skip cert verification with root CA uses require",
			config: &protos.PostgresConfig{
				Host: "h", Port: 5432, Database: "d",
				RequireTls:           true,
				SkipCertVerification: true,
				RootCa:               strPtr("-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----"),
			},
			wantSSLMode:     "require",
			wantRootCertSet: false, // skip_cert_verification takes precedence, no sslrootcert needed
		},
		{
			name: "empty root CA string is treated as absent",
			config: &protos.PostgresConfig{
				Host: "h", Port: 5432, Database: "d",
				RequireTls: true,
				RootCa:     strPtr(""),
			},
			wantSSLMode:     "require",
			wantRootCertSet: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := exec.CommandContext(ctx, "echo")
			cmd.Env = os.Environ()

			appendTLSEnv(ctx, cmd, tt.config)

			gotSSLMode := envValue(cmd, "PGSSLMODE")
			if gotSSLMode != tt.wantSSLMode {
				t.Errorf("PGSSLMODE = %q, want %q", gotSSLMode, tt.wantSSLMode)
			}

			gotRootCert := envValue(cmd, "PGSSLROOTCERT")
			if tt.wantRootCertSet {
				if gotRootCert == "" {
					t.Error("expected PGSSLROOTCERT to be set, but it was empty")
				} else {
					// Verify the temp file was actually written with the CA content.
					content, err := os.ReadFile(gotRootCert)
					if err != nil {
						t.Fatalf("failed to read PGSSLROOTCERT file %q: %v", gotRootCert, err)
					}
					if !strings.Contains(string(content), "BEGIN CERTIFICATE") {
						t.Errorf("PGSSLROOTCERT file content = %q, expected PEM data", string(content))
					}
					os.Remove(gotRootCert)
				}
			} else if gotRootCert != "" {
				t.Errorf("expected PGSSLROOTCERT to be empty, got %q", gotRootCert)
			}
		})
	}
}

func TestAppendTLSEnv_TlsHost(t *testing.T) {
	ctx := t.Context()

	t.Run("PGHOSTADDR set when TlsHost configured", func(t *testing.T) {
		cmd := exec.CommandContext(ctx, "echo")
		cmd.Env = os.Environ()

		config := &protos.PostgresConfig{
			Host: "10.0.0.1", Port: 5432, Database: "d",
			RequireTls: true,
			TlsHost:    "db.example.com",
		}
		appendTLSEnv(ctx, cmd, config)

		if got := envValue(cmd, "PGHOSTADDR"); got != "10.0.0.1" {
			t.Errorf("PGHOSTADDR = %q, want %q", got, "10.0.0.1")
		}
		if got := envValue(cmd, "PGSSLMODE"); got != "require" {
			t.Errorf("PGSSLMODE = %q, want %q", got, "require")
		}
	})

	t.Run("PGHOSTADDR not set when TlsHost empty", func(t *testing.T) {
		cmd := exec.CommandContext(ctx, "echo")
		cmd.Env = os.Environ()

		config := &protos.PostgresConfig{
			Host: "10.0.0.1", Port: 5432, Database: "d",
			RequireTls: true,
		}
		appendTLSEnv(ctx, cmd, config)

		if got := envValue(cmd, "PGHOSTADDR"); got != "" {
			t.Errorf("PGHOSTADDR should be empty, got %q", got)
		}
	})

	t.Run("PGHOSTADDR with root CA and TlsHost", func(t *testing.T) {
		cmd := exec.CommandContext(ctx, "echo") //nolint:gosec
		cmd.Env = os.Environ()

		config := &protos.PostgresConfig{
			Host: "10.0.0.1", Port: 5432, Database: "d",
			RequireTls: true,
			TlsHost:    "db.example.com",
			RootCa:     strPtr("-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----"),
		}
		appendTLSEnv(ctx, cmd, config)

		if got := envValue(cmd, "PGHOSTADDR"); got != "10.0.0.1" {
			t.Errorf("PGHOSTADDR = %q, want %q", got, "10.0.0.1")
		}
		if got := envValue(cmd, "PGSSLMODE"); got != "verify-ca" {
			t.Errorf("PGSSLMODE = %q, want %q", got, "verify-ca")
		}
		// clean up temp file
		if f := envValue(cmd, "PGSSLROOTCERT"); f != "" {
			os.Remove(f)
		}
	})
}

func TestIncompatibleLineRegex(t *testing.T) {
	tests := []struct {
		line  string
		match bool
	}{
		{"SET transaction_timeout = 0;\n", true},
		{"SET  transaction_timeout=0;\n", true},
		{"SET statement_timeout = 0;\n", false},
		{"\\restrict abc123\n", true},
		{"\\unrestrict abc123\n", true},
		{"\\restrict\n", true},
		{"CREATE TABLE t(id int);\n", false},
		{"SELECT 1;\n", false},
	}
	for _, tt := range tests {
		got := incompatibleLineRE.MatchString(tt.line)
		if got != tt.match {
			t.Errorf("incompatibleLineRE.Match(%q) = %v, want %v", tt.line, got, tt.match)
		}
	}
}
