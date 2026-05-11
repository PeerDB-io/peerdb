package connpostgres

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strconv"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

// RunPgDumpSchema streams a schema-only pg_dump from source directly into psql
// on the destination, piping stdout into stdin without intermediate files.
func RunPgDumpSchema(ctx context.Context, srcConfig *protos.PostgresConfig, dstConfig *protos.PostgresConfig) error {
	if err := pipeCommand(ctx, srcConfig, dstConfig, "pg_dump", buildPgDumpArgs(srcConfig)); err != nil {
		return fmt.Errorf("pg_dump schema migration failed: %w", err)
	}

	return nil
}

// pipeCommand runs srcBinary with the given args, piping its stdout into psql on the destination.
func pipeCommand(
	ctx context.Context,
	srcConfig *protos.PostgresConfig,
	dstConfig *protos.PostgresConfig,
	srcBinary string,
	srcArgs []string,
) error {
	psqlArgs := buildPsqlArgs(dstConfig)

	srcCmd := exec.CommandContext(ctx, srcBinary, srcArgs...)
	psqlCmd := exec.CommandContext(ctx, "psql", psqlArgs...)

	// set PGPASSWORD for each command via separate env slices
	srcCmd.Env = append(os.Environ(), "PGPASSWORD="+srcConfig.Password)
	psqlCmd.Env = append(os.Environ(), "PGPASSWORD="+dstConfig.Password)

	// handle TLS env vars
	appendTLSEnv(ctx, srcCmd, srcConfig)
	appendTLSEnv(ctx, psqlCmd, dstConfig)

	return runPipeline(srcCmd, psqlCmd, srcBinary, "psql")
}

// runPipeline wires srcCmd's stdout into dstCmd's stdin and waits for both.
func runPipeline(srcCmd, dstCmd *exec.Cmd, srcName, dstName string) error {
	pr, pw, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("create pipe: %w", err)
	}
	srcCmd.Stdout = pw
	dstCmd.Stdin = pr

	var srcStderr, dstStderr bytes.Buffer
	srcCmd.Stderr = &srcStderr
	dstCmd.Stderr = &dstStderr

	// Start dst first so it's ready to read.
	if err := dstCmd.Start(); err != nil {
		pr.Close()
		pw.Close()
		return fmt.Errorf("start %s: %w", dstName, err)
	}
	// dst now owns the read end in its child process.
	pr.Close()

	if err := srcCmd.Start(); err != nil {
		pw.Close()
		_ = dstCmd.Process.Kill()
		_ = dstCmd.Wait()
		return fmt.Errorf("start %s: %w", srcName, err)
	}
	// src now owns the write end in its child process.
	pw.Close()

	srcDone := make(chan error, 1)
	dstDone := make(chan error, 1)
	go func() { srcDone <- srcCmd.Wait() }()
	go func() { dstDone <- dstCmd.Wait() }()

	var (
		srcErr, dstErr       error
		srcKilled, dstKilled bool
	)
	for range 2 {
		select {
		case err := <-srcDone:
			srcErr = err
			if err != nil && dstCmd.ProcessState == nil {
				_ = dstCmd.Process.Kill()
				dstKilled = true
			}
		case err := <-dstDone:
			dstErr = err
			if srcCmd.ProcessState == nil {
				// dst exited (success or failure) while src is still running;
				// kill src so it doesn't block on a pipe with no reader.
				_ = srcCmd.Process.Kill()
				srcKilled = true
			}
		}
	}

	// Report the original cause, not the side we killed in response.
	if dstErr != nil && !dstKilled {
		return fmt.Errorf("%s failed: %w\nstderr:\n%s", dstName, dstErr, dstStderr.String())
	}
	if srcErr != nil && !srcKilled {
		return fmt.Errorf("%s failed: %w\nstderr:\n%s", srcName, srcErr, srcStderr.String())
	}
	// Fallback: both sides killed (e.g. ctx cancel) — surface whichever error we have.
	if srcErr != nil {
		return fmt.Errorf("%s failed: %w\nstderr:\n%s", srcName, srcErr, srcStderr.String())
	}
	if dstErr != nil {
		return fmt.Errorf("%s failed: %w\nstderr:\n%s", dstName, dstErr, dstStderr.String())
	}
	return nil
}

func buildPgDumpArgs(config *protos.PostgresConfig) []string {
	port := config.Port
	if port == 0 {
		port = 5432
	}

	args := []string{
		"--schema-only",
		"--no-owner",
		"--no-privileges",
		"-h", config.Host,
		"-p", strconv.FormatUint(uint64(port), 10),
		"-d", config.Database,
	}
	if config.User != "" {
		args = append(args, "-U", config.User)
	}
	return args
}

func buildPsqlArgs(config *protos.PostgresConfig) []string {
	port := config.Port
	if port == 0 {
		port = 5432
	}

	args := []string{
		"-h", config.Host,
		"-p", strconv.FormatUint(uint64(port), 10),
		"-d", config.Database,
	}
	if config.User != "" {
		args = append(args, "-U", config.User)
	}
	return args
}

func appendTLSEnv(ctx context.Context, cmd *exec.Cmd, config *protos.PostgresConfig) {
	if config.RequireTls {
		cmd.Env = append(cmd.Env, "PGSSLMODE=require")

		if config.RootCa != nil && *config.RootCa != "" {
			// write root CA to a temp file
			tmpFile, err := os.CreateTemp("", "peerdb-root-ca-*.pem")
			if err != nil {
				slog.WarnContext(ctx, "failed to create temp file for root CA, skipping sslrootcert", slog.Any("error", err))
				return
			}
			if _, err := tmpFile.WriteString(*config.RootCa); err != nil {
				slog.WarnContext(ctx, "failed to write root CA to temp file", slog.Any("error", err))
				tmpFile.Close()
				os.Remove(tmpFile.Name())
				return
			}
			tmpFile.Close()
			cmd.Env = append(cmd.Env, "PGSSLROOTCERT="+tmpFile.Name())
			// note: temp file is cleaned up when the process exits
		}
	}
}
