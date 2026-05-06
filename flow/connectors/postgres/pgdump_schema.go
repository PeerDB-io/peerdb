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

// RunPgDumpSchema first migrates roles via pg_dumpall --roles-only, then streams
// a schema-only pg_dump from source directly into psql on the destination,
// piping stdout into stdin without intermediate files.
func RunPgDumpSchema(ctx context.Context, srcConfig *protos.PostgresConfig, dstConfig *protos.PostgresConfig) error {
	// Step 1: migrate roles from source to destination
	if err := pipeCommand(ctx, srcConfig, dstConfig, "pg_dumpall", buildPgDumpAllArgs(srcConfig)); err != nil {
		return fmt.Errorf("pg_dumpall roles migration failed: %w", err)
	}

	// Step 2: migrate schema from source to destination
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
	appendTLSEnv(srcCmd, srcConfig)
	appendTLSEnv(psqlCmd, dstConfig)

	// pipe source command stdout -> psql stdin
	pipe, err := srcCmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create %s stdout pipe: %w", srcBinary, err)
	}
	psqlCmd.Stdin = pipe

	var srcStderr, psqlStderr bytes.Buffer
	srcCmd.Stderr = &srcStderr
	psqlCmd.Stderr = &psqlStderr

	// start psql first so it's ready to read
	if err := psqlCmd.Start(); err != nil {
		return fmt.Errorf("failed to start psql: %w", err)
	}

	// then start source command which writes to the pipe
	if err := srcCmd.Start(); err != nil {
		// kill psql since source command failed to start
		_ = psqlCmd.Process.Kill()
		_ = psqlCmd.Wait()
		return fmt.Errorf("failed to start %s: %w", srcBinary, err)
	}

	// wait for source command to finish (closes the pipe, signaling EOF to psql)
	srcErr := srcCmd.Wait()
	psqlErr := psqlCmd.Wait()

	if srcErr != nil {
		return fmt.Errorf("%s failed: %w\nstderr: %s", srcBinary, srcErr, srcStderr.String())
	}
	if psqlErr != nil {
		return fmt.Errorf("psql failed: %w\nstderr: %s", psqlErr, psqlStderr.String())
	}

	return nil
}

func buildPgDumpAllArgs(config *protos.PostgresConfig) []string {
	port := config.Port
	if port == 0 {
		port = 5432
	}

	args := []string{
		"--roles-only",
		"-h", config.Host,
		"-p", strconv.FormatUint(uint64(port), 10),
	}
	if config.User != "" {
		args = append(args, "-U", config.User)
	}
	return args
}

func buildPgDumpArgs(config *protos.PostgresConfig) []string {
	port := config.Port
	if port == 0 {
		port = 5432
	}

	args := []string{
		"--schema-only",
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

func appendTLSEnv(cmd *exec.Cmd, config *protos.PostgresConfig) {
	if config.RequireTls {
		cmd.Env = append(cmd.Env, "PGSSLMODE=require")

		if config.RootCa != nil && *config.RootCa != "" {
			// write root CA to a temp file
			tmpFile, err := os.CreateTemp("", "peerdb-root-ca-*.pem")
			if err != nil {
				slog.Warn("failed to create temp file for root CA, skipping sslrootcert", slog.Any("error", err))
				return
			}
			if _, err := tmpFile.WriteString(*config.RootCa); err != nil {
				slog.Warn("failed to write root CA to temp file", slog.Any("error", err))
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
