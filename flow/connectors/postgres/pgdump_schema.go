package connpostgres

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"regexp"
	"strconv"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

// pg_dump from newer Postgres versions emits statements that older
// destinations don't recognize:
//   - SET transaction_timeout = 0;        (PG17+ session GUC)
//   - \restrict / \unrestrict <token>     (pg_dump 17.6+ psql meta-commands
//     that gate replay against an unrelated psql session; older psql treats
//     them as unknown backslash commands and aborts under ON_ERROR_STOP)
//
// These are session/replay housekeeping and safe to drop on the wire so we
// keep ON_ERROR_STOP=1 for genuine DDL failures while remaining cross-version.
var incompatibleLineRE = regexp.MustCompile(`^(SET\s+transaction_timeout\s*=|\\(?:un)?restrict(\s|$))`)

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

	return runPipeline(ctx, srcCmd, psqlCmd, srcBinary, "psql", filterIncompatibleLines)
}

// filterIncompatibleLines copies r->w line by line, dropping statements that
// are valid in newer pg_dump output but rejected by older psql/destinations.
func filterIncompatibleLines(ctx context.Context, r io.Reader, w io.Writer) error {
	br := bufio.NewReaderSize(r, 64*1024)
	for {
		line, err := br.ReadBytes('\n')
		if len(line) > 0 {
			if !incompatibleLineRE.Match(line) {
				if _, werr := w.Write(line); werr != nil {
					return werr
				}
			} else {
				slog.DebugContext(ctx, "dropping incompatible line from pg_dump stream",
					slog.String("line", string(bytes.TrimRight(line, "\n"))))
			}
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

// runPipeline wires srcCmd's stdout into dstCmd's stdin (optionally through a
// filter goroutine) and waits for both processes.
//
// Pipe topology:
//
//	without filter:  src.stdout -> srcW |--pipe--| srcR -> dst.stdin
//	with filter:     src.stdout -> srcW |--pipe--| srcR -> filter -> dstW |--pipe--| dstR -> dst.stdin
//
// File descriptor ownership matters here -- if the parent keeps a write end
// open after the child consumer dies, the producer can hang forever on a
// blocked write. We close each fd as soon as the child or filter goroutine
// owns it.
func runPipeline(
	ctx context.Context,
	srcCmd, dstCmd *exec.Cmd,
	srcName, dstName string,
	filter func(context.Context, io.Reader, io.Writer) error,
) error {
	srcR, srcW, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("create src pipe: %w", err)
	}
	srcCmd.Stdout = srcW

	var (
		dstR, dstW *os.File
		filterDone chan error
	)
	if filter == nil {
		dstCmd.Stdin = srcR
	} else {
		dstR, dstW, err = os.Pipe()
		if err != nil {
			srcR.Close()
			srcW.Close()
			return fmt.Errorf("create dst pipe: %w", err)
		}
		dstCmd.Stdin = dstR
		filterDone = make(chan error, 1)
	}

	var srcStderr, dstStderr bytes.Buffer
	srcCmd.Stderr = &srcStderr
	dstCmd.Stderr = &dstStderr

	// Start dst first so it's ready to read.
	if err := dstCmd.Start(); err != nil {
		srcR.Close()
		srcW.Close()
		if dstW != nil {
			dstR.Close()
			dstW.Close()
		}
		return fmt.Errorf("start %s: %w", dstName, err)
	}
	// dst owns its stdin fd in its child; close our copy.
	if filter == nil {
		srcR.Close()
	} else {
		dstR.Close()
	}

	if err := srcCmd.Start(); err != nil {
		srcW.Close()
		if dstW != nil {
			// filter never started; close its writer so dst sees EOF.
			dstW.Close()
			// and the read side we still hold if filter==nil path wasn't taken.
			if filter != nil {
				srcR.Close()
			}
		}
		_ = dstCmd.Process.Kill()
		_ = dstCmd.Wait()
		return fmt.Errorf("start %s: %w", srcName, err)
	}
	// src owns its stdout fd in its child; close our copy.
	srcW.Close()

	// Run the filter goroutine if configured. It bridges srcR -> dstW.
	if filter != nil {
		go func() {
			err := filter(ctx, srcR, dstW)
			// Always close both ends so the producer/consumer unblock.
			srcR.Close()
			dstW.Close()
			filterDone <- err
		}()
	}

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
				if killErr := dstCmd.Process.Kill(); killErr == nil {
					dstKilled = true
				}
			}
		case err := <-dstDone:
			dstErr = err
			if srcCmd.ProcessState == nil {
				// dst exited (success or failure) while src is still running;
				// kill src so it doesn't block on a pipe with no reader.
				if killErr := srcCmd.Process.Kill(); killErr == nil {
					srcKilled = true
				}
			}
		}
	}

	// Wait for the filter to finish so we surface any I/O error and so the
	// goroutine doesn't outlive this function.
	var filterErr error
	if filterDone != nil {
		filterErr = <-filterDone
	}

	// Report the original cause, not the side we killed in response.
	if dstErr != nil && !dstKilled {
		return fmt.Errorf("%s failed: %w\nstderr:\n%s", dstName, dstErr, dstStderr.String())
	}
	if srcErr != nil && !srcKilled {
		return fmt.Errorf("%s failed: %w\nstderr:\n%s", srcName, srcErr, srcStderr.String())
	}
	if filterErr != nil {
		return fmt.Errorf("filter failed: %w", filterErr)
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
		// Wrap the entire dump in a single transaction so partial failures
		// roll back cleanly (makes the activity safely retryable) and avoid
		// per-statement autocommit overhead on high-latency links.
		"--single-transaction",
		// Without this, psql logs errors to stderr but exits 0, so a half-
		// applied schema would be reported as success. ON_ERROR_STOP=1 makes
		// psql exit non-zero on the first failed statement.
		"-v", "ON_ERROR_STOP=1",
		// Quiet informational chatter; errors still go to stderr.
		"--quiet",
	}
	if config.User != "" {
		args = append(args, "-U", config.User)
	}
	return args
}

func appendTLSEnv(ctx context.Context, cmd *exec.Cmd, config *protos.PostgresConfig) {
	if internal.PGMustUseTlsConnection(config) {
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
