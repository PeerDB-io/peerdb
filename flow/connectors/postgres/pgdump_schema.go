package connpostgres

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/netip"
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
	srcAddr, err := resolvePgAddr(ctx, srcConfig)
	if err != nil {
		return fmt.Errorf("source: %w", err)
	}
	dstAddr, err := resolvePgAddr(ctx, dstConfig)
	if err != nil {
		return fmt.Errorf("destination: %w", err)
	}

	if err := pipeCommand(ctx, srcConfig, dstConfig, srcAddr, dstAddr,
		"pg_dump", buildPgDumpArgs(srcConfig, srcAddr.host)); err != nil {
		return fmt.Errorf("pg_dump schema migration failed: %w", err)
	}

	return nil
}

// pipeCommand runs srcBinary with the given args, piping its stdout into psql on the destination.
func pipeCommand(
	ctx context.Context,
	srcConfig *protos.PostgresConfig,
	dstConfig *protos.PostgresConfig,
	srcAddr pgAddr,
	dstAddr pgAddr,
	srcBinary string,
	srcArgs []string,
) error {
	psqlArgs := buildPsqlArgs(dstConfig, dstAddr.host)

	srcCmd := exec.CommandContext(ctx, srcBinary, srcArgs...)
	psqlCmd := exec.CommandContext(ctx, "psql", psqlArgs...)

	// set PGPASSWORD for each command via separate env slices
	srcCmd.Env = append(os.Environ(), "PGPASSWORD="+srcConfig.Password)
	psqlCmd.Env = append(os.Environ(), "PGPASSWORD="+dstConfig.Password)

	if srcAddr.hostaddr != "" {
		srcCmd.Env = append(srcCmd.Env, "PGHOSTADDR="+srcAddr.hostaddr)
	}
	if dstAddr.hostaddr != "" {
		psqlCmd.Env = append(psqlCmd.Env, "PGHOSTADDR="+dstAddr.hostaddr)
	}

	// handle TLS env vars
	appendTLSEnv(ctx, srcCmd, srcConfig, srcAddr)
	appendTLSEnv(ctx, psqlCmd, dstConfig, dstAddr)

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

// pgAddr is how a raw libpq invocation (pg_dump/psql) should address one peer.
type pgAddr struct {
	// host is the -h value. When hostaddr is empty, it is simply what libpq
	// resolves and dials. When hostaddr is set, libpq dials hostaddr instead
	// and host is only the SNI name sent and the name the server certificate
	// is checked against.
	host string
	// hostaddr is the numeric IP libpq dials (PGHOSTADDR), bypassing DNS.
	// Set only when the dial target and the certificate name differ.
	hostaddr string
}

// lookupHost is swappable for tests.
var lookupHost = func(ctx context.Context, host string) ([]string, error) {
	return net.DefaultResolver.LookupHost(ctx, host)
}

// resolvePgAddr mirrors the pgx path's semantics (ParseConfig/CreateTlsConfig):
// always dial Host; TlsHost, when TLS is in play, is only the name the server
// certificate is checked against.
func resolvePgAddr(ctx context.Context, config *protos.PostgresConfig) (pgAddr, error) {
	host := internal.SanitizePGHost(config.Host)
	tlsHost := internal.SanitizePGHost(config.TlsHost)

	hasRootCA := config.RootCa != nil && *config.RootCa != ""
	shouldUseTls := internal.PGMustUseTlsConnection(config) || hasRootCA

	if !shouldUseTls || tlsHost == "" || tlsHost == host {
		return pgAddr{host: host}, nil
	}

	// Connect to Host, verify as TlsHost: libpq spells it host=TlsHost
	// hostaddr=Host, but hostaddr only accepts numeric IPs, so resolve
	// hostnames ourselves.
	hostaddr := host
	if _, err := netip.ParseAddr(hostaddr); err != nil {
		addrs, err := lookupHost(ctx, host)
		if err != nil {
			return pgAddr{}, fmt.Errorf("failed to resolve host %s: %w", host, err)
		}
		if len(addrs) == 0 {
			return pgAddr{}, fmt.Errorf("host %s resolved to no addresses", host)
		}
		hostaddr = addrs[0]
	}
	return pgAddr{host: tlsHost, hostaddr: hostaddr}, nil
}

func buildPgDumpArgs(config *protos.PostgresConfig, host string) []string {
	port := config.Port
	if port == 0 {
		port = 5432
	}

	args := []string{
		"--schema-only",
		"--no-owner",
		"--no-privileges",
		"-h", host,
		"-p", strconv.FormatUint(uint64(port), 10),
		"-d", config.Database,
	}
	if config.User != "" {
		args = append(args, "-U", config.User)
	}
	return args
}

func buildPsqlArgs(config *protos.PostgresConfig, host string) []string {
	port := config.Port
	if port == 0 {
		port = 5432
	}

	args := []string{
		"-h", host,
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

func appendTLSEnv(ctx context.Context, cmd *exec.Cmd, config *protos.PostgresConfig, addr pgAddr) {
	hasRootCA := config.RootCa != nil && *config.RootCa != ""
	if !internal.PGMustUseTlsConnection(config) && !hasRootCA {
		return
	}

	// libpq checks the certificate name against the -h value (addr.host).
	// Mirror CreateTlsConfig: a cert presented on an IP connection may not
	// carry a matching IP SAN, so we only do name verification for hostnames.
	_, ipErr := netip.ParseAddr(addr.host)
	hostIsIP := ipErr == nil

	switch {
	case config.SkipCertVerification:
		cmd.Env = append(cmd.Env, "PGSSLMODE=require")
	case hasRootCA && hostIsIP:
		cmd.Env = append(cmd.Env, "PGSSLMODE=verify-ca")
	case hasRootCA:
		cmd.Env = append(cmd.Env, "PGSSLMODE=verify-full")
	case hostIsIP:
		// No CA and an IP host: libpq can't verify the chain without also
		// checking the name (sslrootcert=system forces verify-full), so
		// encryption only.
		cmd.Env = append(cmd.Env, "PGSSLMODE=require")
	default:
		// No CA and a hostname: verify against the system trust store.
		// sslrootcert=system needs libpq 16+.
		cmd.Env = append(cmd.Env, "PGSSLMODE=verify-full", "PGSSLROOTCERT=system")
	}

	if hasRootCA && !config.SkipCertVerification {
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
	}
}
