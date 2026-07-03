package oracle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	osexec "os/exec"
	"path/filepath"
	"time"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/digest"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/run"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/wire"
)

type Client struct {
	Engine  string
	BinPath string
	Timeout time.Duration
	cmd     *osexec.Cmd
	stdin   io.WriteCloser
	stdout  io.ReadCloser
	conn    *wire.Conn
	Hello   wire.HelloInfo
}

func NewClient(engine, binPath string, timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &Client{Engine: engine, BinPath: binPath, Timeout: timeout}
}

func (c *Client) Start(ctx context.Context, logPath string) error {
	if c.BinPath == "" {
		return errors.New("oracle binary path is empty")
	}
	if _, err := os.Stat(c.BinPath); err != nil {
		return err
	}
	cmd := osexec.CommandContext(ctx, c.BinPath)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if logPath != "" {
		if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
			return err
		}
		f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return err
		}
		cmd.Stderr = f
	} else {
		cmd.Stderr = os.Stderr
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	c.cmd, c.stdin, c.stdout = cmd, stdin, stdout
	c.conn = wire.New(stdout, stdin)
	hi, err := callWithTimeout(ctx, c.Timeout, c.conn.Hello)
	if err != nil {
		_ = c.Close()
		return err
	}
	if hi.Engine != "" && hi.Engine != c.Engine {
		_ = c.Close()
		return fmt.Errorf("oracle hello engine %q != expected %q", hi.Engine, c.Engine)
	}
	c.Hello = hi
	return nil
}

func (c *Client) ParseBatch(ctx context.Context, cases []run.Case) ([]*digest.Digest, [][]byte, error) {
	if c.conn == nil {
		return nil, nil, errors.New("oracle client not started")
	}
	raw, err := callWithTimeout(ctx, c.Timeout, func() ([][]byte, error) {
		return c.conn.ParseBatch(cases)
	})
	if err != nil {
		return nil, nil, err
	}
	out := make([]*digest.Digest, 0, len(raw))
	copies := make([][]byte, 0, len(raw))
	for _, b := range raw {
		cp := append([]byte(nil), b...)
		d, err := digest.Decode(cp)
		if err != nil {
			return nil, nil, fmt.Errorf("bad digest %q: %w", string(cp), err)
		}
		out = append(out, d)
		copies = append(copies, cp)
	}
	return out, copies, nil
}

func (c *Client) Coverage(ctx context.Context) ([]byte, error) {
	if c.conn == nil {
		return nil, errors.New("oracle client not started")
	}
	return callWithTimeout(ctx, c.Timeout, c.conn.GetCoverage)
}

func (c *Client) Close() error {
	if c.stdin != nil {
		_ = c.stdin.Close()
	}
	if c.stdout != nil {
		_ = c.stdout.Close()
	}
	if c.cmd != nil && c.cmd.Process != nil {
		_ = c.cmd.Process.Kill()
		_, _ = c.cmd.Process.Wait()
	}
	return nil
}

func SingleDigest(ctx context.Context, engine, binPath string, timeout time.Duration, c run.Case, stateDir string) (*digest.Digest, []byte, error) {
	client := NewClient(engine, binPath, timeout)
	if err := client.Start(ctx, filepath.Join(stateDir, "log", "oracle-"+engine+"-single.log")); err != nil {
		return nil, nil, err
	}
	defer client.Close()
	ds, raw, err := client.ParseBatch(ctx, []run.Case{c})
	if err != nil {
		return nil, nil, err
	}
	if len(ds) != 1 {
		return nil, nil, fmt.Errorf("oracle returned %d digests", len(ds))
	}
	return ds[0], raw[0], nil
}

func callWithTimeout[T any](ctx context.Context, timeout time.Duration, fn func() (T, error)) (T, error) {
	var zero T
	type result struct {
		v   T
		err error
	}
	ch := make(chan result, 1)
	go func() {
		v, err := fn()
		ch <- result{v: v, err: err}
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case <-timer.C:
		return zero, context.DeadlineExceeded
	case r := <-ch:
		return r.v, r.err
	}
}

func RawDigestJSON(d *digest.Digest) json.RawMessage {
	if d == nil {
		return nil
	}
	b, _ := json.Marshal(d)
	return b
}
