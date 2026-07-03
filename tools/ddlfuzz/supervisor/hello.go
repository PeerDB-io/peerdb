package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"syscall"
	"time"
)

type helloResponse struct {
	Engine        string `json:"engine"`
	ServerVersion string `json:"server_version"`
	Protocol      int    `json:"protocol"`
}

func HelloSmoke(ctx context.Context, oraclePath, engine string) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, oraclePath)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	pgid, _ := syscall.Getpgid(cmd.Process.Pid)
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()
	defer func() {
		if pgid > 0 {
			_ = syscall.Kill(-pgid, syscall.SIGTERM)
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				_ = syscall.Kill(-pgid, syscall.SIGKILL)
				<-done
			}
		}
	}()

	errc := make(chan error, 1)
	go func() {
		var req [5]byte
		binary.LittleEndian.PutUint32(req[:4], 1)
		req[4] = 3
		if _, err := stdin.Write(req[:]); err != nil {
			errc <- err
			return
		}
		_ = stdin.Close()

		var hdr [4]byte
		if _, err := io.ReadFull(stdout, hdr[:]); err != nil {
			errc <- fmt.Errorf("read hello frame header: %w; stderr=%s", err, tailString(stderr.String(), 2000))
			return
		}
		bodyLen := binary.LittleEndian.Uint32(hdr[:])
		if bodyLen == 0 || bodyLen > 1<<20 {
			errc <- fmt.Errorf("invalid hello frame length %d", bodyLen)
			return
		}
		body := make([]byte, bodyLen)
		if _, err := io.ReadFull(stdout, body); err != nil {
			errc <- fmt.Errorf("read hello frame body: %w; stderr=%s", err, tailString(stderr.String(), 2000))
			return
		}
		if body[0] != 3 {
			errc <- fmt.Errorf("hello response type=%d, want 3", body[0])
			return
		}
		if len(body) < 5 {
			errc <- fmt.Errorf("short hello payload: %d bytes", len(body)-1)
			return
		}
		jsonLen := binary.LittleEndian.Uint32(body[1:5])
		if int(jsonLen) != len(body)-5 {
			errc <- fmt.Errorf("hello json length=%d, payload=%d", jsonLen, len(body)-5)
			return
		}
		var resp helloResponse
		if err := json.Unmarshal(body[5:], &resp); err != nil {
			errc <- fmt.Errorf("parse hello json: %w", err)
			return
		}
		if resp.Protocol != 1 {
			errc <- fmt.Errorf("oracle protocol=%d, want 1", resp.Protocol)
			return
		}
		if resp.Engine != engine {
			errc <- fmt.Errorf("oracle engine=%q, want %q", resp.Engine, engine)
			return
		}
		errc <- nil
	}()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		if pgid > 0 {
			_ = syscall.Kill(-pgid, syscall.SIGKILL)
		}
		return fmt.Errorf("hello smoke timed out after 10s")
	}
}
