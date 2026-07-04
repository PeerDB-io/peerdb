package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

type Result struct {
	ExitCode int
	TimedOut bool
	Stdout   string
	Stderr   string
	Duration time.Duration
}

type Proc struct {
	cmd     *exec.Cmd
	pgid    int
	done    chan struct{}
	waitErr error
	waitMu  sync.Mutex
	once    sync.Once
}

func Start(dir string, stdout, stderr io.Writer, name string, args ...string) (*Proc, error) {
	return StartWithStdin(dir, nil, stdout, stderr, name, args...)
}

func StartWithStdin(dir string, stdin io.Reader, stdout, stderr io.Writer, name string, args ...string) (*Proc, error) {
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.Stdin = stdin
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	pgid, err := syscall.Getpgid(cmd.Process.Pid)
	if err != nil {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		return nil, err
	}
	procTracker.register(cmd, pgid)
	p := &Proc{cmd: cmd, pgid: pgid, done: make(chan struct{})}
	go func() {
		err := cmd.Wait()
		p.waitMu.Lock()
		p.waitErr = err
		p.waitMu.Unlock()
		procTracker.deregister(cmd.Process.Pid)
		close(p.done)
	}()
	return p, nil
}

func (p *Proc) Wait() error {
	if p == nil {
		return nil
	}
	<-p.done
	p.waitMu.Lock()
	defer p.waitMu.Unlock()
	return p.waitErr
}

func (p *Proc) Done() <-chan struct{} {
	if p == nil {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	return p.done
}

func (p *Proc) Err() error {
	if p == nil {
		return nil
	}
	p.waitMu.Lock()
	defer p.waitMu.Unlock()
	return p.waitErr
}

func (p *Proc) StopGracefully(term time.Duration) {
	if p == nil {
		return
	}
	p.once.Do(func() {
		_ = syscall.Kill(-p.pgid, syscall.SIGTERM)
		timer := time.NewTimer(term)
		defer timer.Stop()
		select {
		case <-p.done:
			return
		case <-timer.C:
			_ = syscall.Kill(-p.pgid, syscall.SIGKILL)
			<-p.done
		}
	})
}

func (p *Proc) Kill() {
	if p == nil {
		return
	}
	p.once.Do(func() {
		_ = syscall.Kill(-p.pgid, syscall.SIGKILL)
		<-p.done
	})
}

func RunTimeout(ctx context.Context, dir string, timeout time.Duration, stdin io.Reader, name string, args ...string) (Result, error) {
	var stdout, stderr bytes.Buffer
	start := time.Now()
	p, err := StartWithStdin(dir, stdin, &stdout, &stderr, name, args...)
	if err != nil {
		return Result{ExitCode: -1, Stdout: stdout.String(), Stderr: stderr.String()}, err
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	res := Result{ExitCode: -1}
	var waitErr error
	select {
	case <-p.Done():
		waitErr = p.Err()
	case <-timer.C:
		res.TimedOut = true
		p.Kill()
		waitErr = p.Err()
	case <-ctx.Done():
		p.Kill()
		waitErr = ctx.Err()
	}
	res.Duration = time.Since(start)
	res.Stdout = stdout.String()
	res.Stderr = stderr.String()
	if !res.TimedOut && waitErr == nil {
		res.ExitCode = 0
		return res, nil
	}
	if !res.TimedOut {
		res.ExitCode = exitCode(waitErr)
	}
	if res.TimedOut {
		return res, fmt.Errorf("%s timed out after %s", name, timeout)
	}
	return res, waitErr
}

func exitCode(err error) int {
	if err == nil {
		return 0
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		if status, ok := exitErr.Sys().(syscall.WaitStatus); ok {
			return status.ExitStatus()
		}
	}
	return -1
}

func resultOutputTail(res Result, n int) string {
	out := res.Stdout
	if res.Stderr != "" {
		if out != "" {
			out += "\n"
		}
		out += res.Stderr
	}
	return tailString(out, n)
}
