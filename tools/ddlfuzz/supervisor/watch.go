package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
	"unsafe"
)

func statusCommand(cfg Config, args []string) error {
	fs := flag.NewFlagSet("ddlsuper status", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	var jsonOut, noColor bool
	fs.BoolVar(&jsonOut, "json", false, "emit JSON")
	fs.BoolVar(&noColor, "no-color", false, "disable ANSI color")
	if err := fs.Parse(args); err != nil {
		return err
	}
	snap := CollectStatus(cfg)
	if jsonOut {
		data, err := json.MarshalIndent(snap, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(data))
		return nil
	}
	fmt.Print(RenderStatus(snap, colorEnabled() && !noColor, terminalWidth()))
	return nil
}

func watchCommand(cfg Config, args []string) error {
	fs := flag.NewFlagSet("ddlsuper watch", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	var intervalText string
	var noColor bool
	fs.StringVar(&intervalText, "interval", "3s", "repaint interval")
	fs.BoolVar(&noColor, "no-color", false, "disable ANSI color")
	if err := fs.Parse(args); err != nil {
		return err
	}
	interval, err := time.ParseDuration(intervalText)
	if err != nil {
		return err
	}
	if interval < time.Second {
		interval = time.Second
	}
	tty := isStdoutTTY()
	color := tty && colorEnabled() && !noColor
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if tty {
		fmt.Print("\x1b[?1049h\x1b[?25l")
		defer fmt.Print("\x1b[?25h\x1b[?1049l")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		frame := RenderStatus(CollectStatus(cfg), color, terminalWidth())
		_, height := terminalSize()
		if tty {
			frame = fitFrameHeight(frame, height)
		}
		if tty {
			fmt.Print("\x1b[H")
			for _, line := range splitFrame(frame) {
				fmt.Print(line, "\x1b[K\n")
			}
			fmt.Print("\x1b[J")
		} else {
			fmt.Print(frame, "\n")
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func fitFrameHeight(frame string, height int) string {
	lines := splitFrame(frame)
	if height <= 0 || len(lines) <= height {
		return frame
	}
	for len(lines) > height {
		idx := lastEventBodyLine(lines)
		if idx < 0 {
			break
		}
		lines = append(lines[:idx], lines[idx+1:]...)
	}
	for len(lines) > height {
		idx := lastFindingBodyLine(lines)
		if idx < 0 {
			break
		}
		lines = append(lines[:idx], lines[idx+1:]...)
	}
	if len(lines) > height {
		lines = lines[:height]
	}
	return strings.Join(lines, "\n") + "\n"
}

func lastEventBodyLine(lines []string) int {
	inEvents := false
	last := -1
	for i, line := range lines {
		plain := stripANSI(line)
		if strings.Contains(plain, "EVENTS ") {
			inEvents = true
			continue
		}
		if inEvents && strings.TrimSpace(plain) != "" {
			last = i
		}
	}
	return last
}

func lastFindingBodyLine(lines []string) int {
	inFindings := false
	last := -1
	for i, line := range lines {
		plain := stripANSI(line)
		if strings.Contains(plain, "FINDINGS ") {
			inFindings = true
			continue
		}
		if inFindings && (strings.Contains(plain, "FIX AGENT ") || strings.Contains(plain, "EVENTS ")) {
			inFindings = false
		}
		if inFindings && strings.TrimSpace(plain) != "" && !strings.Contains(plain, "CLASS|SHAPE") {
			last = i
		}
	}
	return last
}

func splitFrame(s string) []string {
	var out []string
	start := 0
	for i, r := range s {
		if r == '\n' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		out = append(out, s[start:])
	}
	return out
}

func colorEnabled() bool {
	if os.Getenv("NO_COLOR") != "" || os.Getenv("TERM") == "dumb" {
		return false
	}
	return isStdoutTTY()
}

func isStdoutTTY() bool {
	info, err := os.Stdout.Stat()
	return err == nil && (info.Mode()&os.ModeCharDevice) != 0
}

func terminalWidth() int {
	w, _ := terminalSize()
	if w <= 0 {
		return 130
	}
	return w
}

func terminalSize() (int, int) {
	type winsize struct {
		Row    uint16
		Col    uint16
		Xpixel uint16
		Ypixel uint16
	}
	var ws winsize
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, os.Stdout.Fd(), uintptr(syscall.TIOCGWINSZ), uintptr(unsafe.Pointer(&ws)))
	if errno != 0 || ws.Col == 0 || ws.Row == 0 {
		return 130, 30
	}
	return int(ws.Col), int(ws.Row)
}
