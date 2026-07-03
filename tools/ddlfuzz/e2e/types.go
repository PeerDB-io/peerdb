//go:build ddlfuzz

package e2e

import (
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	EngineMySQL   = "mysql"
	EngineMariaDB = "mariadb"

	fixtureTable = "fixture"
	rootUser     = "root"
	rootPassword = "ddlfuzz"
)

// Config controls one live-binlog e2e lane process.
type Config struct {
	StateDir    string
	Engines     []string
	Workers     int
	Cases       uint64
	MySQLAddr   string
	MariaDBAddr string
	Smoke       bool
}

func DefaultConfig() Config {
	return Config{
		StateDir:    "../state",
		Engines:     []string{EngineMySQL, EngineMariaDB},
		Workers:     4,
		MySQLAddr:   "127.0.0.1:13306",
		MariaDBAddr: "127.0.0.1:13307",
	}
}

// Summary is returned by Run and printed by the CLI.
type Summary struct {
	StartedAt time.Time
	Elapsed   time.Duration
	Engines   map[string]EngineSummary
}

type EngineSummary struct {
	Cases          uint64
	ExecRejects    uint64
	Findings       uint64
	MatchedDDLs    uint64
	Markers        uint64
	Controls       uint64
	Resets         uint64
	QueueDone      uint64
	CasesPerSecond float64
	PaletteHits    map[string]uint64
	GeneratorPanic bool
}

type engineConfig struct {
	Name      string
	IsMariaDB bool
	Addr      string
	Host      string
	Port      uint16
	Flavor    string
	Image     string
}

func parseEngineList(cfg Config) ([]engineConfig, error) {
	seen := make(map[string]bool)
	var out []engineConfig
	for _, raw := range cfg.Engines {
		for _, part := range strings.Split(raw, ",") {
			name := strings.ToLower(strings.TrimSpace(part))
			if name == "" || seen[name] {
				continue
			}
			seen[name] = true
			switch name {
			case EngineMySQL:
				ec, err := newEngineConfig(name, cfg.MySQLAddr)
				if err != nil {
					return nil, err
				}
				out = append(out, ec)
			case EngineMariaDB:
				ec, err := newEngineConfig(name, cfg.MariaDBAddr)
				if err != nil {
					return nil, err
				}
				out = append(out, ec)
			default:
				return nil, fmt.Errorf("unknown engine %q", name)
			}
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no engines selected")
	}
	return out, nil
}

func newEngineConfig(name, addr string) (engineConfig, error) {
	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		return engineConfig{}, fmt.Errorf("%s addr %q: %w", name, addr, err)
	}
	port64, err := strconv.ParseUint(portText, 10, 16)
	if err != nil {
		return engineConfig{}, fmt.Errorf("%s addr %q: invalid port: %w", name, addr, err)
	}
	ec := engineConfig{
		Name:   name,
		Addr:   addr,
		Host:   host,
		Port:   uint16(port64),
		Flavor: name,
	}
	switch name {
	case EngineMySQL:
		ec.Image = "mysql:9.7.0"
	case EngineMariaDB:
		ec.IsMariaDB = true
		ec.Image = "mariadb:13.0.1-rc"
	}
	return ec, nil
}

func absStateDir(path string) (string, error) {
	if path == "" {
		path = DefaultConfig().StateDir
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	return abs, nil
}

type harnessError struct {
	Engine string
	Err    error
}

func (e harnessError) Error() string {
	if e.Engine == "" {
		return e.Err.Error()
	}
	return e.Engine + ": " + e.Err.Error()
}

func schemaName(workerID int) string {
	return fmt.Sprintf("fuzz_w%d", workerID)
}

func quoteIdent(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
