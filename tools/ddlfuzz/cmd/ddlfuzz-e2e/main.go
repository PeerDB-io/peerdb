package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/e2e"
)

func main() {
	cfg := e2e.DefaultConfig()
	var engines string
	flag.StringVar(&cfg.StateDir, "state", cfg.StateDir, "state directory")
	flag.StringVar(&engines, "engines", strings.Join(cfg.Engines, ","), "comma-separated engines: mysql,mariadb")
	flag.IntVar(&cfg.Workers, "workers", cfg.Workers, "workers per engine")
	flag.Uint64Var(&cfg.Cases, "cases", 0, "cases per engine; 0 runs until signal")
	flag.StringVar(&cfg.MySQLAddr, "mysql-addr", cfg.MySQLAddr, "MySQL host:port")
	flag.StringVar(&cfg.MariaDBAddr, "mariadb-addr", cfg.MariaDBAddr, "MariaDB host:port")
	flag.BoolVar(&cfg.Smoke, "smoke", false, "run smoke integrity checks")
	flag.Parse()

	cfg.Engines = strings.Split(engines, ",")
	summary, err := e2e.Run(context.Background(), cfg)
	printSummary(summary)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ddlfuzz-e2e: %v\n", err)
		os.Exit(3)
	}
}

func printSummary(summary e2e.Summary) {
	names := make([]string, 0, len(summary.Engines))
	for name := range summary.Engines {
		names = append(names, name)
	}
	sort.Strings(names)
	if len(names) == 0 {
		return
	}
	parts := make([]string, 0, len(names))
	for _, name := range names {
		es := summary.Engines[name]
		parts = append(parts, fmt.Sprintf("%s cases=%d rejects=%d findings=%d rate=%.1f/s", name, es.Cases, es.ExecRejects, es.Findings, es.CasesPerSecond))
	}
	fmt.Println("smoke: " + strings.Join(parts, "; "))
}
