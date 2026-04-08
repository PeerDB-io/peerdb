package e2e

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func switchboardDSN(peer string, options map[string]string) string {
	dsn := fmt.Sprintf("postgres://peerdb:peerdb@127.0.0.1:5732/%s?sslmode=disable", peer)
	if len(options) > 0 {
		var optParts []string
		for k, v := range options {
			optParts = append(optParts, fmt.Sprintf("-c peerdb.%s=%s", k, v))
		}
		dsn += "&options=" + strings.Join(optParts, " ")
	}
	return dsn
}

func runPsql(t *testing.T, peer string, extraArgs ...string) (string, error) {
	t.Helper()
	args := append([]string{"-h", "127.0.0.1", "-p", "5732", "-d", peer, "-U", "peerdb"}, extraArgs...)
	cmd := exec.Command("psql", args...)
	cmd.Env = append(os.Environ(), "PGPASSWORD=peerdb")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("psql failed: %s\nOutput: %s", err, string(output))
	}
	return strings.TrimSpace(string(output)), err
}
