package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"golang.org/x/mod/modfile"
)

// pinnedVersions is the source of truth for dependencies that must not be
// accidentally upgraded. Each entry maps a module path to the exact version
// that should appear in go.mod.
//
// To update a pinned dependency, change both go.mod, this map AND
// the Renovate package rules in renovate.json that skips automated updates for that dependency.
var pinnedVersions = map[string]string{
	"cloud.google.com/go/auth":                        "v0.18.2",
	"cloud.google.com/go/bigquery":                    "v1.72.0",
	"cloud.google.com/go/kms":                         "v1.24.0",
	"cloud.google.com/go/pubsub/v2":                   "v2.3.0",
	"cloud.google.com/go/storage":                     "v1.59.2",
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager": "v1.21.0",
	"google.golang.org/api":                           "v0.257.0",
	"github.com/tikv/pd/client":                       "v0.0.0-20251229071808-6173d50c004c",
	"github.com/jackc/pgx/v5":                         "v5.9.1",
}

func main() {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		fmt.Fprintln(os.Stderr, "failed to determine source file path")
		os.Exit(1)
	}
	goModPath := filepath.Join(filepath.Dir(thisFile), "..", "..", "go.mod")
	data, err := os.ReadFile(goModPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to read go.mod: %v\n", err)
		os.Exit(1)
	}

	f, err := modfile.Parse("go.mod", data, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to parse go.mod: %v\n", err)
		os.Exit(1)
	}

	found := make(map[string]string)
	for _, req := range f.Require {
		for _, comment := range req.Syntax.Suffix {
			if strings.Contains(comment.Token, "PINNED") {
				found[req.Mod.Path] = req.Mod.Version
				break
			}
		}
	}
	for _, rep := range f.Replace {
		for _, comment := range rep.Syntax.Suffix {
			if strings.Contains(comment.Token, "PINNED") {
				found[rep.Old.Path] = rep.New.Version
				break
			}
		}
	}

	failed := false
	for mod, pinnedVer := range pinnedVersions {
		ver, ok := found[mod]
		if !ok {
			fmt.Fprintf(os.Stderr, "pinned module %s (expected %s) not found with"+
				" PINNED comment in go.mod — was the comment removed?\n", mod, pinnedVer)
			failed = true
			continue
		}
		if ver != pinnedVer {
			fmt.Fprintf(os.Stderr, "pinned module %s: go.mod has %s, expected %s —"+
				" if this upgrade is intentional, update pinnedVersions in cmd/check_pinned_versions/main.go\n", mod, ver, pinnedVer)
			failed = true
		}
	}

	for mod := range found {
		if _, ok := pinnedVersions[mod]; !ok {
			fmt.Fprintf(os.Stderr, "module %s has a PINNED comment in go.mod but is"+
				" not in pinnedVersions — add it to cmd/check_pinned_versions/main.go\n", mod)
			failed = true
		}
	}

	if failed {
		os.Exit(1)
	}
}
