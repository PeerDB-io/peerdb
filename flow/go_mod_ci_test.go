package main

import (
	"os"
	"strings"
	"testing"

	"golang.org/x/mod/modfile"
)

// pinnedVersions is the source of truth for dependencies that must not be
// accidentally upgraded. Each entry maps a module path to the exact version
// that should appear in go.mod.
//
// To update a pinned dependency, change both go.mod AND this map.
var pinnedVersions = map[string]string{
	"cloud.google.com/go/bigquery":                    "v1.72.0",
	"cloud.google.com/go/pubsub/v2":                   "v2.3.0",
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager": "v1.21.0",
	"google.golang.org/api":                           "v0.257.0",
	"github.com/tikv/pd/client":                       "v0.0.0-20251229071808-6173d50c004c",
}

func TestPinnedDependencies(t *testing.T) {
	data, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatalf("failed to read go.mod: %v", err)
	}

	f, err := modfile.Parse("go.mod", data, nil)
	if err != nil {
		t.Fatalf("failed to parse go.mod: %v", err)
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

	for mod, pinnedVer := range pinnedVersions {
		ver, ok := found[mod]
		if !ok {
			t.Errorf("pinned module %s (expected %s) not found with PINNED comment in go.mod — "+
				"was the comment removed?", mod, pinnedVer)
			continue
		}
		if ver != pinnedVer {
			t.Errorf("pinned module %s: go.mod has %s, expected %s — "+
				"if this upgrade is intentional, update pinnedVersions in go_mod_test.go", mod, ver, pinnedVer)
		}
	}

	for mod := range found {
		if _, ok := pinnedVersions[mod]; !ok {
			t.Errorf("module %s has a PINNED comment in go.mod but is not in pinnedVersions — "+
				"add it to go_mod_test.go", mod)
		}
	}
}
