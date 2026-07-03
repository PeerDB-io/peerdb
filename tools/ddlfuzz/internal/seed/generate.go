//go:build ignore

package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/seed"
)

func main() {
	root, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	for filepath.Base(root) != "ddlfuzz" {
		parent := filepath.Dir(root)
		if parent == root {
			panic("run from inside tools/ddlfuzz")
		}
		root = parent
	}
	flowMysql := filepath.Clean(filepath.Join(root, "..", "..", "flow", "connectors", "mysql"))
	seeds, err := seed.Extract(flowMysql)
	if err != nil {
		panic(err)
	}
	out := filepath.Join(root, "seeds", "seeds.jsonl")
	if err := seed.WriteJSONL(out, seeds); err != nil {
		panic(err)
	}
	fmt.Printf("wrote %d seeds to %s\n", len(seeds), out)
}
