package main

import (
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strings"
	"time"
)

type OracleManifest struct {
	Engine     string    `json:"engine"`
	SourceHash string    `json:"source_hash"`
	BuiltAt    time.Time `json:"built_at"`
	ServerTag  string    `json:"server_tag"`
}

func oracleManifestCommand(cfg Config, args []string) error {
	fs := flag.NewFlagSet("ddlsuper oracle-manifest", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	var engine, out string
	fs.StringVar(&engine, "engine", "", "mysql or mariadb")
	fs.StringVar(&out, "out", "", "manifest output path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if engine != "mysql" && engine != "mariadb" {
		return fmt.Errorf("oracle-manifest requires --engine mysql|mariadb")
	}
	if out == "" {
		return fmt.Errorf("oracle-manifest requires --out")
	}
	hash, err := OracleSourceHash(cfg, engine)
	if err != nil {
		return err
	}
	manifest := OracleManifest{
		Engine:     engine,
		SourceHash: hash,
		BuiltAt:    time.Now().UTC(),
		ServerTag:  oracleServerTag(engine),
	}
	return atomicWriteJSON(out, manifest, 0o644)
}

func OracleSourceHash(cfg Config, engine string) (string, error) {
	var roots []string
	for _, rel := range []string{"tools/ddlfuzz/oracle/proto", "tools/ddlfuzz/oracle/" + engine} {
		roots = append(roots, filepath.Join(cfg.Root, filepath.FromSlash(rel)))
	}
	type item struct {
		rel string
		sum string
	}
	var items []item
	for _, root := range roots {
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if d.IsDir() {
				if d.Name() == "__pycache__" {
					return fs.SkipDir
				}
				return nil
			}
			info, err := d.Info()
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				return nil
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			sum := sha256.Sum256(data)
			rel, err := filepath.Rel(cfg.Root, path)
			if err != nil {
				return err
			}
			items = append(items, item{rel: filepath.ToSlash(rel), sum: hex.EncodeToString(sum[:])})
			return nil
		})
		if err != nil {
			return "", err
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].rel < items[j].rel })
	h := sha256.New()
	for _, it := range items {
		h.Write([]byte(it.rel))
		h.Write([]byte{0})
		h.Write([]byte(it.sum))
		h.Write([]byte{'\n'})
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func LoadOracleManifest(path string) (OracleManifest, error) {
	var manifest OracleManifest
	err := readJSONFile(path, &manifest)
	return manifest, err
}

func oracleManifestPath(cfg Config, engine string) string {
	return filepath.Join(cfg.BuildDir, "oracle-"+engine+".manifest.json")
}

func oracleBinaryPath(cfg Config, engine string) string {
	switch engine {
	case "mysql":
		return cfg.MySQLOracle
	case "mariadb":
		return cfg.MariaOracle
	default:
		return filepath.Join(cfg.BuildDir, "oracle-"+engine)
	}
}

func oracleServerTag(engine string) string {
	switch engine {
	case "mysql":
		return "mysql-9.7.0"
	case "mariadb":
		return "mariadb-c3ec2dc368a8c7165cdbea58208eb828e76ebc57"
	default:
		return engine
	}
}

func buildStamp() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, setting := range info.Settings {
		if setting.Key == "vcs.revision" && strings.TrimSpace(setting.Value) != "" {
			return setting.Value
		}
	}
	return "unknown"
}
