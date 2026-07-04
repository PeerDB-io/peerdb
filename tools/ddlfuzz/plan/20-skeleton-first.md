# 20 — SKELETON FIRST (landing commit before the full fuzzer build)

**Read this before `21-fuzzer.md`.** Your first commit on your branch is a *compiling skeleton*:
the Go module, the export shim, and stub packages with the **exact exported signatures** that
components 30 (e2e lane) and 40 (supervisor) compile against. Landing it first unblocks those two
agents to start in parallel while you implement the real `21-fuzzer.md` behind these frozen
interfaces.

Rules:
- The signatures in this doc are a **contract**. Once committed, do not change a stubbed exported
  name, parameter list, or struct field without flagging it — 30/40 build against them. You may
  add fields/functions freely; you may not rename or re-type what's here.
- Stub bodies compile and either return zero values or `panic("ddlfuzz: not implemented")`. The
  real bodies come from `21-fuzzer.md`. The **shim is real, not a stub** (30 needs it to run).
- Everything conforms to `00-overview.md` (binding: module path, digest schema, signature grammar,
  state-dir layout, D1–D12 reconciliation decisions).
- Work in your own git worktree/branch. Do not commit to `parser-wip` directly.

## Acceptance for this commit

From `/Users/ilia/Code/peerdb`:

```
cd flow && go build ./...                        # shim compiles
cd ../tools/ddlfuzz && go build ./... && go vet ./... && go test ./...   # module green
go build -o build/ddlfuzz ./cmd/ddlfuzz          # binary builds
```

Then one commit: `ddlfuzz: module skeleton + frozen interfaces` (branch = your worktree branch, no
co-author trailer). Message body: "Compiling scaffold; real behavior per plan/21-fuzzer.md."

## Deliverables (create exactly these)

### 1. `tools/ddlfuzz/go.mod`

```
module github.com/PeerDB-io/peerdb/tools/ddlfuzz

go 1.26

require github.com/PeerDB-io/peerdb/flow v0.0.0

replace github.com/PeerDB-io/peerdb/flow => ../../flow
```

Run `go mod tidy` once after the shim exists (step 2) so the flow module's transitive deps resolve
through its `go.sum`. Do **not** add TiDB or heavy deps here; stdlib + `flow` plus sanctioned
go-mysql and modernc.org/sqlite deps only (add `golang.org/x/sync` later only if `internal/run`
needs errgroup).

### 2. `tools/ddlfuzz/.gitignore`

```
/build/
/state/
```

### 3. Export shim — `flow/connectors/mysql/ddlfuzz_export.go` (REAL, not a stub)

Exactly this (all three functions; 30 consumes `FuzzSQLModeFromStatusVars` and `FuzzParseForE2E`,
40 uses none of these directly):

```go
package connmysql

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// FuzzDDLSignature parses query with parseQueryEvent and reduces the result to
// the ddlfuzz comparison signature (grammar in plan/00-overview.md, identical to
// ddl_parser_tidb_diff_test.go's ddlDiffSignature). Panics are NOT recovered
// here; the fuzz driver contains them.
func FuzzDDLSignature(query []byte, sqlMode uint64, isMariaDB bool) (string, error) {
	stmts, err := parseQueryEvent(query, sqlMode, isMariaDB)
	if err != nil {
		return "", err
	}
	return fuzzDDLSignature(stmts), nil
}

// FuzzSQLModeFromStatusVars re-exports the binlog status-var sql_mode walker for
// the e2e lane's plumbing check against real QueryEvent status vars.
func FuzzSQLModeFromStatusVars(statusVars []byte) (uint64, bool) {
	return sqlModeFromStatusVars(statusVars)
}

// FuzzParseForE2E parses like the production QueryEvent path and returns the
// parsed statements as JSON structurally parallel to the oracle digest, per
// 30-e2e-lane.md §Interfaces (precision/scale ints carry ddlColumnDef verbatim,
// -1 = not written; no "other" entries). err mirrors parseQueryEvent; on err the
// JSON is the literal null.
func FuzzParseForE2E(query []byte, sqlMode uint64, isMariaDB bool) ([]byte, error) {
	stmts, err := parseQueryEvent(query, sqlMode, isMariaDB)
	if err != nil {
		return []byte("null"), err
	}
	return json.Marshal(fuzzDDLStmtsToE2E(stmts))
}

func fuzzDDLColSig(c ddlColumnDef) string {
	var sb strings.Builder
	sb.WriteString(c.Name)
	sb.WriteByte('=')
	kind, err := QkindFromMysqlColumnType(c.TypeStr, true, 0)
	if err != nil {
		sb.WriteString("ERR")
	} else {
		sb.WriteString(string(kind))
	}
	if kind == types.QValueKindNumeric {
		fmt.Fprintf(&sb, "(%d,%d)", c.Precision, c.Scale)
	}
	if c.NotNull {
		sb.WriteString(" nn")
	}
	return sb.String()
}

func fuzzDDLSpecSig(sp ddlAlterSpec) string {
	var sb strings.Builder
	switch {
	case sp.NewColumnName != "":
		sb.WriteString("ren " + sp.OldColumnName + ">" + sp.NewColumnName)
	case len(sp.NewColumns) > 0:
		if sp.OldColumnName != "" {
			sb.WriteString("chg " + sp.OldColumnName + " ")
		} else {
			sb.WriteString("col ")
		}
		for i, c := range sp.NewColumns {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(fuzzDDLColSig(c))
		}
	default:
		sb.WriteString("drop " + sp.OldColumnName)
	}
	if sp.HasPosition {
		sb.WriteString(" @pos")
	}
	return sb.String()
}

func fuzzDDLQual(schema, table string) string {
	if schema == "" {
		return table
	}
	return schema + "." + table
}

func fuzzDDLSignature(stmts []ddlStatement) string {
	var parts []string
	for _, s := range stmts {
		switch st := s.(type) {
		case *ddlAlterTable:
			specs := make([]string, len(st.Specs))
			for i, sp := range st.Specs {
				specs[i] = fuzzDDLSpecSig(sp)
			}
			parts = append(parts, "alter "+fuzzDDLQual(st.Schema, st.Table)+"{"+strings.Join(specs, "; ")+"}")
		case *ddlRenameTable:
			prs := make([]string, len(st.Pairs))
			for i, p := range st.Pairs {
				prs[i] = fuzzDDLQual(p.OldSchema, p.OldTable) + ">" + fuzzDDLQual(p.NewSchema, p.NewTable)
			}
			parts = append(parts, "rename "+strings.Join(prs, ", "))
		}
	}
	return strings.Join(parts, " | ")
}

// --- e2e JSON (30-e2e-lane.md §Interfaces is normative for the shape) ---

type e2eCol struct {
	Name      string `json:"name"`
	TypeStr   string `json:"type_str"`
	NotNull   bool   `json:"not_null"`
	Precision int    `json:"precision"`
	Scale     int    `json:"scale"`
}
type e2eSpec struct {
	Op          string    `json:"op"` // add | change | rename_col | drop
	OldName     string    `json:"old_name,omitempty"`
	NewName     string    `json:"new_name,omitempty"`
	Cols        []e2eCol  `json:"cols,omitempty"`
	HasPosition bool      `json:"has_position"`
}
type e2ePair struct {
	OldSchema string `json:"old_schema"`
	OldTable  string `json:"old_table"`
	NewSchema string `json:"new_schema"`
	NewTable  string `json:"new_table"`
}
type e2eStmt struct {
	Kind   string    `json:"kind"` // alter_table | rename_table
	Schema string    `json:"schema,omitempty"`
	Table  string    `json:"table,omitempty"`
	Specs  []e2eSpec `json:"specs,omitempty"`
	Pairs  []e2ePair `json:"pairs,omitempty"`
}
type e2eStmts struct {
	Stmts []e2eStmt `json:"stmts"`
}

func fuzzDDLColToE2E(c ddlColumnDef) e2eCol {
	return e2eCol{Name: c.Name, TypeStr: c.TypeStr, NotNull: c.NotNull, Precision: c.Precision, Scale: c.Scale}
}

func fuzzDDLStmtsToE2E(stmts []ddlStatement) e2eStmts {
	out := e2eStmts{Stmts: []e2eStmt{}}
	for _, s := range stmts {
		switch st := s.(type) {
		case *ddlAlterTable:
			es := e2eStmt{Kind: "alter_table", Schema: st.Schema, Table: st.Table, Specs: []e2eSpec{}}
			for _, sp := range st.Specs {
				var spec e2eSpec
				spec.HasPosition = sp.HasPosition
				switch {
				case sp.NewColumnName != "":
					spec.Op = "rename_col"
					spec.OldName = sp.OldColumnName
					spec.NewName = sp.NewColumnName
				case len(sp.NewColumns) > 0 && sp.OldColumnName != "":
					spec.Op = "change"
					spec.OldName = sp.OldColumnName
					for _, c := range sp.NewColumns {
						spec.Cols = append(spec.Cols, fuzzDDLColToE2E(c))
					}
				case len(sp.NewColumns) > 0:
					spec.Op = "add"
					for _, c := range sp.NewColumns {
						spec.Cols = append(spec.Cols, fuzzDDLColToE2E(c))
					}
				default:
					spec.Op = "drop"
					spec.OldName = sp.OldColumnName
				}
				es.Specs = append(es.Specs, spec)
			}
			out.Stmts = append(out.Stmts, es)
		case *ddlRenameTable:
			es := e2eStmt{Kind: "rename_table"}
			for _, p := range st.Pairs {
				es.Pairs = append(es.Pairs, e2ePair{p.OldSchema, p.OldTable, p.NewSchema, p.NewTable})
			}
			out.Stmts = append(out.Stmts, es)
		}
	}
	return out
}
```

Verify the unexported names against the current parser source before typing: `parseQueryEvent`,
`sqlModeFromStatusVars`, `ddlStatement`, `ddlAlterTable{Schema,Table,Specs}`,
`ddlAlterSpec{OldColumnName,NewColumnName,NewColumns,HasPosition}`,
`ddlColumnDef{Name,TypeStr,Precision,Scale,NotNull}`, `ddlRenameTable{Pairs}`,
`ddlRenamePair{OldSchema,OldTable,NewSchema,NewTable}`, `QkindFromMysqlColumnType` — all present in
`flow/connectors/mysql/ddl_parser.go` / `type_conversion.go`. If a name differs, match the source
(the parser is authoritative), not this doc.

Also add the parity guard `flow/connectors/mysql/ddlfuzz_export_test.go` per
`21-fuzzer.md` step 1 — it can be a `t.Skip("filled in with full impl")` stub for now, but the file
must compile.

### 4. `internal/` stub packages — exact exported surface 30 imports

Create each package with these signatures. Bodies: `panic("ddlfuzz: not implemented")` or zero
returns. Keep the doc comments so intent survives.

**`internal/digest/digest.go`** — data types (real structs; used by compare/replay and by 40 to
render `oracle_digest`):

```go
package digest

type Digest struct {
	Verdict string `json:"verdict"` // "accept" | "reject"
	Error   string `json:"error,omitempty"`
	Stmts   []Stmt `json:"stmts,omitempty"`
}
type Stmt struct {
	Kind   string `json:"kind"` // other | alter_table | rename_table
	Schema string `json:"schema"`
	Table  string `json:"table"`
	Specs  []Spec `json:"specs"`
	Pairs  []Pair `json:"pairs"`
}
type Spec struct {
	Op          string `json:"op"` // add | modify | change | drop | rename_col
	OldName     string `json:"old_name"`
	NewName     string `json:"new_name"`
	Cols        []Col  `json:"cols"`
	HasPosition bool   `json:"has_position"`
}
type Pair struct {
	OldSchema string `json:"old_schema"`
	OldTable  string `json:"old_table"`
	NewSchema string `json:"new_schema"`
	NewTable  string `json:"new_table"`
}
type Col struct {
	Name          string `json:"name"`
	TypeStr       string `json:"type_str"`
	NotNull       bool   `json:"not_null"`
	ParamsWritten []int  `json:"params_written"` // nil when JSON null
}
```

**`internal/gen/gen.go`** — consumed by 30:

```go
package gen

import "math/rand/v2"

// Vocab is the fixture vocabulary the constrained (e2e) profile draws from.
type Vocab struct {
	Table      string   // unqualified fixture table name
	Columns    []string // current live column names
	FreshNames []string // pool for new column names
	Types      []string // written type forms, engine-specific
	IsMariaDB  bool
}

// Profile constrains GenerateConstrained for the e2e lane.
type Profile struct {
	HeadsAlterOnly    bool
	NoAlterRenameTo   bool
	NoConvertCharset  bool
	RenameTableWeight float64
	MaxSpecs          int
}

// GenerateConstrained emits one DDL statement drawn from v under p. (Full grammar
// tables per 21-fuzzer.md step 12; the unconstrained generator shares them.)
func GenerateConstrained(r *rand.Rand, v Vocab, p Profile) string {
	panic("ddlfuzz: not implemented")
}
```

**`internal/findings/findings.go`** — consumed by 30 (and 40 reads the dirs it writes):

```go
package findings

// Finding is one divergence to record. Class must be from the D4 vocabulary
// (00-overview). Meta is merged verbatim into meta.json (additive keys).
type Finding struct {
	Class     string
	Engine    string // "mysql" | "mariadb"
	SQLMode   uint64
	Lane      string // "fast" | "e2e"
	Statement []byte
	OurSig    string
	OurError  string
	Meta      map[string]any
}

// Record computes the canonical descriptor + <sig>, dedups against ledger.jsonl /
// parked.list, and writes state/findings/<sig>/{repro.sql,meta.json} atomically.
// isNew is false when the sig already existed (times_seen bumped).
func Record(stateDir string, f Finding) (sig string, isNew bool, err error) {
	panic("ddlfuzz: not implemented")
}
```

**`internal/compare/compare.go`** — `SplitTopLevel` consumed by 30; the rest is your full-impl
home. Land just the splitter signature now (30 reuses it for spec-list bisection):

```go
package compare

// SplitTopLevel splits b on sep at paren depth 0, honoring string/quoted-ident
// delimiters, returning the segments (separators removed).
func SplitTopLevel(b []byte, sep byte) [][]byte {
	panic("ddlfuzz: not implemented")
}
```

### 5. `cmd/ddlfuzz/main.go` — subcommand skeleton (40 shells out to these)

Real flag parsing + dispatch for `fuzz | golden | replay`; stub actions print
`"<cmd>: not implemented"` to stderr and `os.Exit(70)`. This lets 40 be written against the CLI
shape now; the documented exit codes (0/10/11/1 for `replay`, per D5) are wired in with the full
impl. Include `-h`/per-subcommand usage. Keep the flag names from `21-fuzzer.md` §CLI flags so 40's
invocations don't churn.

### 6. `tools/ddlfuzz/seeds/` and `state/` presence

- `seeds/.gitkeep` (real `seeds.jsonl` is produced by your seed-extraction step later).
- Do **not** commit `state/` or `build/` (gitignored). The state dir is created at runtime.

### 7. Package stubs must be import-clean

Every `internal/*` package above must build and pass `go vet` on its own. Add a trivial
`_test.go` per package only if needed to keep `go test ./...` green (an empty test file is fine);
do not write behavioral tests yet — those ship with the real implementation.

## Handoff signal

Once the acceptance block is green and the skeleton commit is pushed to your branch, that is the
cue (relay it to the coordinator) for the 30 and 40 agents to start: 30 imports
`internal/{gen,findings,compare}` + the shim; 40 builds the module and shells the
`cmd/ddlfuzz` CLI. Then continue with the full `21-fuzzer.md` behind these frozen signatures.
