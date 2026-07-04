# 20 — Go differential driver (`ddlfuzz`)

Component plan for the Go module `github.com/PeerDB-io/peerdb/tools/ddlfuzz` at
`/Users/ilia/Code/peerdb/tools/ddlfuzz`. Conforms to `plan/00-overview.md` (binding). Target
toolchain: go1.26 (host has go1.26.3 darwin/arm64).

## Goal

One binary, `cmd/ddlfuzz`, with four subcommands:

- `fuzz` — generate/mutate cases, run them through `FuzzDDLSignature` (contained) and the oracle
  pool, compare via the reconciliation rules, retain corpus on **oracle SanCov coverage**, file
  findings. ≥100k differential cases/s aggregate on ~10 oracle procs; crash-restartable from
  `state/`.
- `golden` — replay all existing test corpora through both oracles + our parser; report every
  irreconcilable case; exit 0 only when clean. Blocks fuzzing (enforced by 40).
- `replay` — re-execute `state/findings/*` and check each against its `status`; exit codes below.
- `minimize` — divergence-signature-preserving delta debugging of a finding's repro.

Plus one build-tagged export shim in the flow module (step 1) and a checked-in keyword dictionary
extracted from the servers' lexer symbol tables (step 8).

## Scope — single 72h campaign, not a reusable platform

This tool exists to find parser bugs in one 72h run against a parser known to be completable, then
retire. That framing trims several pieces from an earlier, platform-shaped draft; where a section
below still describes the heavier mechanism, this scope note governs:

- **Coverage-driven retention is oracle-SanCov-only.** The oracle inline-8bit bitmaps are cleanly
  attributable (small poll windows, per-engine OR-accumulation) and are the sole corpus-growth
  signal. The Go-side `runtime/coverage` covcounters machinery (parsing the ULEB payload, epoch
  snapshots, on-growth re-attribution) is **dropped from the run path** — its value was
  monitoring, and the oracle bitmap already steers generation into unexplored grammar. Step 10's
  Go-coverage subsection is retained only as an *optional* post-hoc report (build the binary with
  `-cover`, dump one cumulative profile at shutdown for a human to eyeball); nothing in the loop
  depends on it. `edges.go` in `stats.json` may be reported as 0/absent.
- **No periodic corpus distillation during the run.** Retain-on-new-oracle-edge, dedup by content
  hash, and a single optional distillation pass at graceful shutdown (to produce committable
  seeds) — no 30-minute minimization cron. Corpus growth is bounded by the finite grammar, not by
  a live GC.
- **Exit criteria are report metrics, not gates** (see 00-overview). The run ends at the 72h clock
  or on signal; the final report states coverage plateau, checklist coverage, and open/fixed/
  parked counts as *information*, not pass/fail.
- Everything else (containment, comparison + R1–R13, generator, mutators, seeds, golden, replay,
  minimize, oracle pool) is in scope as written.

## Interfaces

Consumed (per overview contracts — do not restate, conform):
- Oracle binaries `tools/ddlfuzz/build/oracle-mysql`, `tools/ddlfuzz/build/oracle-mariadb`
  (components 10/11), speaking wire protocol v1 on stdin/stdout, digest JSON schema as specified.
- `flow/connectors/mysql` via `replace ../../flow`; `QkindFromMysqlColumnType` (exported),
  `FuzzDDLSignature` (shim, `-tags ddlfuzz`).
- `state/` directory layout, `ledger.jsonl`, `parked.list`.

Provided:
- `ddlfuzz` binary at `tools/ddlfuzz/build/ddlfuzz` (optionally built with `-cover` for the
  post-hoc Go-coverage report only, see step 10; the run path does not need it).
- Findings written to `state/findings/<sig>/{repro.sql,meta.json}` per contract.
- Corpus + coverage files per the state-dir contract.
- Exit-code contract for `replay`/`golden` (used by 40): section "replay / minimize".
- The `<sig>` divergence-descriptor definition (section "Divergence descriptor"), referenced by
  30/40.
- **Exported Go APIs consumed by component 30** (in-process, same module; signatures normative in
  30-e2e-lane.md §Interfaces — implement to match):
  - `internal/gen.GenerateConstrained(r *rand.Rand, v gen.Vocab, p gen.Profile) string` — the
    grammar generator restricted to a fixture vocabulary (30 defines `Vocab`/`Profile` fields).
    Internally reuses the same tables as the unconstrained generator with the identifier and
    head/spec choices filtered per Profile.
  - `internal/findings.Record(stateDir string, f findings.Finding) (sig string, isNew bool, err error)`
    — the single findings writer (descriptor, dedup vs ledger/parked, atomic dir writes,
    index.jsonl append). `Finding` carries class, engine, sql_mode, lane, statement bytes,
    our_sig/our_error, and an opaque `Meta map[string]any` merged into meta.json.
  - `internal/minimize.Minimize(stmt []byte, sqlMode uint64, engine string, reproduces func([]byte) bool) []byte`
    — ddmin core with the caller-supplied predicate; plus
    `internal/minimize.FastLanePredicate(sig string) func([]byte) bool` (runs oracle+shim and
    checks descriptor equality) for 30's first-try minimization.
  - `internal/compare.SplitTopLevel(b []byte, sep byte) [][]byte` — the quote/paren-aware
    splitter (used by our minimizer's token stage; 30 reuses it for spec-list bisection).

## Design

### Package layout

```
tools/ddlfuzz/
  go.mod                       # module github.com/PeerDB-io/peerdb/tools/ddlfuzz
                               # require github.com/PeerDB-io/peerdb/flow v0.0.0
                               # replace github.com/PeerDB-io/peerdb/flow => ../../flow
                               # go 1.26
  cmd/ddlfuzz/main.go          # flag parsing, subcommand dispatch
  internal/wire/               # protocol v1 framing (client side)
  internal/oracle/             # process pool, respawn, crash bisection, HELLO, coverage polling
  internal/digest/             # digest JSON types + decoding
  internal/compare/            # oracle-side signature reduction, reconciliation, descriptor, dedup
  internal/exec/               # contained execution of FuzzDDLSignature (workers, watchdog)
  internal/gen/                # grammar-table generator
  internal/mutate/             # mutators + dictionary
  internal/dict/               # checked-in keyword dictionaries + extract.sh
  internal/seed/               # test-corpus extraction (go/ast)
  internal/corpus/             # SQLite corpus store, minimization policy
  internal/sancov/             # oracle bitmap accumulation (the sole coverage-retention signal)
  internal/findings/           # meta.json, ledger, parked, escalation-safe writes
  internal/run/                # fuzz orchestrator, stats, state flush
  internal/golden/             # golden subcommand
  internal/minimize/           # ddmin
```

`go.mod` also needs the transitive requirements of `flow` resolvable; run `go mod tidy` once —
because of the `replace`, the flow module's own deps come from its `go.sum`. Do **not** add TiDB or
other heavy deps to ddlfuzz directly; only `flow` and `golang.org/x/sync` (errgroup) if needed.
Keep stdlib-only otherwise.

### Case model

```go
// internal/run/case.go
type Case struct {
    SQL     []byte // statement bytes, passed verbatim
    SQLMode uint64
    Engine  uint8 // 0 = mysql, 1 = mariadb
    Origin  uint8 // gen | mut | corpus | golden | replay
    Seed    uint64 // PRNG state that produced it (reproducibility)
}
```

A case runs on exactly one engine's oracle and once through `FuzzDDLSignature` with
`isMariaDB = Engine==1`. Batches are `[]Case` of `-batch` (default 1000) cases, all same engine
(each oracle proc serves one engine; sql_mode varies per case inside a batch — the protocol
carries per-case `sql_mode`).

### Pipeline (fuzz)

```
 gen workers (G)        exec workers (W)            oracle client (1 goroutine/proc)
 ┌────────────┐  batch  ┌──────────────────┐ batch  ┌──────────────────────────┐
 │ generator/ │ ───────▶│ FuzzDDLSignature │ ──────▶│ PARSE_BATCH → digests    │
 │ mutators   │  chan   │ (contained)      │  chan  │ (+GET_COVERAGE cadence)  │
 └────────────┘         └──────────────────┘        └──────────────────────────┘
                                                            │ (ourSigs, digests)
                                                     ┌──────▼───────┐
                                                     │ compare +    │──▶ findings/
                                                     │ dedup+corpus │──▶ corpus.db, coverage/
                                                     └──────────────┘
```

Backpressure: bounded channels (`4×nprocs` batches pending toward oracles). Our-side results
travel with the batch (parallel slice `ourSig []string, ourErr []string, ourPanic []bool`).
Comparison runs on the oracle-client goroutine's completion path (cheap; equal-fast-path).

## Implementation steps

### Step 1 — export shim (flow module)

Create `flow/connectors/mysql/ddlfuzz_export.go` exactly as follows (the helper names are
`fuzzDDL*`-prefixed so `go test -tags ddlfuzz` does not collide with the test file's `ddlDiff*`
functions):

```go
//go:build ddlfuzz

package connmysql

import (
	"fmt"
	"strings"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// FuzzDDLSignature parses query with parseQueryEvent and reduces the result to
// the ddlfuzz comparison signature (grammar in tools/ddlfuzz/plan/00-overview.md,
// identical to ddl_parser_tidb_diff_test.go's ddlDiffSignature). err is
// parseQueryEvent's error, with signature "" then. Panics are NOT recovered
// here; the fuzz driver contains them.
func FuzzDDLSignature(query []byte, sqlMode uint64, isMariaDB bool) (string, error) {
	stmts, err := parseQueryEvent(query, sqlMode, isMariaDB)
	if err != nil {
		return "", err
	}
	return fuzzDDLSignature(stmts), nil
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
```

Component 30 (e2e lane) additionally requires two exports in the **same** shim file (both
build-tagged `ddlfuzz`, panics not recovered — callers contain them):

```go
// FuzzSQLModeFromStatusVars extracts the per-event sql_mode from a binlog
// QueryEvent status-vars blob using the same codes-0/1 walker cdc.go uses.
// ok=false when the blob has no Q_SQL_MODE_CODE entry. Thin wrapper over the
// existing unexported sqlModeFromStatusVars.
func FuzzSQLModeFromStatusVars(statusVars []byte) (uint64, bool) {
	return sqlModeFromStatusVars(statusVars)
}

// FuzzParseForE2E parses like FuzzDDLSignature but returns the parsed
// statements serialized as JSON structurally parallel to the oracle digest's
// "stmts" array, per 30-e2e-lane.md §Interfaces (normative for the shape):
// kind/schema/table/specs/pairs/cols, with `precision`/`scale` ints carrying
// ddlColumnDef.Precision/Scale verbatim (-1 = not written) instead of
// params_written, and no "other" entries (benign statements produce nothing,
// matching parseQueryEvent). Spec op mapping: NewColumnName!="" → rename_col;
// NewColumns>0 && OldColumnName!="" → change; NewColumns>0 → add (our AST does
// not distinguish ADD from MODIFY); else drop.
// err mirrors parseQueryEvent; on err the JSON is null.
func FuzzParseForE2E(query []byte, sqlMode uint64, isMariaDB bool) ([]byte, error) {
	stmts, err := parseQueryEvent(query, sqlMode, isMariaDB)
	if err != nil {
		return []byte("null"), err
	}
	return json.Marshal(fuzzDDLStmtsToE2E(stmts))
}
```

`sqlModeFromStatusVars` is confirmed present in `cdc.go` (writeup: "codes 0/1 walker kept").
`fuzzDDLStmtsToE2E` is a private helper in the shim producing the structs of 30's JSON schema;
`type_str` is `ddlColumnDef.TypeStr` verbatim. Add `encoding/json` to the shim imports.

Fidelity guard: add `flow/connectors/mysql/ddlfuzz_export_test.go` with `//go:build ddlfuzz`
asserting `fuzzDDLSignature` ≡ `ddlDiffSignature` and `FuzzDDLSignature` ≡ `ddlDiffOurSig`
semantics over `tidbDiffModeCases` (loop the table, compare outputs; "ERROR" want ⇔ err != nil).
Runs only under `go test -tags ddlfuzz ./connectors/mysql/ -run TestDDLFuzzExportParity`; the
overview's verification gate already requires `go build -tags ddlfuzz ./...` to compile.

### Step 2 — wire protocol client (`internal/wire`)

Little-endian framing per contract. Client owns one process's stdin/stdout.

```go
type Conn struct {
    w    *bufio.Writer // wraps proc stdin, 1 MiB
    r    *bufio.Reader // wraps proc stdout, 1 MiB
    hdr  [4]byte
    body []byte // reused response buffer
}

const (
    MsgParseBatch = 1
    MsgGetCoverage = 2
    MsgHello = 3
)

func (c *Conn) ParseBatch(cases []run.Case) ([][]byte, error) // returns digest_json slices (views into c.body — copy before next call if retained)
func (c *Conn) GetCoverage() ([]byte, error)                  // counters view, valid until next call
func (c *Conn) Hello() (HelloInfo, error)                     // {"engine","server_version","protocol"}
```

Request build: precompute body length, single `Write` of header+body (assemble in a reused
`[]byte`). Response read: `io.ReadFull` header, grow-and-`ReadFull` body. Per-call deadline via
`SetReadDeadline` is unavailable on pipes — use a deadline goroutine in `internal/oracle`
(step 3), not here. `HELLO` validation: `protocol == 1`, `engine` matches expected, record
`server_version`.

### Step 3 — oracle pool (`internal/oracle`)

```go
type Pool struct {
    engine   string   // "mysql" | "mariadb"
    binPath  string   // tools/ddlfuzz/build/oracle-<engine>
    procs    []*proc  // N = -oracle-procs-per-engine (default 5)
    ...
}
type proc struct {
    cmd  *exec.Cmd
    conn *wire.Conn
    covWindow []run.Case // cases since last GET_COVERAGE poll (attribution buffer)
    batchesSincePoll, pollEveryBatches int // adaptive: start 1, double after 16 empty polls, cap 32, reset to 1 on hit
}
```

- **Spawn**: `exec.Command(binPath)`; stderr → per-proc rotating log file
  `state/log/oracle-<engine>-<i>.log` (cap 8 MiB, truncate-on-rotate). On start: `Hello()`
  within 10 s or fail spawn. Respawn with exponential backoff (100 ms → 5 s); >20 consecutive
  spawn failures = fatal (exit 3, supervisor restarts).
- **Submit path** (one goroutine per proc): pull batch from engine channel, `ParseBatch` with a
  watchdog timer of `-oracle-batch-timeout` (default 10 s ≈ 1000×10 ms worst case). On timeout:
  SIGKILL, treat as crash.
- **Crash/timeout bisection**: when `ParseBatch` errors (EOF, timeout, exit), respawn and
  re-submit the in-flight batch in halves recursively (each half a fresh `ParseBatch`):
  - a half that completes → its digests are used normally;
  - recurse into the failing half until a single case crashes/hangs the oracle → finding with
    class `oracle_crash` / `oracle_timeout` (these are findings regardless of verdict — a parse
    oracle must never die);
  - guard: if both halves pass in isolation (state-dependent crash), file the *whole batch* as
    one `oracle_crash` finding with the batch stored under `repro.sql` (newline-`\x00`-separated,
    plus `meta.json.batch=true`) and move on. Cap bisection at 2×log2(batch) resubmissions.
- **Coverage polling**: after every `pollEveryBatches` completed batches, `GetCoverage`; hand the
  bitmap + `covWindow` to `internal/sancov` (step 10). Adaptive cadence as in the struct comment.
- **Backpressure**: engine channel capacity `4×N` batches; producers block.

### Step 4 — digest decoding (`internal/digest`)

```go
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
type Pair struct{ OldSchema, OldTable, NewSchema, NewTable string } // json tags old_schema etc.
type Col struct {
    Name          string `json:"name"`
    TypeStr       string `json:"type_str"`
    NotNull       bool   `json:"not_null"`
    ParamsWritten []int  `json:"params_written"` // nil when JSON null
}
```

Decode with `json.Unmarshal` into pooled `*Digest` (sync.Pool, `Reset()` reslices). Perf note:
stdlib target ≤2 µs/digest for typical ~150-byte digests; if profiling shows this dominating,
replace with a hand-rolled scanner for this fixed schema (fallback, not initial scope).
Unknown digest fields = ignore; malformed digest JSON = protocol error → treat like oracle crash
(bisect isolates the input; class `oracle_crash`, shape `bad_digest`).

### Step 5 — comparison + reconciliation (`internal/compare`)

Two functions:

```go
// OracleSig reduces an accepted digest to the signature grammar, applying the
// multi-statement truncation rule. skip=true means unreachable-by-invariant:
// no signature comparison (no-panic check still applies).
func OracleSig(d *digest.Digest) (sig string, skip bool)

// Diff classifies one completed case. Returns nil when reconciled.
func Diff(c run.Case, ourSig string, ourErr error, ourPanic *exec.PanicInfo, d *digest.Digest) *Divergence
```

`OracleSig` reduction (mirrors the signature grammar exactly):
- Truncate `d.Stmts` at the first `kind=="other"` entry (exclusive). Special case per contract:
  if `len(Stmts) > 1 && Stmts[0].Kind == "other"` → `skip=true` (see Contract issues for the
  ≥1-position refinement rationale).
- `alter_table` → `"alter " + qual(schema,table) + "{" + specs joined "; " + "}"` where each spec:
  - `add`/`modify` → `"col "` + cols; `change` → `"chg " + old_name + " "` + cols;
  - `drop` → `"drop " + old_name`; `rename_col` → `"ren " + old_name + ">" + new_name`;
  - append `" @pos"` when `has_position`.
- col → `name + "=" + qkindOrERR(type_str)`; if qkind is `numeric`, append
  `fmt.Sprintf("(%d,%d)", p, s)` with `p = ParamsWritten[0] if len≥1 else -1`,
  `s = ParamsWritten[1] if len≥2 else -1`; append `" nn"` when `not_null`.
  `qkindOrERR` = `connmysql.QkindFromMysqlColumnType(typeStr, true, 0)`; error → `"ERR"`.
- `rename_table` → `"rename "` + pairs `qual(old)+">"+qual(new)` joined `", "`.
- Statement parts joined `" | "`.

`Diff` decision table (exhaustive):

| # | Oracle verdict | Our result | Outcome |
|---|---|---|---|
| 1 | reject | any return (sig or error), no panic, in deadline | **reconciled** (binlog invariant: rejects are unreachable; our parser MAY error or return anything) |
| 2 | reject | panic / deadline trip | finding, class `panic` / `timeout` |
| 3 | accept, skip=true (multi-stmt, stmts[0] other) | no panic | **reconciled** (unreachable-by-invariant) |
| 4 | accept, skip=true | panic / deadline | finding, class `panic` / `timeout` |
| 5 | accept | panic / deadline | finding, class `panic` / `timeout` |
| 6 | accept | error from parseQueryEvent | finding, class `we_error` (our parser must parse everything the server accepts — no exceptions; if a legitimate one is ever found it is *ledgered*, not special-cased here) |
| 7 | accept | canonical(sig) == canonical(OracleSig) | **reconciled** (canonicalization = R13) |
| 8 | accept | canonical(sig) != canonical(OracleSig) | finding, class `sig_mismatch`, shape from first differing element of the canonical forms (below) |

**Reconciliation rules** (things that must NOT surface as findings; each encodes a known engine
quirk, most learned from `ddl_parser_tidb_diff_test.go`):

R1. **Type-string cosmetics never compared.** Only the qkind survives reduction. Covers:
    `numeric` vs `decimal` canonicalization (writeup: deliberately not canonicalized on our
    side — both are `QValueKindNumeric`); display-width defaults the server records
    (`int(11)`, `year(4)`/`year(-1)`); enum/set element re-escaping/requoting; synonym expansion
    (`INT4`→`int`, `DEC`→`decimal`, `FLOAT8`→`double`, `NCHAR VARYING`→`varchar`,
    `LONG VARBINARY`→`mediumblob`, `MIDDLEINT`→`mediumint`, `CLOB`→`longtext`, …) — both sides
    canonicalize independently; agreement is checked only at the qkind level. Enforced
    structurally: `Diff` never looks at `type_str` except through `QkindFromMysqlColumnType`.

R2. **Numeric (prec,scale): default-aware comparison** (00-overview Reconciliation decision D2).
    Both servers fill type-param defaults during parse (`DECIMAL` ≡ `decimal(10,0)` in
    `Alter_info` on both engines — validated by 10/11), so written-ness is unrecoverable from the
    oracle. Our side keeps `-1` for unwritten params (writeup: "do not substitute display
    defaults" — parser behavior is unchanged). Reconciliation happens in canonicalization (R13):
    on OUR side substitute the decimal defaults for `-1` (`p==-1 → 10`, `s==-1 → 0`); the oracle
    side renders `params_written` verbatim and never parses digits out of `type_str`.
    `DECIMAL` → ours `(-1,-1)`→canonical `(10,0)` vs oracle `(10,0)` ✓; `DECIMAL(12)` → ours
    `(12,-1)`→`(12,0)` vs `[12,0]` ✓; `NUMBER(10)` under ORACLE mode → ours `numeric(10,-1)`→
    `(10,0)` vs oracle decimal `[10,0]` ✓. The substitution is comparison-only (shim output and
    findings meta keep the raw `-1`s). Blind spot accepted: written `DECIMAL(10,0)` vs bare
    `DECIMAL` are indistinguishable — exactly the server's own ambiguity.

R3. **tinyint(1) / BOOL.** Load-bearing: `tinyint(1)` prefix in `type_str` flips the qkind to
    `bool`. Our `BOOLEAN`/`BOOL` synthesizes `tinyint(1)` with Precision=-1 (synthetic, not
    written) — qkind `bool`, and since qkind ≠ numeric no params are rendered, so written-ness of
    the `(1)` is invisible. Oracle must emit `tinyint(1)` in `type_str` for both spellings
    (`TINYINT(1)` and `BOOL`); `params_written` irrelevant here. `TINYINT(1) UNSIGNED` → `bool`
    as well (Qkind checks the `(1)` before unsignedness).

R4. **Integer display widths** don't render (qkind not numeric ⇒ no `(p,s)`), so
    `int(11)` vs `int` agree. Only exception is R3.

R5. **unsigned/zerofill.** Signature-visible only through the qkind (`" unsigned"` suffix
    consumed by Qkind, `zerofill` implies unsigned on both sides). Oracle contract already folds
    zerofill into `" unsigned"`.

R6. **enum/set param text** never compared (R1); `set` → `string`, `enum` → `enum` qkind, params
    never rendered (not numeric).

R7. **ERR-qkind agreement.** Both sides apply the same `QkindFromMysqlColumnType`; a
    server-accepted type unknown to the mapper (MariaDB `uuid`, `inet4/6`, plugin UDTs) reduces
    to `ERR` on both sides and matches. This is how "MariaDB open type slot" cases stay quiet.
    A mismatch like ours=`ERR` vs oracle=`string` is a real finding (type normalization bug).

R8. **Multi-statement truncation** (rule in `OracleSig`): our parser deliberately ignores
    everything after the first benign statement head (writeup, intentional change 7); digests
    keep `other` placeholders. Compare our full signature against the reduction of the digest
    truncated at the first `other`. The contract's stmts[0]-other skip is case 3.

R9. **ALTER-side table rename / benign specs.** `ALTER TABLE t RENAME [TO|AS|=] x`, index/check/
    partition/tablespace/option specs are dropped by our parser and omitted from digest specs —
    both reduce to `alter t{}` (empty spec list still renders the head; classification is still
    compared). No special code needed; listed so nobody "fixes" it.

R10. **Identifiers compared as decoded text, byte-exact** (schema/table/column, as written,
    quoting removed). No case folding: parse-only oracles don't apply `lower_case_table_names`.
    If golden shows an engine folding table names in the parse tree, handle it in the oracle
    (component 10/11), not here.

R11. **sql_mode engine gating** is a *generator* rule, not a compare rule: ORACLE(1<<9) and
    MSSQL(1<<10) bits are only meaningful on MariaDB; the generator never sets them for
    engine=mysql (they alias unrelated/invalid MySQL modes and would make the oracle install a
    sql_mode the real replication stream can't carry).

R12. **Ledger / parked suppression** happens at dedup (step 7), keyed by `<sig>`: a computed
    descriptor whose sig is in `ledger.jsonl` or `parked.list` increments a counter and is
    dropped (no findings dir write). Both files are re-read on SIGHUP and every 60 s (the fix
    agent appends to the ledger while we run).

R13. **Spec canonicalization of BOTH sides before comparison** (00-overview Reconciliation
    decision D1; required by oracle findings 10-C1 and 11-issue-2 — the servers' `Alter_info`
    flattens `ADD (a,b)` to per-column entries, cannot distinguish `MODIFY c` from `CHANGE c c`,
    and loses inter-class spec order). Implementation: parse each signature string into a small
    struct form (`[]stmt{kind, qual, specs []spec{kind, old, cols []col, pos}}` — the grammar is
    regular, ~80 LOC parser in `internal/compare`), canonicalize, re-render, compare strings.
    Canonicalization steps, per `alter` statement:
    1. Flatten every multi-column `col` spec into one `col` spec per column (`chg` specs always
       carry one column already).
    2. Rewrite `chg X <col>` where the column's name equals `X` into the `col` form (the oracles
       already classify `change==field_name` as `modify`, which renders as `col`; this normalizes
       our `CHANGE c c` output to match).
    3. Bucket specs in class order: all `col`/`chg` (preserving relative order — this is
       create_list textual order on the oracle side and source order on ours), then all `ren`,
       then all `drop` (each preserving relative order).
    4. Lift `@pos` to a statement-level flag: strip per-spec `@pos`, append a single ` @pos`
       inside the statement braces iff any spec had it. Rationale: the consumer semantic is
       `hasPositionShiftingDdlChanges`, an OR across specs (cdc.go). Accepted blind spot: both
       sides having position on *different* specs of the same statement compares equal.
    5. Numeric-param default substitution on OUR side only (R2).
    `OracleSig` and our-signature comparison both go through this pass; `golden` and `replay`
    use the identical code path. The raw (pre-canonical) signatures are what meta.json records.

**Mismatch shape** (for class `sig_mismatch`): walk both signatures' parse (split on `" | "`,
then heads/specs/cols) and report the first difference as one of:
`stmt_count`, `stmt_kind` (alter vs rename), `table_qual`, `spec_count`, `spec_kind`
(col/chg/ren/drop), `col_count`, `col_name`, `col_kind(<ours>≠<theirs>)`,
`col_params(<p,s>≠<p,s>)`, `col_notnull`, `position`, `rename_pairs`. Identifier *values* are
excluded from the shape except qkind names and params (which are engine semantics, not
spellings) — `col_name` differences report only the dimension, not the names.

### Divergence descriptor and `<sig>`

Canonical descriptor = the JSON serialization (sorted keys, no whitespace) of:

```json
{
  "v": 1,
  "engine": "mysql",
  "sql_mode": 4,                       // masked to ANSI_QUOTES|ORACLE|MSSQL|NO_BACKSLASH_ESCAPES
  "class": "sig_mismatch",             // any value from the D4 finding-class vocabulary:
                                       // fast-lane {sig_mismatch,we_error,panic,timeout,
                                       // oracle_crash,oracle_timeout}, e2e-* classes (lane
                                       // "e2e"), or oracle-reject-live-accept
  "lane": "fast",                      // "fast" | "e2e" — part of identity so a fast and an
                                       // e2e finding of the same shape dedup separately
  "shape": "col_kind(int32≠int16)"     // per class, see below; for e2e classes 30 supplies it
}
```

`shape` per class:
- `sig_mismatch`: the first-difference dimension string above.
- `we_error`: our error text normalized — apply, in order,
  `regexp(\x60at byte \d+\x60) → "at byte ?"`, `regexp(\x60"(?s)[^"]*"\x60) → "\"?\""` (kills
  identifier/token spellings in `%q` verbs), collapse runs of whitespace.
- `panic`: first line of the panic value with the same normalizations + hex addresses
  (`0x[0-9a-f]+ → 0x?`) removed, then `"@" + frame` where frame is the first
  `connmysql.`-package function on the stack (function name only).
- `timeout`: `"head=" + first word of the statement uppercased` (or `""`).
- `oracle_crash` / `oracle_timeout`: `"head=" + first word` (crash detail lives in the oracle
  log, not the descriptor — exit signals aren't stable).

`<sig>` = first 12 hex chars of `sha256(descriptor)`. Stability: engine and masked mode are part
of intent; identifiers, byte offsets, addresses, and incidental statement text are normalized
out, so re-discovery through different random inputs maps to the same sig. Note sql_mode masking
means the same bug found under `0` and under `ANSI_QUOTES` yields two sigs — intended: mode bits
change lexing and thus are part of the bug identity.

`findings/<sig>/meta.json` (contract fields + extras):

```json
{"sig":"...","engine":"mysql","sql_mode":4,"lane":"fast",
 "our_sig":"alter t{col c=int32}","our_error":"","oracle_digest":{...},
 "status":"open","discovered_at":"2026-07-03T12:00:00Z","minimized":false,
 "class":"sig_mismatch","shape":"col_kind(int32≠int16)","descriptor_v":1,
 "origin":"gen","seed":"0x1234abcd","server_version":"9.7.0","times_seen":3}
```

Writes: `repro.sql` (exact input bytes) + `meta.json` via temp-file + rename; existing sig →
increment `times_seen` only (never overwrite repro of an open finding; if `status` is `fixed`
and it re-diverges, that is replay's job to catch, but fuzz also flips it back to `open` and
updates `repro.sql`, logging loudly).

### Step 6 — panic/hang containment (`internal/exec`)

```go
type Worker struct {
    id      int
    curSeq  atomic.Uint64 // bumped before each case
    curDead atomic.Int64  // deadline unixnano for current case
}
func (w *Worker) RunBatch(b []run.Case) []Result // Result{Sig string; Err error; Panic *PanicInfo}
```

- Each case executes inside `func() (r Result) { defer func(){ if p := recover(); p != nil {
  r.Panic = &PanicInfo{Value: fmt.Sprint(p), Stack: debug.Stack()} } }(); sig, err :=
  connmysql.FuzzDDLSignature(c.SQL, c.SQLMode, c.Engine==1); ... }()`.
- **Watchdog** goroutine ticks every 25 ms, scans all workers: if a worker's `curSeq` hasn't
  advanced and `now > curDead` (deadline = start + `-case-deadline`, default 100 ms), the case is
  declared hung: file finding class `timeout` (input recorded from the worker's published case
  pointer), mark the goroutine abandoned, and start a replacement worker. Abandoned goroutines
  cannot be killed; when abandoned-count exceeds 8 or process RSS exceeds `-mem-ceiling`
  (default 4 GiB, checked via `runtime.MemStats.Sys` + OS RSS each watchdog tick), flush state
  and `os.Exit(3)` — the supervisor restarts us; findings already on disk.
- `debug.SetMemoryLimit(ceiling*3/4)` at startup (soft backstop for allocation-heavy inputs);
  a single case OOMing the process before the watchdog reacts is caught by the supervisor
  restart + preflight replay (the batch is journaled: see state flush, step 11).
- Deadline is generous (100 ms ≈ 10⁵× median) so false positives are effectively impossible;
  watchdog resolution means actual containment latency ≤125 ms.

### Step 7 — dedup and findings (`internal/findings`)

- In-memory `map[sig]*findingState` loaded from `state/findings/*/meta.json` at startup.
- `parked.list` + `ledger.jsonl` loaded at startup, re-read every 60 s and on SIGHUP; suppressed
  sigs counted in stats (`suppressed=N`).
- New sig → write finding (rate limit: max 200 open findings; beyond that, log + count only —
  protects the 72h run from a divergence-storm filling the disk; supervisor sees the counter).
- Every finding also appends one line to `state/findings/index.jsonl` (sig, class, ts) for cheap
  supervisor tailing. (Additive to the state contract; 40 may read but need not.)

**Finding-class vocabulary** (closed set per 00-overview Reconciliation decision D4; the `class`
component of the divergence descriptor MUST be one of these — shared with 30/40):

```
fast lane:  sig_mismatch     our accepted signature ≠ oracle-derived signature (canonical forms)
            we_error         oracle accepted, parseQueryEvent returned an error
            panic            FuzzDDLSignature panicked (any input, any verdict)
            timeout          per-case deadline tripped (our watchdog)
            oracle_crash     oracle process died on a case (isolated by bisection)
            oracle_timeout   oracle batch/case exceeded batch-timeout
e2e lane:   e2e-statusvar-walk, e2e-sqlmode-mismatch, e2e-plumbing-sig, e2e-panic,
            e2e-query-rewrite, e2e-parse-error-live-accept, e2e-missed-column-effect,
            e2e-col-attr, e2e-position-missed,
            e2e-missing-event, e2e-unexpected-event, e2e-exec-reject-applied
                                                   (defined in 30-e2e-lane.md §4, §6-§7)
cross:      oracle-reject-live-accept              (filed by `replay --from`, below)
```

The descriptor and `findings.Record` accept exactly this set; anything else is a programming
error (panic in dev, reject+log in prod).

### Step 8 — keyword dictionary from lexer symbol tables (`internal/dict`)

Allowed black-box exception (overview constraint 2): extract the token *bag* only.

Sources (verified present, read-only):
- MySQL: `~/Code/mysql-server/sql/lex.h`, `static const SYMBOL symbols[]` at line 61. Entries are
  `{SYM("NAME", TOKEN)}` — the first macro arg is the literal keyword/operator string.
- MariaDB: `~/Code/mariadb-server/sql/lex.h`, `SYMBOL symbols[]` at line 49. Entries are
  `{ "NAME", SYM(TOKEN)}` — first field is the string. (MariaDB also has `sql_functions[]` in the
  same file for function names; include it too — function-shaped tokens are useful mutation
  fodder in DEFAULT/GENERATED expressions.)

Extraction script `internal/dict/extract.sh` (checked in; run manually, output committed — the
72h run never touches the server trees):

```sh
#!/bin/sh
# Usage: extract.sh <mysql-src> <maria-src>  (writes internal/dict/{mysql,mariadb}.txt)
set -eu
mysql_src=$1 maria_src=$2 out=$(dirname "$0")
# grab the first double-quoted string on each symbols[] line
awk '/SYM\("/{ if (match($0,/"[^"]+"/)) print substr($0,RSTART+1,RLENGTH-2) }' \
  "$mysql_src/sql/lex.h" | LC_ALL=C sort -u > "$out/mysql.txt"
awk '/^[[:space:]]*\{[[:space:]]*"/{ if (match($0,/"[^"]+"/)) print substr($0,RSTART+1,RLENGTH-2) }' \
  "$maria_src/sql/lex.h" | LC_ALL=C sort -u > "$out/mariadb.txt"
```

Both `.txt` files are committed. `internal/dict` embeds them via `//go:embed mysql.txt
mariadb.txt` and exposes `Tokens(engine) []string`. No yacc, no production structure — just the
strings. A `dict_test.go` asserts the files are non-empty (>500 mysql, >600 maria tokens
expected) and ASCII-only.

### Step 9 — seed corpus extraction (`internal/seed`)

Mechanism: **parse the `_test.go` files with `go/ast`**, not runtime reflection (no live DB, no
building the connmysql test binary). One-shot generator `internal/seed/gen.go` run via
`go generate` (committed output), producing `tools/ddlfuzz/seeds/seeds.jsonl` — one
`{sql, engine, sql_mode

Extract from `flow/connectors/mysql/`:
- `ddl_parser_tidb_diff_test.go`: `tidbDiffCorpus` (`.sql`,`.engine`), `tidbDiffModeCases`
  (`.sql`,`.sqlMode`,`.isMariaDB`), and the keys of `tidbDiffOverrides`/`tidbDiffFailWant`.
- `ddl_parser_test.go`, `ddl_parser_alter_test.go`, `ddl_parser_types_test.go`,
  `ddl_parser_classify_test.go`, `ddl_lexer_test.go`: pull every string literal assigned to a
  struct field named `sql`, `query`, `input`, or `src` (walk composite literals; resolve simple
  string concatenations `"a"+"b"` via `go/constant`). This is a superset heuristic — over-
  inclusion is harmless (extra seeds).

Mapping to `Case`: `engine=="mysql"`→{mysql}, `"mariadb"`→{maria}, `"both"`/absent→both engines.
`sql_mode` from the case if present, else derive from `note`/context is unreliable — default 0
and let the mode matrix (step 10 generator) cover modes; but for `tidbDiffModeCases` carry the
literal `sqlMode`/`isMariaDB` exactly (they are mode-load-bearing). Decode Go escape sequences
(`\x00`, `ë`, `\r`, `\n`) so bytes match what the parser sees — use `strconv.Unquote` on the
original literal text.

Expected yield ≈700 statements (≈230 tidb-diff corpus entries + ≈480 unit-table rows across the
five `ddl_*_test.go` files; 00-overview Reconciliation decision D11). `seed_test.go` asserts ≥600
seeds and that every
`tidbDiffModeCases` entry is present with its exact mode/engine. The `seeds/` dir also carries
per-fix `<sig>.sql`+`<sig>.meta.json` pairs (fix agent, 40-C7), loaded alongside `seeds.jsonl`
(Reconciliation decision D8).

At `fuzz`/`golden` startup, `seeds.jsonl` is loaded into the corpus store (step 10) and marked
`origin=corpus`; the committed dir means a fresh checkout can fuzz with zero prior state.

### Step 10 — coverage-driven corpus retention (`internal/sancov`, `internal/corpus`)

Per the Scope note, corpus growth is driven **only** by oracle SanCov coverage. There is no
`internal/gocov`, no covcounters parsing, no epoch re-attribution, and no live distillation cron.

**Oracle side (the one retention signal):** `GET_COVERAGE` returns cumulative inline-8bit
counters. `internal/sancov` keeps an OR-accumulated `[]byte` per engine
(`state/coverage/{mysql,mariadb}.sancov`). After each poll, `grew := any i where counters[i]!=0 &&
accum[i]==0`; on growth, retain a bounded sample of that poll's `covWindow` (at most
`-retain-per-poll`, default 256, smallest inputs first — attribution is window-level guesswork, and
unbounded whole-window retention measured 354k entries/2.7 GB in 90s and throttled the loop ~20×),
then `accum |= counters`. The store additionally enforces `-corpus-budget` (default 40 GiB) against
the SQLite corpus file set (`corpus.db` plus SQLite sidecars): over budget, `Add` refuses,
`retention_skipped` counts it in stats — a hard disk ceiling; coverage accumulation and findings
are unaffected.
`edges.{mysql,mariadb}` in `stats.json` = popcount of each `accum`.

**Corpus store** (`internal/corpus`): SQLite database at `state/corpus.db`, table `corpus`
containing `engine`, `hash`, `sql_mode`, `origin`, `added_at`, and `sql`. `sha1 =
sha1(sql || 0x00 || sqlMode-le)` (mode part of identity because lexing depends on it). Dedup by
unique `(engine, hash)`. The store exposes count, size, and random SQL sampling APIs so callers do
not depend on the physical storage format.

**Distillation: one optional pass at graceful shutdown**, not a cron. To produce committable
seeds, sort the retained corpus by size ascending and greedily keep an input only if it adds an
oracle SanCov index not already covered by the kept set (re-poll a fresh oracle proc per
candidate; a few minutes for a corpus of thousands). Skippable — findings and per-fix seeds are
the durable artifacts; distillation only tidies the corpus for the exit-criteria report.

**Optional Go-coverage report (post-hoc, off the run path):** for a human-facing sanity check on
how much of `ddl_lexer.go`/`ddl_parser.go` the campaign exercised, the binary *may* be built with
`go build -tags ddlfuzz -cover -coverpkg=github.com/PeerDB-io/peerdb/flow/connectors/mysql` and
dump a single cumulative profile via `runtime/coverage.WriteCountersDir` at shutdown, rendered
with `go tool covdata`. This is a report, not a retention input; the run never reads it back, and
`edges.go` in `stats.json` is 0/absent when the binary is built without `-cover`.

### Step 11 — orchestrator, stats, state flush (`internal/run`)

- **PRNG**: each generator/mutator worker gets a `math/rand/v2` `*rand.Rand` seeded
  `NewPCG(globalSeed, workerID)`. `globalSeed` from `-seed` flag or `time.Now().UnixNano()` at
  first start, then persisted to `state/seed` and reloaded on restart (deterministic resume).
  Each emitted `Case.Seed` records the PCG state before generation so a single case is
  reproducible via `replay --seed`.
- **Stats line** (stderr, every `-stats-interval` default 5 s):
  `execs/s=NNN corpus=mysql:A,maria:B edges=go:E,my:F,ma:G findings=open:O,supp:S`
  `abandoned=X procs=my:5/5,ma:5/5 uptime=Th`. `execs/s` is EWMA over the interval.
- **`state/stats.json`** written atomically every **30 s** — schema per 40's C6 (Reconciliation
  decision D12): `{ts (RFC3339), execs_total, execs_per_sec, corpus_count:{mysql,mariadb},
  edges:{go,mysql,mariadb}, oracle_restarts:{mysql,mariadb}, findings_emitted_total}` plus
  additive extras `run_seed`, `class_counts`, `suppressed`. This is the machine-readable
  heartbeat 40 polls (staleness >5 min ⇒ 40 treats the fuzzer as wedged).
- **State flush cadence**: `{mysql,mariadb}.sancov` every 30 s and on shutdown; corpus files
  written immediately on retention (already durable); findings written immediately. A tiny
  **journal** `state/inflight.jsonl` records batches handed to exec workers but not yet compared
  (append on dispatch, truncate-compact every 30 s to only still-inflight); on restart, preflight
  (40) already replays findings, and we re-load and re-run journaled inflight cases first so an
  input that crashed us mid-batch is re-encountered and filed.
- **Graceful shutdown**: on SIGTERM/SIGINT, stop generators, drain exec+oracle channels with a
  hard **60 s** budget (coordinator requirement), flush all state atomically (temp+rename for
  every file), then exit 0. Exceeding 60 s → flush-what-we-have and exit 0 anyway (never hang the
  supervisor). All state files use temp+rename so a mid-write kill never corrupts them.

### Step 12 — generator (`internal/gen`)

Weighted grammar generator. ~95% of cases have an **actionable head** (ALTER TABLE / RENAME
TABLE(S) / SET STATEMENT-wrapped actionable); ~5% benign heads (to keep classification honest and
exercise the "ignore rest" path). Per case, independently sample:
- engine ∈ {mysql, maria} (50/50, tunable `-engine-bias`);
- sql_mode from the matrix `{0, ANSI_QUOTES, NO_BACKSLASH_ESCAPES, ORACLE, MSSQL}` plus pairwise
  combos of the byte-safe-compatible ones; ORACLE/MSSQL only when engine==maria (R11). Weighted
  toward 0 (~50%) since most prod traffic is mode 0.

Grammar tables (Go slices of weighted alternatives, sampled recursively with a depth budget of 6
to bound size). All emission is **byte-safe**: identifiers/strings emit ASCII plus a curated set
of whole valid UTF-8 sequences (`ë`, `ö`, `ñ`, `世`) — never lone bytes ≥0x80 (overview charset
scope). Every construct in the "Generator construct checklist" below is a table entry.

Structure:
- `head()` → ALTER-TABLE head (with optional `ONLINE`/`IGNORE` runs, `IF EXISTS`, MariaDB
  `WAIT n`/`NOWAIT`), RENAME TABLE/TABLES head, SET STATEMENT wrapper, or a benign head.
- `alterSpecList()` → 1–5 specs joined by `,`, each from `alterSpec()`; ~70% column-relevant
  (ADD/MODIFY/CHANGE/DROP/RENAME COLUMN, ADD multi-col paren list), ~30% benign
  (index/constraint/partition/option/ALGORITHM/LOCK/CONVERT/RENAME-table-form/etc — full
  checklist). Deliberately emit the LL(2) ambiguity forms often (`ADD period ...`,
  `ADD VECTOR ...`, `ADD SYSTEM ...`, `ADD PERIOD FOR`).
- `columnDef()` → `type()` + 0–4 attributes from `attr()`.
- `type()` → from the type table (all synonyms, MariaDB UDT/Oracle-mode names, spatial family,
  params sometimes present/absent/out-of-range).
- identifier zoo, lexical zoo (comments/strings) injected as wrappers and as ident/value choices.
- trailing `;`-chains (0–3 statements), each independently generated.

Output assembled into a byte slice; NUL bytes occasionally appended/embedded (trailing-NUL and
mid-string NUL cases). ~1% of cases are pure lexical-zoo prefixes on an actionable body
(comment/whitespace storms).

### Step 13 — mutators (`internal/mutate`)

Input: one or two corpus entries (same engine). Operators (weighted, 1–3 applied per case):
- **token splice/swap/dup/delete**: tokenize with a *copy* of `connmysql`'s lexer is not
  available across the module boundary, so use a lightweight local tokenizer (whitespace/quote/
  paren-aware, good enough for mutation) to pick token boundaries; splice/swap/dup/delete one.
- **dictionary substitution**: replace a token with a random token from `internal/dict` for the
  case's engine (keyword bag) — surfaces keyword-shaped-name and unexpected-keyword paths.
- **quote/comment flips**: wrap a region in `/*! */`, `/*M! */`, `/*!NNNNN */`, `-- \n`, `#\n`,
  `$tag$…$tag$`, `[…]`, backticks, or `"…"`; flip a `'` region delimiter; double a closing quote.
- **crossover**: take head from entry A, spec list from entry B.
- **bounded byte-level**: flip/insert/delete ≤4 bytes, but only ASCII printables and whole UTF-8
  sequences from the curated set; never produce a lone continuation byte (byte-safety invariant
  is asserted post-mutation — if violated, drop the byte op and retry).
- **sql_mode mutation**: toggle a mode bit (respecting engine gating).

Mutated cases carry `origin=mut`. Corpus-vs-generator mix controlled by `-mut-ratio` (default:
60% mutation once corpus is warm, 40% fresh generation; 100% generation until corpus ≥ seeds).

### Step 14 — `golden` subcommand (`internal/golden`)

Replays the seed corpus (step 9 extraction; same source of truth) through both oracles and
`FuzzDDLSignature`, applying the step-5 comparison rules. For each seed:
- run on its declared engine(s) and mode;
- expectations mapping: a seed whose SQL is a key of `tidbDiffFailWant`/`tidbDiffOverrides` in the
  test file carries that annotation into the seed record as `expect_sig`; golden then also checks
  that **our** signature equals the annotated value (these encode known engine truth where TiDB
  diverged, and must hold). For non-annotated seeds, the check is purely our-sig == oracle-sig
  under the reconciliation rules.
- `ERROR` expectations (`tidbDiffModeCases` want=="ERROR", and the two ANSI/MSSQL fail-want
  entries): the annotation predicts our parser errors. If the oracle *accepts* such a statement,
  that is a **triage item, not an automatic oracle failure** — it's one of (a) an oracle-harness
  bug (10/11), (b) a stale test annotation written against TiDB's stricter behavior that the real
  server actually accepts (00-overview §"golden validation before fuzzing" explicitly permits
  "documented expectation fix"), or (c) a genuine we-error divergence. Golden reports it as an
  irreconcilable case with the three candidate dispositions; the human/agent resolving golden
  picks one (fix oracle / update the annotation with a server-source citation / file as a real
  finding). Golden does not pre-judge it as an oracle bug.
- Report every irreconcilable case to stdout as JSON lines `{sql, engine, sql_mode, our, oracle,
  expect, class}` and a summary. **Exit 0 iff zero irreconcilable cases**, else exit 1. 40's gate
  runs `ddlfuzz golden` and blocks fuzzing on nonzero exit.

### Step 15 — `replay` and `minimize`

**`replay`** two modes:
- `ddlfuzz replay` (no arg): re-execute every `state/findings/*/repro.sql` against a fresh oracle
  of its engine + `FuzzDDLSignature`, recompute the descriptor. Contract for 40's preflight:
  `fixed` findings MUST now reconcile; `ledgered`/`parked` MAY diverge; `open` may diverge.
  Anything that should pass but diverges (a `fixed` that re-diverges, or an unreadable finding)
  blocks. Exit codes (coordinator contract):
  - **0** — all `fixed` reconcile, no unexpected state (preflight OK to proceed);
  - **10** — a `fixed` finding re-diverged (regression) — 40 must not restart into fuzzing;
  - **11** — a finding dir is malformed/unreadable or its engine oracle won't start;
  - **1** — internal error (bad args, oracle build missing, etc).
  Emits a JSON summary on stdout: `{checked, fixed_ok, fixed_regressed:[sig...],
  malformed:[sig...], open:N, ledgered:N, parked:N}`.
- `ddlfuzz replay <sig>` (single-finding, used for reproduction/CI): replay just that finding;
  stdout JSON `{sig, engine, sql_mode, our_sig, our_error, oracle_digest, reconciled:bool,
  class, shape}`. Exit **0** reconciled, **10** still diverges (`class` in the JSON covers
  panic/timeout too — a panic is a divergence, not an 11), **11** finding not found/malformed or
  its engine oracle won't start, **1** internal error. (Exit-code semantics per 00-overview
  Reconciliation decision D5; 40's gate and the fix-agent prompt rely on them.)
- `ddlfuzz replay --from <jsonl> --expect-accept` (oracle cross-check, per Reconciliation
  decision D7; invoked ~6-hourly by 40 on a rotated copy of `state/e2e-live-accepted.jsonl`):
  each line is `{engine, sql_mode, statement}`; send each to its engine's oracle; a **reject**
  verdict files a finding class `oracle-reject-live-accept` (lane `"e2e"`, meta carries the
  oracle error and the source line) — a statement a live server executed but the parse oracle
  rejects is an oracle-harness bug. Exit **0** all accepted, **10** ≥1 finding filed, **1**
  internal error. The input file is owned by the caller (40 rotates before invoking, deletes
  after a clean run).

**`minimize`** (`ddlfuzz minimize <sig>`): delta-debugging (ddmin) with a
divergence-preserving predicate: `pred(bytes) == (descriptor(run(bytes)).sig == targetSig)` —
re-runs the candidate through both the oracle (fresh proc) and `FuzzDDLSignature` and recomputes
the full descriptor; the shrink is accepted only when the sig is byte-identical to the target
(so we never "minimize" into a different bug). Stages, each bounded by `-minimize-budget`
(default 30 s total): (1) line granularity (split on `\n`), (2) token granularity (local
tokenizer boundaries), (3) char/byte granularity (respecting byte-safety — never split a UTF-8
sequence). On success rewrite `repro.sql`, set `meta.json.minimized=true`. Idempotent; safe to
re-run. `minimize` is invoked by the fuzz loop opportunistically (one background minimize at a
time for newly-filed non-trivial findings) and by 40.

### CLI flags (all subcommands share a root flagset where sensible)

```
-state <dir>               default ./state
-mysql-oracle <path>       default build/oracle-mysql
-maria-oracle <path>       default build/oracle-mariadb
-oracle-procs-per-engine N default 5
-oracle-batch-timeout d    default 10s
-batch N                   default 1000
-exec-workers N            default = GOMAXPROCS-2
-gen-workers N             default 2
-case-deadline d           default 100ms
-mem-ceiling bytes         default 4GiB
-seed uint                 default 0 => time-based, persisted
-seeds <dir>               default ./seeds  (committed seed corpus)
-engine-bias f             default 0.5 (P(mysql))
-mut-ratio f               default 0.6
-stats-interval d          default 5s
-minimize-budget d         default 30s
-max-open-findings N       default 200
-duration d                default 0 (until signal)   [fuzz]
```

Subcommands: `ddlfuzz fuzz|golden|replay|minimize [args]`. `-h` prints per-subcommand help.

## Generator construct checklist (public-doc enumeration)

Every item below is a generator table entry, enumerated from dev.mysql.com (MySQL 8.4 ALTER TABLE
+ CREATE TABLE + Data Types) and mariadb.com/kb (ALTER TABLE, Data Types index, SET STATEMENT).
The generator must be able to emit each; the checklist is also the construct-coverage target
reported as a final-report metric ("public-doc construct checklist coverage", 00-overview §Exit) —
informational, not a pass/fail gate.

### ALTER TABLE heads
- `ALTER TABLE t …`
- `ALTER ONLINE TABLE t …` (MariaDB)
- `ALTER IGNORE TABLE t …`
- `ALTER ONLINE IGNORE TABLE t …` (MariaDB)
- `ALTER TABLE IF EXISTS t …` (MariaDB)
- `ALTER TABLE t WAIT 5 …` / `ALTER TABLE t NOWAIT …` (MariaDB)
- schema-qualified `ALTER TABLE db.t …`

### ALTER specs — column-relevant (parsed)
- `ADD [COLUMN] col type [FIRST|AFTER col]`
- `ADD [COLUMN] IF NOT EXISTS col type` (MariaDB)
- `ADD [COLUMN] (col type, col2 type, …)` (multi-column paren list, mixed with index elements)
- `MODIFY [COLUMN] [IF EXISTS] col type [FIRST|AFTER col]`
- `CHANGE [COLUMN] [IF EXISTS] old new type [FIRST|AFTER col]`
- `DROP [COLUMN] [IF EXISTS] col [RESTRICT|CASCADE]`
- `RENAME COLUMN [IF EXISTS] old TO new`

### ALTER specs — benign (consumed, dropped)
- `ADD {INDEX|KEY} [IF NOT EXISTS] [name] [type] (cols) [index_option…]`
- `ADD {FULLTEXT|SPATIAL} [INDEX|KEY] [name] (cols)`
- `ADD VECTOR [INDEX|KEY] [name] (cols)` (MariaDB)
- `ADD [CONSTRAINT [sym]] PRIMARY KEY [type] (cols)`
- `ADD [CONSTRAINT [sym]] UNIQUE [INDEX|KEY] [name] [type] (cols)`
- `ADD [CONSTRAINT [sym]] FOREIGN KEY [name] (cols) REFERENCES …`
- `ADD [CONSTRAINT [sym]] CHECK (expr) [[NOT] ENFORCED]`
- `ADD PERIOD FOR SYSTEM_TIME (s,e)` / `ADD PERIOD FOR p (s,e)` (MariaDB)
- `ADD SYSTEM VERSIONING` / `DROP SYSTEM VERSIONING` (MariaDB)
- `ALTER [COLUMN] col SET DEFAULT literal|(expr)` / `DROP DEFAULT`
- `ALTER [COLUMN] col SET {VISIBLE|INVISIBLE}`
- `ALTER INDEX idx {VISIBLE|INVISIBLE}` / `ALTER {INDEX|KEY} [IF EXISTS] idx [NOT] IGNORED` (Maria)
- `ALTER {CHECK|CONSTRAINT} sym [NOT] ENFORCED`
- `DROP {CHECK|CONSTRAINT} sym` / `DROP CONSTRAINT [IF EXISTS] name` (Maria)
- `DROP {INDEX|KEY} [IF EXISTS] idx` / `DROP PRIMARY KEY` / `DROP FOREIGN KEY [IF EXISTS] sym`
- `DISABLE KEYS` / `ENABLE KEYS`
- `RENAME [TO|AS] newtbl` / `RENAME = newtbl` (opt_to bare `=`)
- `RENAME {INDEX|KEY} old TO new`
- `ORDER BY col [, col…]`
- `CONVERT TO CHARACTER SET cs [COLLATE c]` / `CONVERT TO CHARACTER SET DEFAULT`
- `[DEFAULT] CHARACTER SET [=] cs` / `[DEFAULT] COLLATE [=] c`
- `{DISCARD|IMPORT} TABLESPACE`
- `ALGORITHM [=] {DEFAULT|INSTANT|INPLACE|NOCOPY|COPY}`
- `LOCK [=] {DEFAULT|NONE|SHARED|EXCLUSIVE}`
- `FORCE`
- `{WITH|WITHOUT} VALIDATION` (alter_commands_modifier)
- `SECONDARY_LOAD` / `SECONDARY_UNLOAD`
- table_option run (ENGINE=, AUTO_INCREMENT=, ROW_FORMAT=, COMMENT=, COMPRESSION=,
  ENGINE_ATTRIBUTE=, KEY_BLOCK_SIZE=, STATS_*=, TABLESPACE …, UNION=(…), etc.)
- partition_options: `ADD PARTITION (…)`, `DROP PARTITION p`, `TRUNCATE/ANALYZE/CHECK/OPTIMIZE/
  REBUILD/REPAIR/COALESCE/REORGANIZE PARTITION …`, `EXCHANGE PARTITION p WITH TABLE t [WITH|
  WITHOUT VALIDATION]`, `REMOVE PARTITIONING`, `DISCARD/IMPORT PARTITION … TABLESPACE`,
  `CONVERT TABLE t TO partition_definition` / `CONVERT PARTITION p TO TABLE t` (MariaDB),
  `PARTITION BY RANGE/LIST/HASH/KEY (…) (PARTITION … VALUES …)`

### RENAME TABLE
- `RENAME TABLE a TO b`
- `RENAME TABLES a TO b` (undocumented-but-valid on both engines)
- multi-pair `RENAME TABLE a TO b, c TO d, …`
- schema-qualified pairs `RENAME TABLE db.a TO db.b`
- per-pair `WAIT n`/`NOWAIT` (MariaDB): `RENAME TABLE a WAIT 5 TO b, c NOWAIT TO d`

### Data types (type table)
- integers: `TINYINT`,`SMALLINT`,`MEDIUMINT`,`INT`,`INTEGER`,`BIGINT` + synonyms
  `INT1`,`INT2`,`INT3`,`INT4`,`INT8`,`MIDDLEINT`; optional `(M)` display width; `UNSIGNED`/
  `SIGNED`/`ZEROFILL`.
- fixed-point: `DECIMAL`,`NUMERIC`,`DEC`,`FIXED` with `()`,`(M)`,`(M,D)`, `UNSIGNED`.
- float: `FLOAT`,`FLOAT4`,`FLOAT8`,`REAL`,`DOUBLE`,`DOUBLE PRECISION`, `(M,D)`.
- bit/bool: `BIT[(M)]`, `BOOL`, `BOOLEAN`.
- serial: `SERIAL`.
- date/time: `DATE`,`TIME[(f)]`,`DATETIME[(f)]`,`TIMESTAMP[(f)]`,`YEAR[(4)]`.
- char/text: `CHAR[(n)]`,`VARCHAR(n)`,`CHARACTER`,`CHARACTER VARYING`,`VARCHARACTER`,
  `NCHAR`,`NVARCHAR`,`NATIONAL CHAR`,`NATIONAL CHAR VARYING`,`NATIONAL VARCHAR`,
  `NCHAR VARYING`,`CHAR VARYING`, `TINYTEXT`,`TEXT[(n)]`,`MEDIUMTEXT`,`LONGTEXT`,
  `LONG`,`LONG VARCHAR`, `CLOB` (MariaDB).
- binary/blob: `BINARY[(n)]`,`VARBINARY(n)`,`TINYBLOB`,`BLOB[(n)]`,`MEDIUMBLOB`,`LONGBLOB`,
  `LONG VARBINARY`; `CHAR(n) BYTE` (MariaDB→binary); `CHARACTER SET binary`/`CHARSET binary`
  remap.
- enum/set: `ENUM('a','b',…)`, `SET('x','y',…)` (with commas, quotes, `*/`, `FOR` inside).
- json: `JSON`.
- spatial: `GEOMETRY`,`POINT`,`LINESTRING`,`POLYGON`,`MULTIPOINT`,`MULTILINESTRING`,
  `MULTIPOLYGON`,`GEOMETRYCOLLECTION`,`GEOMCOLLECTION`; `… REF_SYSTEM_ID=n` (MariaDB).
- vector: `VECTOR(n)`.
- MariaDB UDT/plugin (open slot): `UUID`,`INET4`,`INET6` (unknown-to-mapper ⇒ ERR both sides).
- Oracle mode (MariaDB + sql_mode ORACLE): `NUMBER`,`NUMBER(p)`,`NUMBER(p,s)`,`VARCHAR2(n)`,
  `RAW(n)`,`CLOB`, lengthless `BLOB`→longblob; schema-qualified type names
  `mariadb_schema.DATE`,`oracle_schema.DATE`,`maxdb_schema.TIMESTAMP`.

### Column attributes
- `NOT NULL` / `NULL` / `NOT NULL ENABLE` (Oracle-ism)
- `DEFAULT literal` / `DEFAULT (expr)` / `DEFAULT CURRENT_TIMESTAMP[(f)]` /
  `ON UPDATE CURRENT_TIMESTAMP[(f)]` / `DEFAULT NULL` / charset-introducer defaults
  (`_utf8mb4 X'…'`, `N'…'`, `b'…'`, `0x…`, `0b…`, `.5`, `1e5`, `1.e5`, `-1.5e-10`)
- `AUTO_INCREMENT`, `UNIQUE [KEY]`, `[PRIMARY] KEY`
- `COMMENT 'str'` (with commas, `*/`, `--` inside), `$$…$$` (MySQL 9)
- `COLLATE c`, `CHARACTER SET cs`/`CHARSET cs`
- `COLUMN_FORMAT {FIXED|DYNAMIC|DEFAULT}`, `STORAGE {DISK|MEMORY}`
- `ENGINE_ATTRIBUTE='json'`, `SECONDARY_ENGINE_ATTRIBUTE='json'`
- `VISIBLE`/`INVISIBLE`
- `GENERATED ALWAYS AS (expr) {VIRTUAL|STORED|PERSISTENT}` (PERSISTENT = MariaDB), `AS (expr)`
- `CHECK (expr) [NOT ENFORCED]`
- column `REFERENCES other(col) [MATCH FULL] [ON DELETE …] [ON UPDATE …]`
- `SERIAL DEFAULT VALUE`
- `COMPRESSED` / `COMPRESSED=zlib` (MariaDB)
- `REF_SYSTEM_ID=n` (MariaDB spatial)
- `WITH SYSTEM VERSIONING` / period column markers

### SET STATEMENT wrapping (MariaDB)
- `SET STATEMENT var=value FOR <actionable>`
- multiple vars `SET STATEMENT a=1, b=2 FOR …`
- value is a full expression: `sql_mode=CONCAT(@@sql_mode,',ANSI_QUOTES')`, `a=CAST(1 AS CHAR)`,
  string value containing `FOR` (`lc_messages='en FOR us'`) — FOR found quote/paren-aware at
  depth 0.

### Identifier zoo
- bare, backtick-quoted (with doubled `` `` ``), ANSI double-quoted (mode-gated, doubled `"`),
  MSSQL bracket (MariaDB mode-gated, `[...]`)
- keyword-shaped names: `first`,`after`,`period`,`system`,`vector`,`column`,`table`,`select`,
  `set`,`add`,`key` (quoted and bare where legal)
- schema-qualified `db.t`, all-digit-after-dot `db.1234`, digit-leading `1ea10`
- UTF-8 identifiers (`tëst`, `cölumn_ñ`, `世界`) as whole valid sequences

### Lexical zoo
- comments: `/* */`, `/*+ hint */`, `/*! code */`, `/*!NNNNN code */` (5/6 digit + <5),
  `/*M! code */` / `/*M!NNNNN */` (MariaDB), `/*!! */` reversed (MariaDB, digit-gated),
  `-- ` line (space/control required), `#` line, `*/`-inside-string, unterminated (reject-side)
- strings: `'…'`, `"…"` (mode-dependent), `N'…'`, charset-introducer `_utf8mb4'…'`, hex `X'…'`,
  bit `b'…'`, `$tag$…$tag$` (MySQL 9), doubled-quote escape, backslash escape (mode-dependent),
  `[brackets]` under MSSQL
- whitespace kinds: space, tab, `\n`, `\r`, `\v`, `\f` (and bare-`\r` non-terminating a line
  comment)
- trailing/embedded NUL, trailing `;`, `;`-chains of multiple statements

### Benign / non-actionable heads (for the ~5% + multi-statement tails)
- `CREATE TABLE/INDEX/VIEW/PROCEDURE/FUNCTION/TRIGGER/EVENT/SEQUENCE …`, `DROP …`, `INSERT`,
  `UPDATE`, `DELETE`, `GRANT`, `REVOKE`, `RENAME USER`, `XA …`, `BEGIN`, `COMMIT`, `SET …`,
  `SET STATEMENT … FOR <benign>`, `TRUNCATE`, `ANALYZE`, `OPTIMIZE`, `REPAIR`, `FLUSH`,
  `SAVEPOINT`, `ALTER {DATABASE|SCHEMA|VIEW|EVENT|USER|TABLESPACE|ALGORITHM=…}` — actionable-
  looking text inside string literals of these.

Checklist size: **~150 enumerated constructs** across the sections above.

## Comparison & reconciliation rules (exhaustive)

The `Diff` decision table (cases 1–8) and rules **R1–R13** in step 5 are the complete rule set
(R13 = the both-sides spec canonicalization required by the oracles' flattened `Alter_info`);
not repeated here. Summary of what is and is not a finding:

- **Findings**: `sig_mismatch`, `we_error` (oracle accept + our error), `panic`, `timeout`,
  `oracle_crash`, `oracle_timeout`.
- **Never findings** (reconciled): oracle reject with any non-panic our behavior (case 1);
  unreachable multi-statement `stmts[0]==other` (case 3); type-string cosmetics / synonyms /
  display widths / enum-set text / numeric canonicalization (R1,R4,R6); written-vs-defaulted
  numeric params, resolved by R13's default substitution on our side vs the oracle's recorded
  `params_written` (R2); tinyint(1)/bool equivalence (R3); unsigned/zerofill folding (R5); mutual
  `ERR` for unmapped types (R7); dropped benign specs / alter-side renames (R9); spec grouping,
  MODIFY/CHANGE-same-name, and inter-class spec order, all normalized by R13; anything in
  `ledger.jsonl` or `parked.list` (R12).

## Acceptance checks

- `go build -tags ddlfuzz ./...` in `flow/` succeeds (shim compiles); the overview verification
  gate passes unchanged.
- `cd tools/ddlfuzz && go build ./... && go vet ./... && go test ./...` green. Unit tests:
  - `internal/compare`: table-driven, one row per rule R1–R13 reusing the exact quirks from
    `ddl_parser_tidb_diff_test.go` (e.g. `NUMBER(10)`→`numeric(10,-1)`, `BOOL`→`bool`,
    `numeric` vs `decimal`, MariaDB `uuid`→ERR both, `int(11)`==`int`, multi-stmt truncation).
    Each row: `(ourSig, digest) → expected Divergence|nil`.
  - `internal/compare` descriptor: same input under different identifiers/byte-offsets ⇒ same
    `<sig>`; different engine or masked-mode ⇒ different `<sig>`.
  - `internal/wire`: round-trip framing against a fake in-memory server.
  - `internal/sancov`: OR-accumulation grows the accum bitmap and flags new-edge windows.
  - `internal/seed`: ≥600 seeds; every `tidbDiffModeCases` present with exact mode/engine.
  - `internal/dict`: files non-empty, ASCII-only.
  - `internal/exec`: a deliberately-panicking stub is contained and yields class `panic`; a
    sleep-stub trips the watchdog and yields class `timeout`.
- `ddlfuzz golden` exits 0 (once oracles exist) — the go/no-go for fuzzing.
- `ddlfuzz replay` on a seeded synthetic finding returns the documented exit codes.
- Throughput smoke: with fake oracles echoing digests, `fuzz` sustains ≥100k cases/s aggregate
  and stats.json updates every 30 s; SIGTERM flushes and exits 0 within 60 s.
- Crash-resume: kill -9 mid-run; restart replays `inflight.jsonl`; no state file corrupt.

## Risks & fallbacks

- **Oracle-bitmap-only retention misses corpus reachable only via a Go-side path the oracle
  doesn't distinguish.** Accepted for a 72h run: the oracle grammar is a strict superset of ours,
  so any construct our parser can reach is reachable in the oracle's grammar too; a divergence in
  a construct the oracle covers but we mishandle still surfaces as a finding regardless of whether
  the input was *retained*. Retention only accelerates re-exploration. The dropped Go-coverage
  attribution was the platform-y part; the campaign does not need it.
- **Digest JSON decode cost dominating at 100k/s.** Mitigation: `sync.Pool` reuse + fast-path
  equality (compare our sig to a lazily-built oracle sig; skip full decode when verdict byte
  says reject and our side didn't panic). Fallback: hand-rolled scanner for the fixed schema.
- **Divergence storm** (a single real bug hit by millions of inputs). Mitigation: dedup by
  `<sig>` + `max-open-findings` cap + minimize-once policy; the fix agent (40, now headless
  `codex exec`) ledgers/fixes, and R12 suppression re-reads the ledger every 60 s so throughput
  recovers without a restart.
- **Watchdog false positives** under GC pauses. Mitigation: 100 ms deadline is 10⁵× median;
  `debug.SetGCPercent` tuned modestly; abandoned-worker count tolerated up to 8 before restart.
- **Local mutation tokenizer diverges from the real lexer**, producing malformed splices.
  Acceptable: mutations need not be lexically valid (that is the point); byte-safety is the only
  hard invariant and is asserted post-mutation.

## Effort

Scoped to the single-campaign build (Scope note): oracle-SanCov-only retention, no `internal/gocov`,
no live distillation.

- Shim + exports + parity test: 0.5 d.
- `internal/wire` + `internal/oracle` (pool, respawn, bisection, coverage poll): 3 d.
- `internal/digest` + `internal/compare` (rules incl. R13 canonicalization, descriptor, dedup) +
  unit tests: 3 d.
- `internal/exec` (containment, watchdog): 1 d.
- `internal/sancov` + `internal/corpus` (retention + shutdown distillation): 1 d.
- `internal/gen` (full grammar tables): 3 d.
- `internal/mutate` + `internal/dict` + `internal/seed`: 2 d.
- `internal/run` (orchestrator, stats, flush, journal, resume): 2 d.
- `golden`, `replay`, `minimize` subcommands: 2 d.
- Integration, perf tuning to ≥100k/s, acceptance checks: 2 d.

**Total ≈ 19–20 person-days** (excludes oracle components 10/11 and supervisor 40). The bulk is
the comparison/reconciliation layer and the generator — both irreducible; the descope saved ~2–3d
of coverage-attribution plumbing that a one-run campaign doesn't need.

## Contract issues

1. **Multi-statement skip rule is narrower than the general case.** The overview says: skip
   signature comparison iff `len(stmts) > 1 && stmts[0].kind == "other"`. But our parser also
   truncates at the *first* `other` even when it is at position ≥1 (writeup intentional change 7:
   "Multi-statement events whose first statement is benign ignore the rest" — and more generally,
   after a parsed actionable statement, a following benign head stops classification). The
   overview's stmts[0] special-case handles only the leading-benign event; for
   `alter … ; <benign> ; alter …` the digest has `[alter, other, alter]` and our parser yields
   only the first alter. **My design conforms**: `OracleSig` truncates the digest at the first
   `other` before comparison (R8), which reduces `[alter, other, alter]` to just `alter` and
   matches us — the overview's explicit skip is the strict subset where truncation yields an empty
   signature and the case is entirely unreachable. I believe this is the intended behavior and the
   contract's wording is a simplification, not a conflict; flagging so 30/40 use the same
   truncation reducer. No contract change requested — I conform and extend consistently.

2. **`server rejects ⇒ our side may return anything` vs `we_error` as a finding.** The correctness
   contract says on server-accept our signature must *exactly* equal the oracle's, "any mismatch
   in any direction … is a bug", with no severity tiers. `we_error` (case 6) is therefore always a
   finding, even though the writeup's own safety property treats "benign misreported" as tolerable
   noise. I follow the overview (stricter): `we_error` is filed, then either fixed or *ledgered*
   with a citation. No deviation, noted only because it tightens the writeup's tolerance.

3. **`params_written` null-vs-value comparison is under-specified in the contract.** The digest
   contract says `params_written` is "null when none were written". Our side encodes unwritten as
   `-1`. R2 defines the exact mapping (null ⇒ -1 per position; present ⇒ the value), and states
   the oracle must never parse defaults out of `type_str`. This is a component-20-owned
   reconciliation detail (the contract explicitly delegates it), so it is a definition, not a
   change — recorded here for the oracle authors (10/11) to honor `params_written` semantics.

No binding contract is violated; items above are clarifications I have conformed to.
