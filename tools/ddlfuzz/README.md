# ddlfuzz

ddlfuzz is a differential fuzzer for PeerDB's MySQL/MariaDB DDL parser
(`flow/connectors/mysql/ddl_parser.go`). That parser reads DDL from binlog
QueryEvents during CDC and extracts the schema effects of `ALTER TABLE` and
`RENAME TABLE`; every other statement must be recognized as benign. It is
tested against the real thing: the actual MySQL and MariaDB server parsers.
Any disagreement is a bug.

A fuzz case is `(statement bytes, sql_mode, engine)`. The oracle parses it with
the real server parser and returns a digest: accept or reject, plus the reduced
ALTER/RENAME effect. If the server rejects, the statement can never appear in a
binlog, so we only require no panic or hang. If the server accepts, our parsed
signature must exactly match the oracle's, after canonicalization rules that
absorb known cosmetic engine differences. During a campaign, each finding is
handed to a headless coding agent that fixes it unattended, with the supervisor
independently verifying every fix.

## How It Works

The pieces:

* **Oracles** — small C++ drivers linked against real server source
  (MySQL 9.7, MariaDB 13.1). Each parses in-process and emits a JSON
  digest of the statement's effects plus SanitizerCoverage counters,
  over a stdin/stdout frame protocol.

* **ddlfuzz** — the fast lane. Generates and mutates DDL (grammar tables plus a
  dictionary extracted from the servers' lexers), runs each case through our
  parser in-process and through a pool of oracle processes, and compares.
  Oracle SanCov coverage decides what enters the corpus. Divergences are
  fingerprinted by a normalized behavior key, so re-discoveries dedupe, and
  filed under `state/findings/<sig>/` with an exact repro.

* **ddlfuzz-e2e** — the slow lane. Real MySQL/MariaDB containers, real binlog.
  Executes DDL, tails the binlog exactly like production CDC, and checks our
  parsed schema delta against what the server actually did.

* **ddlsuper** — the campaign supervisor. Runs both lanes for ~72h, restarts and
  un-wedges children, and drives a fix loop: each finding group goes to a
  headless codex agent that must fix the parser, fix the oracle harness, or
  ledger a genuine engine divergence with citation. The supervisor trusts
  nothing: it re-runs the full gate, replays all findings, and hard-resets to
  the last good commit on any failure. While a campaign runs it is the sole git
  authority on `parser-wip` — develop in `worktrees/`, land via `merge-staged`.

## Usage

```
./build/build.sh               # fuzzer binaries; oracle/*/build.sh for oracles
./build/ddlsuper run           # full campaign (run in tmux)
./build/ddlsuper status        # campaign digest
./build/ddlfuzz golden         # committed seeds vs both oracles; must exit 0
./build/ddlfuzz replay <sig>   # 0 = reconciled, 10 = still diverges
./build/ddlsuper merge-staged  # land staged changes into a live campaign
```

Design docs are in `plan/` (start with `plan/00-overview.md`); they are the
binding contracts. Operator notes for live campaigns are in `CLAUDE.md`.
