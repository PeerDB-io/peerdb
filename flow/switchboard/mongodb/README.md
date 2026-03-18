# MongoDB Wire Command Compiler

Parses Extended JSON wire commands and validates them against an allowlist of read-only commands. `help` is matched by regex and returns a formatted command reference.

To preview help output without a running server: `go test . -run TestPrintAllHelp -v`

## Pipeline

```
 '{"find": "users", "filter": {"active": true}}'
                          │
                          ▼
 ┌──────────── mongodb.go ────────────────────┐
 │                                            │
 │  1. Match "help" → return help text        │
 │  2. bson.UnmarshalExtJSON → bson.D         │
 │  3. validateCommand (allowlist check)      │
 │  4. lookupCommand → ReturnsCursor, AdminDB │
 │                                            │
 │  → ExecSpec{Command, ResultKind, AdminDB}  │
 │                                            │
 └────────────────────────────────────────────┘
```

The caller (`upstream_mongodb.go`) adds a comment tag for cancel support, picks the database, and calls `RunCommand`/`RunCommandCursor` based on `ResultKind`.

## Package layout

```
mongodb/
├── allowlist.go       Wire command allowlist, validation, comment support lookup
├── allowlist_test.go
├── mongodb.go         Compile() entry point, ExecSpec, error types, help text
└── mongodb_test.go
```

## Allowlist

The allowlist (`allowlist.go`) is the security gate. Each entry declares:

- **Name**: wire command name (e.g. `find`, `aggregate`, `ping`)
- **Required/Optional**: expected fields (for documentation; not enforced at parse time)
- **SupportsComment**: whether the command supports a `comment` field for cancel tracking
- **ReturnsCursor**: whether `RunCommandCursor` should be used (vs `RunCommand`)
- **AdminDB**: whether the command must run against the `admin` database

Commands not in the allowlist are rejected. This is default-deny.

## Integration with upstream_mongodb.go

`Compile()` returns an `ExecSpec` containing the command and result kind (Cursor vs Scalar). The upstream then:

- Adds `{comment: "peerdb-<8hex>"}` to commands that support it, enabling cancel via `currentOp`/`killOp`
- Picks the target database (connection db, or `admin` for commands like `listDatabases`)
- Calls `RunCommandCursor` or `RunCommand` based on `ResultKind`

All MongoDB results come back as a single `result` column with OID 114 (json). Help output uses TEXT columns.
