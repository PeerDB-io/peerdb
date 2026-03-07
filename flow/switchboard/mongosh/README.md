# mongosh — MongoDB Wire Command Compiler

Parses Extended JSON wire commands and validates them against an allowlist of allowed read-only commands. The `help` command is matched by regex and returns a formatted command reference.

## Pipeline

```
 '{"find": "users", "filter": {"active": true}}'
                          │
                          ▼
 ┌─────────────── compile.go ───────────────────┐
 │                                              │
 │  1. Match "help" → return help text          │
 │  2. bson.UnmarshalExtJSON → bson.D           │
 │  3. ValidateCommand (allowlist check)        │
 │  4. LookupCommand → ReturnsCursor, AdminDB   │
 │                                              │
 │  → ExecSpec{Command, ResultKind, AdminDB}    │
 │                                              │
 └──────────────────────────────────────────────┘
```

The caller (`upstream_mongodb.go`) adds a comment tag for cancel support, picks the database, and calls `RunCommand`/`RunCommandCursor` based on `ResultKind`.

## Package layout

```
mongosh/
├── compile.go             Compile() entry point, shell commands, ExecSpec, error types
├── compile_test.go        Compilation tests
└── command/
    ├── allowlist.go        Wire command allowlist + ValidateCommand + LookupCommand
    ├── help.go             Help text generator (help())
    └── *_test.go
```

## Allowlist

The allowlist (`command/allowlist.go`) is the security gate. Each entry declares:

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
