# mongosh — MongoDB Shell Compiler

Compiles mongosh-style JavaScript statements into BSON wire commands for execution via the MongoDB Go driver. This is not a JavaScript runtime — it parses the AST statically, evaluates only literal expressions, and produces a `bson.D` command document that `upstream_mongodb.go` sends with `RunCommand` or `RunCommandCursor`.

## Three-layer pipeline

```
 "db.users.find({active: true}).sort({name: 1}).limit(10)"
                          │
                          ▼
 ┌─────────────── parser/ ────────────────────────────────┐
 │                                                        │
 │  goja JavaScript parser → AST                          │
 │  extractCallChain() walks DotExpression/CallExpression │
 │  evalLiteral() evaluates BSON constructors & literals  │
 │                                                        │
 │  → Statement{Collection:"users", Method:"find",        │
 │     Arguments:[`{"active":true}`],                     │
 │     Chainers:[{sort,[`{"name":1}`]}, {limit,["10"]}]}  │
 │                                                        │
 └────────────────────────┬───────────────────────────────┘
                          │
                          ▼
 ┌─────────────── command/ ───────────────────────────────┐
 │                                                        │
 │  Registry["find"] → MethodSpec                         │
 │    ParseArgsAccordingTo(spec.Args, rawArgs)            │
 │    spec.Build("users", parsedArgs) → bson.D + hints    │
 │    for each chainer: spec.Chainers[name].Apply(cmd)    │
 │  ValidateCommand(cmd) against allowlist                │
 │                                                        │
 │  → bson.D{find:"users", filter:{...}, sort:{...},      │
 │     limit:10, singleBatch:true, batchSize:10}          │
 │                                                        │
 └────────────────────────┬───────────────────────────────┘
                          │
                          ▼
 ┌─────────────── mongosh.go (Compile) ───────────────────┐
 │                                                        │
 │  Orchestrates the above, handles shell commands        │
 │  (show collections, show dbs, help()) via regex        │
 │  before falling through to the parser.                 │
 │                                                        │
 │  → ExecSpec{Command, ResultKind, Hints, ...}           │
 │                                                        │
 └────────────────────────────────────────────────────────┘
```

The caller (`upstream_mongodb.go`) adds a comment tag for cancel support, picks the database, and calls `RunCommand`/`RunCommandCursor` based on `ResultKind`.

## Package layout

```
mongosh/
├── mongosh.go              Compile() entry point, shell commands, ExecSpec, error types
├── mongosh_test.go         End-to-end compilation tests
├── parser/
│   ├── parse.go            goja AST → Statement
│   ├── statement.go        Statement and Chainer structs
│   ├── literal.go          AST expressions → Go values (BSON constructors, orderedMap)
│   └── literal_test.go
└── command/
    ├── registry.go         Table-driven MethodSpec/ChainerSpec definitions
    ├── builder.go          BuildFunc implementations (one per method)
    ├── chainer.go          ChainerSpec implementations (sort, limit, skip, etc.)
    ├── args.go             Raw JSON strings → typed Go values
    ├── allowlist.go        Wire command allowlist + ValidateCommand/ValidateInput
    ├── help.go             Help text generators
    └── *_test.go
```

## parser/

The parser turns a mongosh string into a `Statement` without evaluating any JavaScript. It uses [goja](https://github.com/dop251/goja)'s parser for the JavaScript AST, then walks the tree structurally.

### Step 1: JavaScript AST (goja)

`db.users.find({active: true}).sort({name: 1})` parses into nested AST nodes:

```
CallExpression                          ← .sort({name: 1})
├─ callee: DotExpression
│  ├─ left: CallExpression              ← .find({active: true})
│  │  ├─ callee: DotExpression
│  │  │  ├─ left: DotExpression
│  │  │  │  ├─ left: Identifier "db"
│  │  │  │  └─ identifier: "users"
│  │  │  └─ identifier: "find"
│  │  └─ args: [ObjectLiteral {active: true}]
│  └─ identifier: "sort"
└─ args: [ObjectLiteral {name: 1}]
```

### Step 2: extractCallChain — peel off calls from the outside in

Walk inward. Each `CallExpression` whose callee is a `DotExpression` yields a method call. Once there are no more calls, the remaining dots/identifiers are the head:

```
head:  ["db", "users"]
calls: [{name:"find", args:[ObjectLiteral]}, {name:"sort", args:[ObjectLiteral]}]
```

`BracketExpression` is also handled, so `db["my-collection"].find()` works.

### Step 3: extractStatement — classify by head length

- `head = ["db"]` → first call is a **database method** (`db.runCommand(...)`)
- `head = ["db", "users"]` → first call is a **collection method**, remaining calls are **chainers**
- `explain()` and `help()` are detected as special forms

Result for our example:

```
Statement{
  Type:       "collection",
  Collection: "users",
  Method:     "find",
  Arguments:  [`{"active":true}`],       ← JSON strings, not AST nodes
  Chainers:   [{Name:"sort", Arguments:[`{"name":1}`]}],
}
```

### Step 4: evalLiteral — AST expressions to JSON strings

Each argument's AST expression is recursively evaluated to a Go value, then JSON-encoded. This handles JavaScript literals (numbers, strings, arrays, objects) and BSON constructors:

```
ObjectId("abc...")    →  {"$oid": "abc..."}
ISODate("2024-...")   →  {"$date": "2024-..."}
NumberLong("123")     →  {"$numberLong": "123"}
/pattern/i            →  {"$regularExpression": {"pattern": "...", "options": "i"}}
```

The full list of supported constructors is in `literal.go`.

**Why Extended JSON as the intermediate format?** The parser outputs JSON strings, and the command layer parses them back with `bson.UnmarshalExtJSON` to get real BSON types (`bson.ObjectID`, `bson.DateTime`, etc.). This round-trip looks redundant — the parser could call `bson.ObjectIDFromHex()` etc. directly for roughly the same amount of code. The main reason is testability: the literal tests are plain string pairs (`{input: "ObjectId('hex')", want: '{"$oid":"hex"}'}`) that are easy to visually and programmatically compare. A secondary benefit is that the parser doesn't duplicate the driver's validation — it doesn't check whether an ObjectId hex string is 24 characters or whether a date is valid ISO 8601. It emits the notation and lets `UnmarshalExtJSON` or the server reject bad values later, saving lines in an already oversized package.

JavaScript objects are evaluated to `orderedMap` (a `[]keyValue` slice with custom `MarshalJSON`) rather than Go's `map[string]any`. This preserves field insertion order, which matters because MongoDB identifies commands by their first field.

## command/

### Registry

The registry is table-driven — a `map[string]MethodSpec` keyed by lowercased method name. Each `MethodSpec` has:

- **Name**: display name preserving original casing (e.g. `"findOne"` not `"findone"`), used in help output.
- **Scope**: `"collection"` or `"database"` — determines how the parser classifies `db.coll.method()` vs `db.method()`.
- **Args**: a `[]ArgKind` declaring the expected argument types in order. The kinds are `ArgJSON` (object/array), `ArgString`, `ArgInt`, and `ArgBool`. `ArgOptional` is a prefix marker — `{ArgJSON, ArgOptional, ArgJSON}` means "required doc, optional doc". `ParseArgsAccordingTo` walks this spec against the raw JSON strings from the parser, converting each to a typed Go value.
- **Build**: a `BuildFunc` that takes the collection name and parsed arguments and returns the `bson.D` command, `ExecHints`, and `ResultKind`.
- **Chainers**: a `map[string]ChainerSpec` of allowed chainers for this method. If a chainer isn't in this map, the compiler rejects it.

Adding a new mongosh method means adding one `MethodSpec` and a builder function — no new control flow.

### Builders and chainers

Each method has a `BuildFunc` that takes the collection name and parsed arguments and returns a `bson.D` command. For example, `FindBase` produces `{find: "coll", filter: {...}}`, `AggregateBase` produces `{aggregate: "coll", pipeline: [...], cursor: {}}`, and `PassCommand` (for `runCommand`) passes the document through as-is.

Chainers modify the command after the builder creates it. Each `ChainerSpec` has an `Apply` function that calls `Upsert` to set or update fields in the `bson.D`. Some chainers also set `ExecHints` — for instance, `.limit(10)` on a `find` not only sets the `limit` field but also tells `upstream_mongodb.go` to set `singleBatch` and `batchSize` for efficient cursor handling.

Chainers are applied in statement order, matching mongosh behavior.

### Allowlist

The second security layer. After the registry builds a command, `ValidateCommand` checks that the first key of the `bson.D` appears in the allowlist (a categorized map of read-only wire protocol commands: query, diagnostic, replication status, etc.). The registry controls which *shell methods* are allowed; the allowlist controls which *wire commands* can actually be sent, catching `db.runCommand({insert: ...})` and similar.

Each allowlist entry also declares whether the command supports the `comment` field, which is how cancel tracking works.

## mongosh.go — Compile() orchestration

`Compile` ties the layers together:

1. **Shell commands** are matched first by regex — `show collections`, `show databases`/`show dbs`, and `help()` bypass the parser entirely and return pre-built `ExecSpec` values with `Formatter` functions for columnar output.
2. **Input validation** rejects multiple semicolon-separated statements.
3. **Parse** via `parser.ParseStatement` → `Statement`.
4. **Help detection** — `db.help()`, `db.coll.help()`, `db.coll.find().help()` return help text.
5. **Registry validation** — method must exist, chainers must be allowed for that method.
6. **Build** — parse arguments per the spec, call the builder, apply chainers in order.
7. **Explain wrapping** — if the statement was `db.coll.explain().find(...)`, wrap the built command in `{explain: cmd}` and force result kind to Scalar.
8. **Allowlist validation** — final gate before returning the `ExecSpec`.

## Integration with upstream_mongodb.go

`Compile()` returns an `ExecSpec` containing the command, result kind (Cursor vs Scalar), execution hints, and an optional `Formatter`. The upstream then:

- Adds `{comment: "peerdb-<8hex>"}` to commands that support it, enabling cancel via `currentOp`/`killOp`
- Picks the target database (connection db, or `admin` for commands like `listDatabases`)
- Calls `RunCommandCursor` or `RunCommand` based on `ResultKind`
- If a `Formatter` is set (shell commands like `show collections`), collects all docs and returns columnar TEXT output instead of raw JSON

All regular MongoDB results come back as a single `result` column with OID 114 (json). Formatted results use TEXT columns instead.
