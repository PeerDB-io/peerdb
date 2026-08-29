# Catalog migrations

Schema migrations for the PeerDB catalog (Postgres), run with [goose](https://github.com/pressly/goose).

### Installing goose cli

```
go install github.com/pressly/goose/v3/cmd/goose@latest
```

### Create new migration file

```
goose -dir flow/db/migrations -s create <short_name> sql
```
The generated template contains placeholder statements and a `-- +goose Down` section; replace the placeholders and delete the Down section.

### Migration guidelines

- Never edit a migration that has shipped. Goose does not checksum applied files.
- Goose splits files into statements line-by-line: a line ending in `;` ends a statement. If a string literal contains an end-of-line semicolon (see `00030`, whose seeded value embeds a multi-line SQL script), wrap that statement in `-- +goose StatementBegin` / `-- +goose StatementEnd`. A marked block is sent as a single command, so wrap exactly one statement per block.
- For `CREATE INDEX CONCURRENTLY`, add `-- +goose NO TRANSACTION` at the top of the file. Such a file loses per-file atomicity, so keep it single-purpose and idempotent. Note that on retry:
  ```
  CREATE INDEX CONCURRENTLY IF NOT EXISTS my_index ...
  ```
  PostgreSQL sees the invalid object's name and skips creation. The migration succeeds, but the index remains unusable. Instead:
  ```
  DROP INDEX CONCURRENTLY IF EXISTS my_index;
  CREATE INDEX CONCURRENTLY my_index ON ...;
  ```
  This removes any partial or invalid index before rebuilding it, making a failed migration safe to retry.

## History and bootstrap from refinery

The catalog was previously migrated by [refinery](https://github.com/rust-db/refinery) in Rust. Those migrations lived in `nexus/catalog/migrations/` as `V<n>__<name>.sql` and were recorded in `public.refinery_schema_history`. The files here are the same SQL, renamed to goose's `000<n>_<name>.sql` format: **version numbers are the shared identity across both tools**, which is what makes the cutover safe.

On every run, before applying anything, `bootstrapFromRefinery` (in `migrations.go`) translates refinery's ledger into goose's in a single transaction under an advisory lock:

1. Probe for both ledger tables. Act only when `goose_db_version` is absent and `refinery_schema_history` is present. A fresh installation skips straight to goose; an already-bootstrapped catalog skips bootstrap after.
2. Create `goose_db_version` with the same DDL goose itself uses, including goose's version-0 sentinel row.
3. Insert one applied row per version actually present in `refinery_schema_history` so for a deployment that applied up to V40, goose will apply 41 and up.
