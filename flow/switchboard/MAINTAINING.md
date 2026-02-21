# Maintaining the Switchboard stacked PRs

Switchboard is split into three stacked PRs so each upstream can be reviewed independently:

```
main
 └── switchboard-pg      (shared infra + PostgreSQL)
      └── switchboard-mysql   (adds MySQL)
           └── switchboard-mongo   (adds MongoDB)
                └── switchboard-stack   (this file)
```

Each branch has a single commit. When a branch needs changes, amend its commit and rebase the branches above it.

## Amending a branch

```sh
git checkout switchboard-pg
# make your changes
git add -p
git commit --amend --no-edit
```

Then propagate downward (see below).

## Propagating a fix down the stack

After amending `switchboard-pg`, rebase the rest of the stack:

```sh
git checkout switchboard-mysql
git rebase switchboard-pg

git checkout switchboard-mongo
git rebase switchboard-mysql

git checkout switchboard-stack
git rebase switchboard-mongo
```

After amending `switchboard-mysql`, rebase mongo and stack:

```sh
git checkout switchboard-mongo
git rebase switchboard-mysql

git checkout switchboard-stack
git rebase switchboard-mongo
```

After amending `switchboard-mongo`, rebase stack:

```sh
git checkout switchboard-stack
git rebase switchboard-mongo
```

`switchboard-stack` is the leaf — amending it needs no propagation.

## Expected conflicts during rebase

Two files change at every layer and will conflict:

**`flow/switchboard/upstream.go`** — the `NewUpstream` switch gains one case per layer. On conflict, keep all cases accumulated so far plus the new one being added. The default error message should reflect the supported set.

**`flow/switchboard/README.md`** — description, architecture diagram, cancel section, and upstream list each grow. On conflict, keep all content accumulated so far plus the incoming additions.

**`flow/go.mod`** — dep changes are usually non-overlapping. If there's a conflict, run `go mod tidy` after resolving and commit the result.

## After each rebase

Rebases can silently corrupt files (duplicate imports, resurrected deleted code, wrong conflict resolution). After rebasing each branch:

1. **Visually inspect the known-sensitive files** — `upstream.go`, `README.md`, and any test file that was modified in earlier layers. Check that the diff from the parent branch looks exactly like the incremental change you expect, not some merged or mangled version:

```sh
git diff switchboard-pg..switchboard-mysql -- flow/switchboard/upstream.go
git diff switchboard-mysql..switchboard-mongo -- flow/switchboard/upstream.go
```

2. **Build and lint** before moving to the next branch:

```sh
# From flow/
go build ./switchboard/... ./e2e/...
golangci-lint run ./switchboard/... ./e2e/...
```

When fixing a lint issue, run the linter and confirm zero issues before committing the fix.

## Pushing after rebase

All affected branches need a force-push after any rebase:

```sh
git push --force-with-lease origin switchboard-pg switchboard-mysql switchboard-mongo switchboard-stack
```

## Syncing with main

When main advances, rebase the whole stack:

```sh
git checkout switchboard-pg
git rebase main

git checkout switchboard-mysql
git rebase switchboard-pg

git checkout switchboard-mongo
git rebase switchboard-mysql

git checkout switchboard-stack
git rebase switchboard-mongo
```

Resolve any conflicts at the pg layer first before moving down.
