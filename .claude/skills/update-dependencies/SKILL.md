---
name: update-peerdb-deps
description: Updates dependencies in the PeerDB repository. Use when explicitly requested via a command.
---

Make a to-do list for the following sequence, then execute it:
1. Make sure you're on the latest `main` branch. Run `./generate-protos.sh` to avoid build failures down the line
2. Create a new branch `update-dependencies` (delete the old one if it's more than a few days old). Don't delete the remote branch
3. Update /nexus (Rust)
    a. `cargo update`
    b. see if `cargo outdated -R` has anything new (apart from `prost` that didn't resolve its compatibility issues after recent upstream changes, and `tonic`/`tonic-health`/`pbjson`/`pbjson-types`/`reqwest` it depends on, don't touch those). If it does, update those one by one in the right order and resolve incompatibilities, flagging to the user if something is non-trivial
    c. make sure the project builds with all features enabled (some are disabled by default)
4. Update /ui (TypeScript)
    a. `npm update`
    b. `npm out`, update the remaining ones one by on in the right order, resolve incompatibilities, flagging to the user if something is non-trivial
    c. test the build with `npm run build`
5. Update /flow (Go)
    a. `go get -u . && go mod tidy` - then undo the updates for dependencies marked as `// BREAKING`, we have tasks for resolving those separately
    b. flag if anything else was breaking and doesn't have an easy fix
    c. validate with `go build`
6. Update Buf plugins in `buf.gen.yaml`, flag if anything is more than a simple version bump
7. Report the summary to the user, listing all deps that needed a manual intervention. Prompt them to run the UI app locally to check for runtime errors. If there are unresolved Go dependency breaks, remind the user to mark them as // BREAKING if they're planning to tackle those later
