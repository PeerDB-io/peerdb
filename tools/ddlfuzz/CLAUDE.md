# ddlfuzz

Differential DDL fuzzer (our parser vs C++ MySQL/MariaDB oracles) plus a supervisor
(`ddlsuper`) that runs a long campaign, files findings, and drives codex fixes. The supervisor
is the **sole git authority** on `parser-wip` while it runs.

## Landing changes into a live campaign — `ddlsuper merge-staged`

Never commit, rebase, or `reset` the main checkout directly while a campaign runs, and never leave
new files under `tools/ddlfuzz/` there: a fix attempt's rollback runs `git reset --hard` +
`DeleteNewUntracked` and will wipe them. Develop elsewhere and land through `merge-staged`, which
coordinates with the fix loop at a quiescent point (never races it).

Workflow:
1. Develop on a feature branch in a worktree under `tools/ddlfuzz/worktrees/<name>/` (gitignored,
   so rollback and the drift check ignore it).
2. Roll feature branch(es) up onto `ddlfuzz-staged` in the `tools/ddlfuzz/staged/` worktree using
   normal merges. All conflict resolution happens here.
3. From `tools/ddlfuzz`, run `./build/ddlsuper merge-staged`. It `--ff-only` merges `ddlfuzz-staged`
   into the campaign tree, runs the full gate + golden + `replay --all`, and on any failure rolls
   the campaign tree back to `last_good_commit` (fix in staged, retry). With no supervisor running
   it does the same inline.

**Run `merge-staged` in a background shell.** It blocks until the fix loop hits a safe point — worst
case one full in-flight attempt (~1–2h; `--ack-timeout` default 2h) — then pays a full gate. Don't
tie up a foreground shell waiting; background it and read the printed result when it returns. Exit
codes: `0` merged · `2` conflict/not-ff/stale · `3` gate/golden/stale-oracle (rolled back) ·
`4` replay regression (rolled back) · `6` slot busy · `7` canceled. `ddlsuper merge-cancel
[--keep-hold]` cancels from another shell; `ddlsuper status` shows the merge slot.

Oracle changes: rebuild in staged (`oracle/*/build.sh`, which writes the manifest) so the manifest
hash matches — merge servicing verifies it before copying binaries and re-runs golden.

Full protocol and state layout: `plan/43-merge-staged.md`.
