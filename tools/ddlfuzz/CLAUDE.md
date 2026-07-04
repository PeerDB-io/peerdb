# ddlfuzz

Differential DDL fuzzer (our parser vs the C++ MySQL/MariaDB oracles) plus a supervisor
(`ddlsuper`) that runs a long campaign, files findings, and drives codex fixes. While a campaign
runs, the supervisor is the sole git authority on `parser-wip`.

## Landing changes into a live campaign — `ddlsuper merge-staged`

While a campaign runs, develop in a worktree and land through `merge-staged`, which coordinates
with the fix loop at a quiescent point. Keep the work in worktrees: the main checkout belongs to
the fix loop, whose rollback (`git reset --hard` + `DeleteNewUntracked`) treats anything committed
or created there as transient.

Workflow:
1. Develop on a feature branch in a worktree under `tools/ddlfuzz/worktrees/<name>/` — gitignored,
   so rollback and the drift check leave it alone.
2. Roll feature branch(es) up onto `ddlfuzz-staged` in the `tools/ddlfuzz/staged/` worktree with
   normal merges. Resolve all conflicts here.
3. From `tools/ddlfuzz`, run `./build/ddlsuper merge-staged`. With a campaign running it merges the
   current campaign head into `ddlfuzz-staged` (normal merge, CLI side), hands off via the merge
   slot, and waits; the supervisor (fix loop, at a quiescent point) runs `git merge --ff-only
   ddlfuzz-staged` onto the campaign tree, then the full gate + golden + `replay --all`, and keeps
   the merge once all pass. On any failure the campaign tree returns to `last_good_commit`, so you
   fix in staged and retry. With no supervisor running, `merge-staged` takes the lock and runs all
   of this itself, inline.

**Run `merge-staged` in a background shell.** It waits for the fix loop to reach a safe point — up
to one full in-flight attempt (~1–2h; `--ack-timeout` default 2h) — then pays a full gate, so
background it and read the printed result when it returns. Exit codes: `0` merged · `2`
conflict/not-ff/stale · `3` gate/golden/stale-oracle (rolled back) · `4` replay regression (rolled
back) · `6` slot busy · `7` canceled. Cancel from another shell with `ddlsuper merge-cancel
[--keep-hold]`; `ddlsuper status` shows the merge slot.

`--no-restart` (only relevant when the merged diff touches `supervisor/`): by default a
supervisor-touching merge rebuilds `ddlsuper` and exec-restarts it in place — same PID, fast
resume, new supervisor code live immediately (`supervisor_restart=restarting`). `--no-restart`
skips the exec: the merge still lands, validates, and rebuilds `ddlsuper`, but it writes a
`state/RESTART_REQUIRED` marker (`supervisor_restart=required`, shown as a banner in `ddlsuper
status`) and leaves the running supervisor on the *old* image — correct against the merged tree,
just not-new. Pick up the rebuilt binary whenever you like with Ctrl-C + `ddlsuper run`: preflight
sees the marker, HEAD matches the validated head, so it fast-resumes (skips golden + gate) and
clears the marker. Use it to avoid betting a live campaign on an in-place exec of a risky
supervisor change, or to defer the restart to a controlled moment. (Merges that don't touch
`supervisor/` never restart, so the flag is a no-op there; and with no supervisor running the
merge is inline and inherently no-restart — it rebuilds `ddlsuper` and the next `run` fast-resumes.)

For oracle changes, rebuild in staged (`oracle/*/build.sh`, which writes the manifest) so the hash
matches — merge servicing verifies the manifest before copying binaries and re-runs golden.

Full protocol and state layout: `plan/43-merge-staged.md`.
