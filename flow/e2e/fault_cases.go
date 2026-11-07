package e2e

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/internal/testhooks"
)

// faultHeldFailWait bounds how long a pause_fail case waits, while its pause
// holds, for the fail site in the other loop to fire.
const faultHeldFailWait = 90 * time.Second

// faultLargeBatchRows is the size of the one multi-file batch a case drives,
// for sources that support it.
const faultLargeBatchRows = 3

// FaultCaseTestName is the subtest name of a generated case. t.Run nests on "/".
func FaultCaseTestName(c testhooks.Case) string {
	return strings.ReplaceAll(c.Name(), "/", "__")
}

// faultCaseID is a short, stable ID for a case: flow names end up in
// replication slot names, which are limited to 63 bytes.
func faultCaseID(c testhooks.Case) string {
	sum := sha256.Sum256([]byte(c.Name()))
	return "g" + hex.EncodeToString(sum[:4])
}

// RunFaultShards runs every generated case of workload kind under
// TestFaultShard/<shard>/<case>, each case in parallel with its own suite.
func RunFaultShards[T e2eshared.Suite](
	t *testing.T, kind testhooks.Workload, setup func(*testing.T) T, workload func(T) FaultWorkload,
) {
	t.Helper()
	for shard := range testhooks.Shards {
		t.Run(fmt.Sprintf("%02d", shard), func(t *testing.T) {
			t.Parallel()
			for _, c := range testhooks.ShardCases(kind, shard) {
				t.Run(FaultCaseTestName(c), func(t *testing.T) {
					t.Parallel()
					s := setup(t)
					t.Cleanup(func() {
						ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
						defer cancel()
						s.Teardown(ctx)
					})
					RunFaultCase(workload(s), c)
				})
			}
		})
	}
}

// RunFaultCase arms the case's sites, drives the workload so each armed site
// can be reached, and runs the end-of-case checks.
func RunFaultCase(w FaultWorkload, c testhooks.Case) {
	h := NewFaultHarness(w, faultCaseID(c))
	switch c.Kind {
	case testhooks.CaseSingle, testhooks.CaseRetryPair:
		if c.First.Kind == testhooks.KindPause {
			h.drivePause(c.First, nil)
		} else {
			h.driveFails(c.First, c.Second)
		}
	case testhooks.CasePauseFail:
		h.drivePause(c.First, c.Second)
		// Gives the fail site a chance outside the hold too.
		h.Insert(0)
		h.Insert(1)
		h.InsertLarge(0, faultLargeBatchRows)
	}
	h.Finish()
}

// loopTable is the hook table filter that scopes a site to table 0's loops.
// CDC loops cover every table, so they take no filter.
func (h *FaultHarness) loopTable() string {
	if h.w.IndependentTables {
		return h.Table(0).HookTable
	}
	return ""
}

// driveFails arms first, and second to fire after first, then inserts across a
// schema change and a SyncFlow restart so sites in every loop are reached.
func (h *FaultHarness) driveFails(first testhooks.Site, second *testhooks.Site) {
	h.Arm(first, nil, "")
	if second != nil {
		h.Arm(*second, &first, "")
	}
	// A schema-change-only second site needs the schema change after first fired.
	schemaLater := second != nil && second.SchemaChangeOnly && !first.SchemaChangeOnly
	if !schemaLater {
		h.AddExtraColumn(0)
	}
	h.Insert(0)
	h.Insert(1)
	// Before WaitFired, in case first is reached only by a multi-file batch.
	h.InsertLarge(0, faultLargeBatchRows)
	if first.Loop == testhooks.LoopStartup {
		h.RestartSyncFlow()
	}
	if second != nil {
		h.WaitFired(first, faultLiveness)
		if schemaLater {
			h.AddExtraColumn(0)
		}
	}
	h.Insert(0)
	h.RestartSyncFlow()
	h.Insert(0)
	h.Insert(1)
}

// drivePause arms pause site p, drives its loop into the gap, does the work that
// races the gap, and releases it. If fail is set, it is armed in the other loop
// as soon as p holds, and gets a bounded wait to fire before the release.
// If p is never reached, drivePause returns without arming fail.
func (h *FaultHarness) drivePause(p testhooks.Site, fail *testhooks.Site) {
	table := h.loopTable()
	held := func() {
		if fail == nil {
			return
		}
		if fail.SchemaChangeOnly {
			h.AddExtraColumn(0)
		}
		h.Arm(*fail, nil, table)
	}
	switch p {
	case testhooks.CDCNormalizeIdleGap, testhooks.QCDCNormalizeIdleGap:
		h.Arm(p, nil, table)
		// Normalize reaches its idle gap only after finishing work.
		h.Insert(0)
		if !h.WaitFired(p, faultLiveness) {
			return
		}
		held()
		// Sync publishes a request while normalize sits between check and subscribe.
		h.Insert(0)
	case testhooks.CDCSyncBackpressureGap, testhooks.QCDCSyncBackpressureGap:
		// Slow normalize so sync fills the buffer and reaches the gap.
		release := h.Throttle(0, 20*time.Second)
		h.Arm(p, nil, table)
		for range FaultBuffer + 1 {
			h.Insert(0)
		}
		if !h.WaitFired(p, faultLiveness) {
			return
		}
		held()
		// Normalize catches up while sync sits between check and subscribe.
		release()
		domain := h.Table(0).Domain
		h.WaitProgress(faultLiveness, func(fp FaultProgress) bool {
			d := fp.Domains[domain]
			return d.Normalized >= d.Synced
		})
	case testhooks.QCDCNormalizeStartupGap:
		// This batch stays pending: its normalize is slow and the restart cancels it.
		release := h.Throttle(0, time.Minute)
		h.Insert(0)
		h.Arm(p, nil, table)
		h.RestartSyncFlow()
		if !h.WaitFired(p, faultLiveness) {
			return
		}
		held()
		release()
		// Sync publishes a newer request while normalize holds its startup snapshot.
		h.Insert(0)
	default:
		h.t.Fatalf("no driver for pause site %s", p.Name)
	}
	h.CheckSiblingProgresses(1)
	if fail != nil {
		h.WaitFired(*fail, faultHeldFailWait)
	}
	h.Release(p)
}
