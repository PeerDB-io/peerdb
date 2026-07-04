package golden

import (
	"fmt"
	"math/rand/v2"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/seed"
)

func TestRunPoolOrderInvariant(t *testing.T) {
	const n = 200
	jobs := make([]job, n)
	for i := range jobs {
		jobs[i] = job{index: i, seed: seed.Seed{SQL: fmt.Sprintf("stmt-%d", i)}, engine: "mysql"}
	}

	fn := func(j job) caseResult {
		time.Sleep(time.Duration(rand.IntN(200)) * time.Microsecond)
		return caseResult{row: Row{SQL: j.seed.SQL, Engine: j.engine}}
	}

	want := make([]caseResult, n)
	for i, j := range jobs {
		want[i] = caseResult{row: Row{SQL: j.seed.SQL, Engine: j.engine}}
	}

	for _, workers := range []int{1, 2, 4, 8, 16, 64} {
		for rep := 0; rep < 5; rep++ {
			got := runPool(jobs, workers, fn)
			if len(got) != len(want) {
				t.Fatalf("workers=%d rep=%d: len=%d want %d", workers, rep, len(got), len(want))
			}
			for i := range got {
				if got[i].row.SQL != want[i].row.SQL {
					t.Fatalf("workers=%d rep=%d: results[%d].SQL=%q want %q (order not preserved)",
						workers, rep, i, got[i].row.SQL, want[i].row.SQL)
				}
			}
		}
	}
}

func TestRunPoolRunsEachJobOnce(t *testing.T) {
	const n = 500
	jobs := make([]job, n)
	for i := range jobs {
		jobs[i] = job{index: i}
	}
	var calls int64
	got := runPool(jobs, 8, func(job) caseResult {
		atomic.AddInt64(&calls, 1)
		return caseResult{}
	})
	if calls != n {
		t.Fatalf("fn called %d times, want %d", calls, n)
	}
	if len(got) != n {
		t.Fatalf("len(results)=%d, want %d", len(got), n)
	}
}

func TestRunPoolEmpty(t *testing.T) {
	got := runPool(nil, 4, func(job) caseResult {
		t.Fatal("fn should not be called for empty job set")
		return caseResult{}
	})
	if len(got) != 0 {
		t.Fatalf("len=%d, want 0", len(got))
	}
}
