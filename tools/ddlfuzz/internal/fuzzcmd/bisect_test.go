package fuzzcmd

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/compare"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/run"
)

type bisectRecording struct {
	singles []recordedSingle
	batches []recordedBatch
}

type recordedSingle struct {
	c   run.Case
	err error
}

type recordedBatch struct {
	batch []run.Case
	err   error
	shape string
}

func makeCases(n int) []run.Case {
	out := make([]run.Case, n)
	for i := range out {
		out[i] = run.Case{
			SQL:     []byte(fmt.Sprintf("ALTER TABLE t%d ADD COLUMN c INT", i)),
			SQLMode: compare.SQLModeANSIQuotes,
			Engine:  run.EngineMySQL,
			Origin:  run.OriginGen,
			Seed:    uint64(i),
		}
	}
	return out
}

func containsSeed(batch []run.Case, seed uint64) bool {
	for _, c := range batch {
		if c.Seed == seed {
			return true
		}
	}
	return false
}

func runFakeBisect(ctx context.Context, batch []run.Case, fail func([]run.Case) error, budget int) (bisectRecording, int) {
	var rec bisectRecording
	submits := 0
	b := &bisector{
		submit: func(ctx context.Context, batch []run.Case) error {
			submits++
			return fail(batch)
		},
		recordSingle: func(c run.Case, err error) {
			rec.singles = append(rec.singles, recordedSingle{c: c, err: err})
		},
		recordBatch: func(batch []run.Case, err error, shape string) {
			cp := append([]run.Case(nil), batch...)
			rec.batches = append(rec.batches, recordedBatch{batch: cp, err: err, shape: shape})
		},
		budget: budget,
	}
	b.bisect(ctx, batch, fail(batch))
	return rec, submits
}

func TestBisectDeterministicSingleCrasher(t *testing.T) {
	batch := makeCases(1000)
	failErr := errors.New("oracle died")
	budget := bisectBudget(len(batch))
	rec, submits := runFakeBisect(context.Background(), batch, func(batch []run.Case) error {
		if containsSeed(batch, 731) {
			return failErr
		}
		return nil
	}, budget)

	if len(rec.singles) != 1 || rec.singles[0].c.Seed != 731 {
		t.Fatalf("singles=%v, want only seed 731", rec.singles)
	}
	if len(rec.batches) != 0 {
		t.Fatalf("batches=%v, want none", rec.batches)
	}
	if submits > budget {
		t.Fatalf("submits=%d > budget=%d", submits, budget)
	}
	if submits > 2*bitLen(len(batch))+2 {
		t.Fatalf("submits=%d, want approximately 2*log2(1000)", submits)
	}
}

func TestBisectTwoIndependentCrashers(t *testing.T) {
	batch := makeCases(1000)
	failErr := errors.New("oracle died")
	budget := bisectBudget(len(batch))
	rec, submits := runFakeBisect(context.Background(), batch, func(batch []run.Case) error {
		if containsSeed(batch, 123) || containsSeed(batch, 876) {
			return failErr
		}
		return nil
	}, budget)

	if len(rec.singles) != 2 {
		t.Fatalf("single count=%d, want 2", len(rec.singles))
	}
	got := []uint64{rec.singles[0].c.Seed, rec.singles[1].c.Seed}
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	if got[0] != 123 || got[1] != 876 {
		t.Fatalf("single seeds=%v, want [123 876]", got)
	}
	if len(rec.batches) != 0 {
		t.Fatalf("batches=%v, want none", rec.batches)
	}
	if submits > budget {
		t.Fatalf("submits=%d > budget=%d", submits, budget)
	}
}

func TestBisectStateDependentFullBatch(t *testing.T) {
	batch := makeCases(1000)
	failErr := errors.New("full batch crash")
	rec, _ := runFakeBisect(context.Background(), batch, func(batch []run.Case) error {
		if len(batch) == 1000 {
			return failErr
		}
		return nil
	}, bisectBudget(len(batch)))

	if len(rec.singles) != 0 {
		t.Fatalf("single count=%d, want 0", len(rec.singles))
	}
	if len(rec.batches) != 1 {
		t.Fatalf("batch count=%d, want 1", len(rec.batches))
	}
	if got := rec.batches[0]; len(got.batch) != 1000 || got.shape != shapeStateDependent || got.err != failErr {
		t.Fatalf("batch=(len %d shape %q err %v), want full state-dependent with original error", len(got.batch), got.shape, got.err)
	}
}

func TestBisectStateDependentMidLevel(t *testing.T) {
	batch := makeCases(1000)
	failErr := errors.New("large half crash")
	rec, _ := runFakeBisect(context.Background(), batch, func(batch []run.Case) error {
		if len(batch) >= 500 {
			return failErr
		}
		return nil
	}, bisectBudget(len(batch)))

	if len(rec.singles) != 0 {
		t.Fatalf("single count=%d, want 0", len(rec.singles))
	}
	if len(rec.batches) == 0 {
		t.Fatal("batch count=0, want at least one 500-case state-dependent finding")
	}
	for _, got := range rec.batches {
		if len(got.batch) != 500 || got.shape != shapeStateDependent || got.err != failErr {
			t.Fatalf("batch=(len %d shape %q err %v), want 500-case state-dependent with original error", len(got.batch), got.shape, got.err)
		}
	}
}

func TestBisectBudgetExhaustion(t *testing.T) {
	batch := makeCases(1000)
	failErr := errors.New("always fails")
	budget := bisectBudget(len(batch))
	rec, submits := runFakeBisect(context.Background(), batch, func(batch []run.Case) error {
		return failErr
	}, budget)

	if submits != budget {
		t.Fatalf("submits=%d, want budget %d", submits, budget)
	}
	if len(rec.batches) == 0 {
		t.Fatal("batch count=0, want at least one budget-exhausted finding")
	}
	seen := map[uint64]bool{}
	for _, got := range rec.batches {
		if got.shape != shapeBudgetExhausted {
			t.Fatalf("shape=%q, want %q", got.shape, shapeBudgetExhausted)
		}
		for _, c := range got.batch {
			if seen[c.Seed] {
				t.Fatalf("case seed %d filed in more than one batch finding", c.Seed)
			}
			seen[c.Seed] = true
		}
	}
}

func TestBisectLenOneFailingBatch(t *testing.T) {
	batch := makeCases(1)
	failErr := errors.New("single crash")
	rec, submits := runFakeBisect(context.Background(), batch, func(batch []run.Case) error {
		return failErr
	}, bisectBudget(len(batch)))

	if submits != 0 {
		t.Fatalf("submits=%d, want 0 extra submits", submits)
	}
	if len(rec.singles) != 1 || rec.singles[0].c.Seed != 0 {
		t.Fatalf("singles=%v, want only seed 0", rec.singles)
	}
	if len(rec.batches) != 0 {
		t.Fatalf("batches=%v, want none", rec.batches)
	}
}

func TestBisectClassPerLevelErrorPreserved(t *testing.T) {
	batch := makeCases(8)
	rec, _ := runFakeBisect(context.Background(), batch, func(batch []run.Case) error {
		if containsSeed(batch, 6) {
			if len(batch) == 1 {
				return context.DeadlineExceeded
			}
			return errors.New("parent crash")
		}
		return nil
	}, bisectBudget(len(batch)))

	if len(rec.singles) != 1 {
		t.Fatalf("single count=%d, want 1", len(rec.singles))
	}
	if !errors.Is(rec.singles[0].err, context.DeadlineExceeded) {
		t.Fatalf("single err=%v, want context deadline exceeded", rec.singles[0].err)
	}
	if got := crashClass(rec.singles[0].err); got != "oracle_timeout" {
		t.Fatalf("crashClass=%q, want oracle_timeout", got)
	}
}

func TestBisectStopSentinel(t *testing.T) {
	batch := makeCases(1000)
	calls := 0
	rec, submits := runFakeBisect(context.Background(), batch, func(batch []run.Case) error {
		calls++
		if calls == 2 {
			return errBisectStop
		}
		return nil
	}, bisectBudget(len(batch)))

	if submits != 2 {
		t.Fatalf("submits=%d, want left and right resubmits before abort", submits)
	}
	if len(rec.singles) != 0 || len(rec.batches) != 0 {
		t.Fatalf("singles=%d batches=%d, want no findings", len(rec.singles), len(rec.batches))
	}
}

func TestBisectDescriptorShapesDistinct(t *testing.T) {
	base := compare.Descriptor{
		V:       1,
		Engine:  "mysql",
		SQLMode: compare.SQLModeANSIQuotes,
		Class:   "oracle_crash",
		Lane:    "fast",
	}
	shapes := []string{shapeStateDependent, shapeBudgetExhausted, "head=ALTER"}
	sigs := map[string]struct{}{}
	for _, shape := range shapes {
		desc := base
		desc.Shape = shape
		sigs[compare.DescriptorSig(desc)] = struct{}{}
	}
	if len(sigs) != len(shapes) {
		t.Fatalf("descriptor sig count=%d, want %d distinct sigs", len(sigs), len(shapes))
	}
}
