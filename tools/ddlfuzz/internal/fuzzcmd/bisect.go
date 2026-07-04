package fuzzcmd

import (
	"context"
	"errors"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/run"
)

var errBisectStop = errors.New("bisection stopped")

const (
	shapeStateDependent  = "state-dependent batch"
	shapeBudgetExhausted = "bisect budget exhausted"
)

func bisectBudget(n int) int { return 4*bitLen(n) + 8 }

func crashClass(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "oracle_timeout"
	}
	return "oracle_crash"
}

type bisector struct {
	submit       func(context.Context, []run.Case) error
	recordSingle func(run.Case, error)
	recordBatch  func([]run.Case, error, string)
	budget       int
}

// bisect is called with a batch known to have failed with err.
func (b *bisector) bisect(ctx context.Context, batch []run.Case, err error) {
	if len(batch) == 1 {
		b.recordSingle(batch[0], err)
		return
	}
	if b.budget <= 0 {
		b.recordBatch(batch, err, shapeBudgetExhausted)
		return
	}
	mid := len(batch) / 2
	errL := b.trySubmit(ctx, batch[:mid])
	errR := b.trySubmit(ctx, batch[mid:])
	if ctx.Err() != nil || errors.Is(errL, errBisectStop) || errors.Is(errR, errBisectStop) {
		return
	}
	if errL == nil && errR == nil {
		b.recordBatch(batch, err, shapeStateDependent)
		return
	}
	if errL != nil {
		b.bisect(ctx, batch[:mid], errL)
	}
	if errR != nil {
		// Even when the left subtree exhausted the budget, recursing here files
		// this half loudly (the budget check precedes any submission).
		b.bisect(ctx, batch[mid:], errR)
	}
}

func (b *bisector) trySubmit(ctx context.Context, batch []run.Case) error {
	b.budget--
	return b.submit(ctx, batch)
}
