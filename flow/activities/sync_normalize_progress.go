package activities

import (
	"context"
	"sync"
)

// syncNormalizeProgress connects one sync worker to one normalize worker
// through two batch IDs. syncedBatchID is the last batch sync has staged, and
// normalizedBatchID is the last batch normalize has applied. The batches between
// them are pending work. Sync raises syncedBatchID and waits while the gap is at
// the buffer limit. Normalize raises normalizedBatchID, waits while the gap is
// empty, and then takes every pending batch up to syncedBatchID in one range.
type syncNormalizeProgress struct {
	progressChanged   chan struct{}
	mu                sync.Mutex
	syncedBatchID     int64
	normalizedBatchID int64
}

func newSyncNormalizeProgress(syncedBatchID, normalizedBatchID int64) *syncNormalizeProgress {
	return &syncNormalizeProgress{
		syncedBatchID: syncedBatchID, normalizedBatchID: normalizedBatchID, progressChanged: make(chan struct{}),
	}
}

func (p *syncNormalizeProgress) RecordSyncedBatch(batchID int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if batchID > p.syncedBatchID {
		p.syncedBatchID = batchID
		p.notifyLocked()
	}
}

func (p *syncNormalizeProgress) RecordNormalizedBatch(batchID int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if batchID > p.normalizedBatchID {
		p.normalizedBatchID = batchID
		p.notifyLocked()
	}
}

func (p *syncNormalizeProgress) notifyLocked() {
	close(p.progressChanged)
	p.progressChanged = make(chan struct{})
}

func (p *syncNormalizeProgress) SyncedBatchID() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.syncedBatchID
}

func (p *syncNormalizeProgress) NormalizedBatchID() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.normalizedBatchID
}

func (p *syncNormalizeProgress) WaitForUnnormalizedBatch(ctx context.Context, gapHooks ...func()) (int64, error) {
	for {
		p.mu.Lock()
		if err := ctx.Err(); err != nil {
			p.mu.Unlock()
			return 0, err
		}
		if p.syncedBatchID > p.normalizedBatchID {
			batchID := p.syncedBatchID
			p.mu.Unlock()
			return batchID, nil
		}
		// Capture the channel while still holding the lock used to check batch IDs.
		// A later update closes this channel, so it wakes us even if select starts afterward.
		progressChanged := p.progressChanged
		p.mu.Unlock()
		for _, hook := range gapHooks {
			hook()
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-progressChanged:
		}
	}
}

func (p *syncNormalizeProgress) WaitForNormalizeCatchup(ctx context.Context, backpressureThreshold int64, gapHooks ...func()) error {
	for {
		p.mu.Lock()
		if err := ctx.Err(); err != nil {
			p.mu.Unlock()
			return err
		}
		if p.syncedBatchID-p.normalizedBatchID < backpressureThreshold {
			p.mu.Unlock()
			return nil
		}
		progressChanged := p.progressChanged
		p.mu.Unlock()
		for _, hook := range gapHooks {
			hook()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-progressChanged:
		}
	}
}
