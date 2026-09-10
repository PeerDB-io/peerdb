package common

import (
	"context"

	"golang.org/x/sync/errgroup"
)

type sendMsg[D, RT any] struct {
	decodedChunk D
	resumeToken  RT
}

// PullRecordsWorkerPool implements an ordered, batched concurent worker pool for use with
// PullRecords. The canonical use-case is for decoding events (type E) coming from the
// source database into decoded events (type D), with an associated resumeToken
// to thread through to the Send function along with events (type RT). We use the term
// "chunk" to refer to batches of events in this worker, to avoid nomenclature clashes
// elsewhere with batch sizes.
//
// Items of work ("events") arrive through AddItem, and are chunked into instances of WorkerFunc
// that are spun up (up to a max of `Concurrency` in parallel). The result of WorkerFunc
// is passed through to Send, along with the resumeToken from the last call to AddItem
// that added an event that was part of this chunk. Order is maintained through all calls;
// events that arrive earlier will be part of earlier chunks than events that arrived later,
// and chunks will maintain this AddItem-implied order on their way to Send(). Within a chunk,
// the slice of events is also ordered from earliest to latest.
type PullRecordsWorkerPool[E, D, RT any] struct {
	WorkerFunc func(events []E) (D, error)
	Send       func(ctx context.Context, items D, resumeToken RT) error

	lastToken       RT
	sem             chan struct{}
	sender          chan chan sendMsg[D, RT]
	workerCtx       context.Context //nolint:containedctx // errgroup ctx must reach the worker in Flush.
	ctxCancel       context.CancelFunc
	eg              *errgroup.Group
	inProgressChunk []E

	Concurrency, ChunkSize int

	closed bool
}

// Buffered channel size for channels to pass records to the send function (see below).
// Used as a min; if the passed-in `Concurrency` is higher, that's used instead.
const workerBufferedChanSize = 10

// Init initializes this PullRecordsWorker. Once Wait() has returned, Init()
// can be called again to reuse this struct.
func (p *PullRecordsWorkerPool[E, D, RT]) Init(ctx context.Context) {
	p.inProgressChunk = make([]E, 0, p.ChunkSize)
	p.closed = false
	p.sem = make(chan struct{}, max(1, p.Concurrency))
	p.sender = make(chan chan sendMsg[D, RT], max(workerBufferedChanSize, p.Concurrency))
	ctx, p.ctxCancel = context.WithCancel(ctx) //nolint:gosec // G118: cancelled in Wait.
	p.eg, ctx = errgroup.WithContext(ctx)
	p.workerCtx = ctx

	// Start the send loop.
	p.eg.Go(func() error {
		for {
			select {
			case sendChan, ok := <-p.sender:
				if !ok {
					return nil
				}
				select {
				case msg := <-sendChan:
					if err := p.Send(ctx, msg.decodedChunk, msg.resumeToken); err != nil {
						return err
					}
				case <-ctx.Done():
					return ctx.Err()
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})
}

// AddItem adds one event to be chunked and passed to a worker. Not safe for concurrent
// use.
func (p *PullRecordsWorkerPool[E, D, RT]) AddItem(ctx context.Context, event E, token RT) error {
	p.inProgressChunk = append(p.inProgressChunk, event)
	p.lastToken = token
	if len(p.inProgressChunk) >= p.ChunkSize {
		return p.Flush(ctx)
	}
	return nil
}

// Flush schedules work for any in-flight events that were added by previous calls to AddItem,
// but were not dispatched to a worker yet. Not safe for concurrent use with AddItem.
func (p *PullRecordsWorkerPool[E, D, RT]) Flush(ctx context.Context) error {
	if len(p.inProgressChunk) == 0 {
		// Nothing to do.
		return nil
	}
	// Grab a slot in the semaphore.
	select {
	case p.sem <- struct{}{}:
		// Grabbed a slot.
		curChunk := p.inProgressChunk
		sendChan := make(chan sendMsg[D, RT])
		lastToken := p.lastToken
		select {
		case p.sender <- sendChan:
		case <-ctx.Done():
			p.ctxCancel()
			return ctx.Err()
		case <-p.workerCtx.Done():
			return nil
		}
		p.eg.Go(func() error {
			defer func() {
				// Release the slot.
				<-p.sem
			}()

			decoded, err := p.WorkerFunc(curChunk)
			if err != nil {
				return err
			}
			sendMsg := sendMsg[D, RT]{
				decodedChunk: decoded,
				resumeToken:  lastToken,
			}
			select {
			case sendChan <- sendMsg:
			case <-p.workerCtx.Done():
				return context.Cause(p.workerCtx)
			}
			return nil
		})
		p.inProgressChunk = make([]E, 0, p.ChunkSize)
		return nil
	case <-ctx.Done():
		p.ctxCancel()
		return ctx.Err()
	case <-p.workerCtx.Done():
		return context.Cause(p.workerCtx)
	}
}

// Wait waits for all in-progress work to finish, or for the passed-in context to be cancelled,
// whichever happens sooner.
func (p *PullRecordsWorkerPool[E, D, RT]) Wait(ctx context.Context) error {
	if p.closed {
		// Wait called a second time.
		return nil
	}
	// Gracefully drain.
	close(p.sender)
	p.closed = true
	defer p.ctxCancel()
	wgWaiter := make(chan error)
	go func() {
		wgWaiter <- p.eg.Wait()
	}()
	select {
	case err := <-wgWaiter:
		return err
	case <-ctx.Done():
		p.ctxCancel()
		return <-wgWaiter
	}
}
