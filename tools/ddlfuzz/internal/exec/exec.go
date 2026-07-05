package exec

import (
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/run"
)

type ParserFunc func(sql []byte, sqlMode uint64, isMariaDB bool) (string, error)

type PanicInfo struct {
	Value   string
	Stack   []byte
	Timeout bool
}

type Result struct {
	Sig   string
	Err   error
	Panic *PanicInfo
}

type Worker struct {
	id       int
	parser   ParserFunc
	deadline time.Duration

	mu     sync.Mutex
	gen    *generation
	ticker *time.Ticker
	closed bool
}

type generation struct {
	reqCh    chan []run.Case
	resCh    chan []Result
	progress atomic.Int64
	res      []Result
}

func NewWorker(id int, deadline time.Duration, parser ParserFunc) *Worker {
	if deadline <= 0 {
		deadline = 100 * time.Millisecond
	}
	if parser == nil {
		parser = DefaultParser
	}
	w := &Worker{id: id, deadline: deadline, parser: parser}
	w.gen = w.newGeneration()
	return w
}

func (w *Worker) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return
	}
	w.closed = true
	w.closeGeneration(w.gen)
	if w.ticker != nil {
		w.ticker.Stop()
		w.ticker = nil
	}
}

func (w *Worker) RunBatch(b []run.Case) []Result {
	w.mu.Lock()
	defer w.mu.Unlock()

	out := make([]Result, len(b))
	for offset := 0; offset < len(b); {
		g := w.gen
		g.progress.Store(-1)
		w.drainTicker()
		g.reqCh <- b[offset:]

		lastProgress := int64(-1)
		lastObserved := time.Now()
	wait:
		for {
			select {
			case res := <-g.resCh:
				copy(out[offset:], res)
				return out
			case tick := <-w.watchdog().C:
				progress := g.progress.Load()
				if progress != lastProgress {
					lastProgress = progress
					lastObserved = tick
					continue
				}
				if progress < 0 || tick.Sub(lastObserved) < w.deadline {
					continue
				}

				copy(out[offset:offset+int(progress)], g.res[:progress])
				out[offset+int(progress)] = Result{
					Panic: &PanicInfo{Value: "ddlfuzz parser timeout", Timeout: true},
				}
				w.closeGeneration(g)
				w.gen = w.newGeneration()
				offset += int(progress) + 1
				break wait
			}
		}
	}
	return out
}

func (w *Worker) newGeneration() *generation {
	g := &generation{
		reqCh: make(chan []run.Case),
		resCh: make(chan []Result, 1),
	}
	g.progress.Store(-1)
	go func() {
		for batch := range g.reqCh {
			if cap(g.res) < len(batch) {
				g.res = make([]Result, len(batch))
			} else {
				g.res = g.res[:len(batch)]
				clear(g.res)
			}
			for i, c := range batch {
				g.progress.Store(int64(i))
				g.res[i] = callParser(w.parser, c)
			}
			g.resCh <- g.res
		}
	}()
	return g
}

func (w *Worker) closeGeneration(g *generation) {
	if g == nil {
		return
	}
	close(g.reqCh)
}

func (w *Worker) watchdog() *time.Ticker {
	if w.ticker != nil {
		return w.ticker
	}
	period := w.deadline / 2
	if period <= 0 {
		period = w.deadline
	}
	if period <= 0 {
		period = time.Nanosecond
	}
	w.ticker = time.NewTicker(period)
	return w.ticker
}

func (w *Worker) drainTicker() {
	if w.ticker == nil {
		return
	}
	for {
		select {
		case <-w.ticker.C:
		default:
			return
		}
	}
}

func callParser(parser ParserFunc, c run.Case) (r Result) {
	defer func() {
		if p := recover(); p != nil {
			r.Panic = &PanicInfo{Value: fmt.Sprint(p), Stack: debug.Stack()}
		}
	}()
	sig, err := parser(c.SQL, c.SQLMode, c.Engine == run.EngineMariaDB)
	r.Sig = sig
	r.Err = err
	return r
}
