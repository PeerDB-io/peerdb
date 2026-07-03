package exec

import (
	"fmt"
	"runtime/debug"
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
	curSeq   atomic.Uint64
	curDead  atomic.Int64
	parser   ParserFunc
	deadline time.Duration
}

func NewWorker(id int, deadline time.Duration, parser ParserFunc) *Worker {
	if deadline <= 0 {
		deadline = 100 * time.Millisecond
	}
	if parser == nil {
		parser = DefaultParser
	}
	return &Worker{id: id, deadline: deadline, parser: parser}
}

func (w *Worker) RunBatch(b []run.Case) []Result {
	out := make([]Result, len(b))
	for i, c := range b {
		out[i] = w.runOne(c)
	}
	return out
}

func (w *Worker) runOne(c run.Case) Result {
	seq := w.curSeq.Add(1)
	w.curDead.Store(time.Now().Add(w.deadline).UnixNano())
	defer w.curDead.Store(0)

	done := make(chan Result, 1)
	go func() {
		done <- callParser(w.parser, c)
	}()

	timer := time.NewTimer(w.deadline)
	defer timer.Stop()
	select {
	case r := <-done:
		w.curSeq.CompareAndSwap(seq, seq+1)
		return r
	case <-timer.C:
		w.curSeq.CompareAndSwap(seq, seq+1)
		return Result{Panic: &PanicInfo{Value: "ddlfuzz parser timeout", Timeout: true}}
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
