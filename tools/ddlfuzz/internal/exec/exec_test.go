package exec

import (
	"runtime"
	"testing"
	"time"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/run"
)

func TestWorkerContainsPanic(t *testing.T) {
	w := NewWorker(1, time.Second, func([]byte, uint64, bool) (string, error) {
		panic("boom")
	})
	t.Cleanup(w.Close)
	res := w.RunBatch([]run.Case{{SQL: []byte("ALTER TABLE t ADD c INT")}})
	if len(res) != 1 || res[0].Panic == nil || res[0].Panic.Timeout {
		t.Fatalf("panic not contained: %+v", res)
	}
}

func TestWorkerTimeout(t *testing.T) {
	w := NewWorker(1, 10*time.Millisecond, func([]byte, uint64, bool) (string, error) {
		time.Sleep(200 * time.Millisecond)
		return "", nil
	})
	t.Cleanup(w.Close)
	res := w.RunBatch([]run.Case{{SQL: []byte("ALTER TABLE t ADD c INT")}})
	if len(res) != 1 || res[0].Panic == nil || !res[0].Panic.Timeout {
		t.Fatalf("timeout not reported: %+v", res)
	}
}

func TestWorkerBatchResumesAfterHungCase(t *testing.T) {
	w := NewWorker(1, 5*time.Millisecond, func(sql []byte, _ uint64, _ bool) (string, error) {
		if string(sql) == "hang" {
			select {}
		}
		return "sig:" + string(sql), nil
	})
	t.Cleanup(w.Close)

	res := w.RunBatch([]run.Case{
		{SQL: []byte("before")},
		{SQL: []byte("hang")},
		{SQL: []byte("after")},
	})
	if len(res) != 3 {
		t.Fatalf("got %d results, want 3", len(res))
	}
	if res[0].Sig != "sig:before" || res[0].Panic != nil {
		t.Fatalf("before result = %+v", res[0])
	}
	if res[1].Panic == nil || !res[1].Panic.Timeout {
		t.Fatalf("hang result = %+v", res[1])
	}
	if res[2].Sig != "sig:after" || res[2].Panic != nil {
		t.Fatalf("after result = %+v", res[2])
	}
}

func TestWorkerTwoHangsInOneBatch(t *testing.T) {
	w := NewWorker(1, 5*time.Millisecond, func(sql []byte, _ uint64, _ bool) (string, error) {
		switch string(sql) {
		case "hang1", "hang2":
			select {}
		default:
			return "sig:" + string(sql), nil
		}
	})
	t.Cleanup(w.Close)

	res := w.RunBatch([]run.Case{
		{SQL: []byte("ok1")},
		{SQL: []byte("hang1")},
		{SQL: []byte("ok2")},
		{SQL: []byte("hang2")},
		{SQL: []byte("ok3")},
	})
	if len(res) != 5 {
		t.Fatalf("got %d results, want 5", len(res))
	}
	for _, idx := range []int{0, 2, 4} {
		if res[idx].Sig == "" || res[idx].Panic != nil {
			t.Fatalf("result %d = %+v", idx, res[idx])
		}
	}
	for _, idx := range []int{1, 3} {
		if res[idx].Panic == nil || !res[idx].Panic.Timeout {
			t.Fatalf("result %d = %+v", idx, res[idx])
		}
	}
}

func TestWorkerGoroutineCountStableAcrossBatches(t *testing.T) {
	w := NewWorker(1, time.Second, func([]byte, uint64, bool) (string, error) {
		return "ok", nil
	})
	t.Cleanup(w.Close)

	batch := []run.Case{
		{SQL: []byte("a")},
		{SQL: []byte("b")},
		{SQL: []byte("c")},
	}
	before := runtime.NumGoroutine()
	for i := 0; i < 1000; i++ {
		res := w.RunBatch(batch)
		if len(res) != len(batch) {
			t.Fatalf("got %d results, want %d", len(res), len(batch))
		}
	}
	runtime.Gosched()
	after := runtime.NumGoroutine()
	if after > before+2 {
		t.Fatalf("goroutine count grew from %d to %d", before, after)
	}
}

func BenchmarkRunBatch(b *testing.B) {
	w := NewWorker(1, time.Second, func([]byte, uint64, bool) (string, error) {
		return "ok", nil
	})
	defer w.Close()

	batch := make([]run.Case, 1024)
	for i := range batch {
		batch[i].SQL = []byte("ALTER TABLE t ADD c INT")
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(batch)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res := w.RunBatch(batch)
		if len(res) != len(batch) {
			b.Fatalf("got %d results, want %d", len(res), len(batch))
		}
	}
}
