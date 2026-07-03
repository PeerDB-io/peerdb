package exec

import (
	"testing"
	"time"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/run"
)

func TestWorkerContainsPanic(t *testing.T) {
	w := NewWorker(1, time.Second, func([]byte, uint64, bool) (string, error) {
		panic("boom")
	})
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
	res := w.RunBatch([]run.Case{{SQL: []byte("ALTER TABLE t ADD c INT")}})
	if len(res) != 1 || res[0].Panic == nil || !res[0].Panic.Timeout {
		t.Fatalf("timeout not reported: %+v", res)
	}
}
