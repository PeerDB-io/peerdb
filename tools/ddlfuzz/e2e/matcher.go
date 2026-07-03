//go:build ddlfuzz

package e2e

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

type expectKind string

const (
	expectMarker  expectKind = "marker"
	expectDDL     expectKind = "ddl"
	expectControl expectKind = "control"
)

type caseExpect struct {
	Kind            expectKind
	WorkerID        int
	CaseID          string
	Submitted       string
	SQLModeName     string
	SQLModeRelevant uint64
	Before          snapshot
	After           snapshot
	BeforeTables    map[string]bool
	AfterTables     map[string]bool
	Source          string
	Queue           *queueItem
}

type expectQueue struct {
	mu    sync.Mutex
	cond  *sync.Cond
	items []caseExpect
}

func newExpectQueue() *expectQueue {
	q := &expectQueue{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *expectQueue) Push(exp caseExpect) {
	q.mu.Lock()
	q.items = append(q.items, exp)
	q.mu.Unlock()
	q.cond.Broadcast()
}

func (q *expectQueue) PopWait(ctx context.Context, timeout time.Duration) (caseExpect, error) {
	deadline := time.Now().Add(timeout)
	stop := context.AfterFunc(ctx, func() {
		q.cond.Broadcast()
	})
	defer stop()

	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.items) == 0 {
		if err := ctx.Err(); err != nil {
			return caseExpect{}, err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return caseExpect{}, fmt.Errorf("timed out waiting for matcher expectation")
		}
		timer := time.AfterFunc(remaining, func() {
			q.cond.Broadcast()
		})
		q.cond.Wait()
		timer.Stop()
	}
	exp := q.items[0]
	copy(q.items, q.items[1:])
	q.items = q.items[:len(q.items)-1]
	return exp, nil
}

func (q *expectQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

type matcher struct {
	ec       engineConfig
	stateDir string
	queues   map[string]*expectQueue
	stats    *Stats
	sides    *sideChannels
	errs     chan<- error
	syncer   *replication.BinlogSyncer
}

func startMatcher(ctx context.Context, ec engineConfig, stateDir string, queues map[string]*expectQueue, stats *Stats, sides *sideChannels, errs chan<- error) (*matcher, error) {
	pos, err := captureBinlogPosition(ec)
	if err != nil {
		return nil, err
	}
	syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID:         uint32(time.Now().UnixNano() & 0xffffffff),
		Flavor:           ec.Flavor,
		Host:             ec.Host,
		Port:             ec.Port,
		User:             rootUser,
		Password:         rootPassword,
		DisableRetrySync: true,
		UseDecimal:       true,
		ParseTime:        true,
		HeartbeatPeriod:  time.Minute,
	})
	stream, err := syncer.StartSync(pos)
	if err != nil {
		syncer.Close()
		return nil, err
	}
	m := &matcher{ec: ec, stateDir: stateDir, queues: queues, stats: stats, sides: sides, errs: errs, syncer: syncer}
	go m.run(ctx, stream)
	return m, nil
}

func (m *matcher) Close() {
	if m.syncer != nil {
		m.syncer.Close()
	}
}

func (m *matcher) run(ctx context.Context, stream *replication.BinlogStreamer) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		eventCtx, cancel := context.WithTimeout(ctx, time.Second)
		ev, err := stream.GetEvent(eventCtx)
		cancel()
		if err != nil {
			if ctx.Err() != nil || strings.Contains(err.Error(), "context deadline") {
				m.updatePending()
				continue
			}
			m.reportHarness(fmt.Errorf("binlog stream: %w", err))
			return
		}
		qe, ok := ev.Event.(*replication.QueryEvent)
		if !ok {
			continue
		}
		schema := string(qe.Schema)
		q := m.queues[schema]
		if q == nil {
			continue
		}
		exp, err := q.PopWait(ctx, 30*time.Second)
		if err != nil {
			m.reportHarness(fmt.Errorf("schema %s: %w", schema, err))
			return
		}
		m.updatePending()
		m.handleQueryEvent(ctx, ev, qe, exp)
	}
}

func (m *matcher) handleQueryEvent(ctx context.Context, ev *replication.BinlogEvent, qe *replication.QueryEvent, exp caseExpect) {
	defer func() {
		if r := recover(); r != nil {
			recordE2EFinding(m.stateDir, m.stats, findingInput{
				Class:       "e2e-panic",
				Engine:      m.ec,
				Statement:   []byte(exp.Submitted),
				SQLMode:     exp.SQLModeRelevant,
				SQLModeName: exp.SQLModeName,
				RawEvent:    slices.Clone(ev.RawData),
				StatusVars:  slices.Clone(qe.StatusVars),
				Meta: map[string]any{
					"panic": fmt.Sprint(r),
				},
			})
		}
	}()

	query := string(qe.Query)
	switch exp.Kind {
	case expectMarker:
		if !controlQueryMatches(query, exp.Submitted) {
			m.reportHarness(fmt.Errorf("marker mismatch schema=%s case=%s got=%q want=%q", string(qe.Schema), exp.CaseID, query, exp.Submitted))
			return
		}
		m.stats.IncMarker(m.ec.Name)
	case expectControl:
		if !controlQueryMatches(query, exp.Submitted) {
			m.reportHarness(fmt.Errorf("control mismatch schema=%s got=%q want=%q", string(qe.Schema), query, exp.Submitted))
			return
		}
		m.stats.IncControl(m.ec.Name)
	case expectDDL:
		checkLiveDDL(ctx, m.ec, m.stateDir, m.stats, m.sides, ev, qe, exp)
	}
}

func controlQueryMatches(got, want string) bool {
	return got == want || got == want+" /* generated by server */"
}

func (m *matcher) updatePending() {
	pending := 0
	for _, q := range m.queues {
		pending += q.Len()
	}
	m.stats.SetPending(m.ec.Name, pending)
}

func (m *matcher) reportHarness(err error) {
	select {
	case m.errs <- harnessError{Engine: m.ec.Name, Err: err}:
	default:
	}
}

func captureBinlogPosition(ec engineConfig) (gomysql.Position, error) {
	conn, err := client.Connect(ec.Addr, rootUser, rootPassword, "")
	if err != nil {
		return gomysql.Position{}, err
	}
	defer conn.Close()

	queries := []string{"SHOW BINARY LOG STATUS"}
	if ec.IsMariaDB {
		queries = []string{"SHOW BINLOG STATUS", "SHOW MASTER STATUS"}
	} else {
		queries = []string{"SHOW BINARY LOG STATUS", "SHOW MASTER STATUS"}
	}
	var lastErr error
	for _, query := range queries {
		rs, err := conn.Execute(query)
		if err != nil {
			lastErr = err
			continue
		}
		if rs == nil || rs.Resultset == nil || rs.RowNumber() == 0 {
			lastErr = fmt.Errorf("%s returned no rows", query)
			continue
		}
		defer rs.Close()
		name, err := rs.GetString(0, 0)
		if err != nil {
			return gomysql.Position{}, err
		}
		pos, err := rs.GetUint(0, 1)
		if err != nil {
			return gomysql.Position{}, err
		}
		return gomysql.Position{Name: strings.Clone(name), Pos: uint32(pos)}, nil
	}
	return gomysql.Position{}, lastErr
}
