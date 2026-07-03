//go:build ddlfuzz

package e2e

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync/atomic"
	"time"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/corpus"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/gen"
	"github.com/go-mysql-org/go-mysql/client"
)

type engineRuntime struct {
	ec        engineConfig
	stateDir  string
	workers   int
	target    uint64
	smoke     bool
	runNonce  string
	stats     *Stats
	sides     *sideChannels
	errs      chan<- error
	queues    map[string]*expectQueue
	queueChan chan queueItem
	corpus    *corpus.Store
	issued    atomic.Uint64
}

type workerState struct {
	rt              *engineRuntime
	id              int
	schema          string
	conn            *client.Conn
	rng             *rand.Rand
	currentTable    string
	casesSinceReset int
}

func (rt *engineRuntime) pending() int {
	total := 0
	for _, q := range rt.queues {
		total += q.Len()
	}
	return total
}

func (rt *engineRuntime) nextSeq() (uint64, bool) {
	seq := rt.issued.Add(1)
	if rt.target > 0 && seq > rt.target {
		return seq, false
	}
	return seq, true
}

func (rt *engineRuntime) runWorker(ctx context.Context, id int, done chan<- struct{}) {
	defer func() {
		if r := recover(); r != nil {
			rt.reportHarness(fmt.Errorf("worker %d panic: %v", id, r))
		}
		done <- struct{}{}
	}()

	schema := schemaName(id)
	conn, err := client.Connect(rt.ec.Addr, rootUser, rootPassword, schema)
	if err != nil {
		rt.reportHarness(fmt.Errorf("worker %d connect: %w", id, err))
		return
	}
	defer conn.Close()

	ws := &workerState{
		rt:           rt,
		id:           id,
		schema:       schema,
		conn:         conn,
		rng:          rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(id)<<32|uint64(len(rt.ec.Name)))),
		currentTable: fixtureTable,
	}

	for {
		select {
		case <-ctx.Done():
			return
		case item := <-rt.queueChan:
			ws.runOne(ctx, 0, &item)
			continue
		default:
		}

		seq, ok := rt.nextSeq()
		if !ok {
			return
		}
		for rt.pending() > 256 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Millisecond):
			}
		}
		ws.runOne(ctx, seq, nil)
	}
}

func (rt *engineRuntime) reportHarness(err error) {
	select {
	case rt.errs <- harnessError{Engine: rt.ec.Name, Err: err}:
	default:
	}
}

func (ws *workerState) runOne(ctx context.Context, seq uint64, item *queueItem) {
	if ctx.Err() != nil {
		return
	}
	if seq == 0 {
		seq = uint64(time.Now().UnixNano())
	}
	caseID := fmt.Sprintf("%s_%d_%d", ws.rt.runNonce, ws.id, seq)
	q := ws.rt.queues[ws.schema]
	if _, err := ws.conn.Execute("SET SESSION sql_mode = ''"); err != nil {
		ws.rt.reportHarness(fmt.Errorf("worker %d reset marker sql_mode %s: %w", ws.id, caseID, err))
		return
	}
	marker := markerSQL(caseID)
	q.Push(caseExpect{Kind: expectMarker, WorkerID: ws.id, CaseID: caseID, Submitted: marker})
	if _, err := ws.conn.Execute(marker); err != nil {
		ws.rt.reportHarness(fmt.Errorf("worker %d marker %s: %w", ws.id, caseID, err))
		return
	}

	modeEntry := ws.paletteEntry(seq, item)
	if _, err := ws.conn.Execute("SET SESSION sql_mode = " + quoteLiteral(modeEntry)); err != nil {
		ws.rt.reportHarness(fmt.Errorf("worker %d set sql_mode %q: %w", ws.id, modeEntry, err))
		return
	}
	readback, err := ws.sqlModeReadback()
	if err != nil {
		ws.rt.reportHarness(fmt.Errorf("worker %d readback sql_mode: %w", ws.id, err))
		return
	}
	relevant := relevantFromReadback(readback)

	beforeTables, err := readTables(ws.conn, ws.schema)
	if err != nil {
		ws.rt.reportHarness(fmt.Errorf("worker %d before tables: %w", ws.id, err))
		return
	}
	before, err := readSnapshot(ws.conn, ws.schema, fixtureTable)
	if err != nil {
		ws.rt.reportHarness(fmt.Errorf("worker %d before snapshot: %w", ws.id, err))
		return
	}

	stmt, source := ws.statement(seq, before, item, modeEntry)
	if _, err := ws.conn.Execute(stmt); err != nil {
		ws.rt.stats.IncCases(ws.rt.ec.Name)
		ws.rt.stats.IncExecReject(ws.rt.ec.Name)
		_ = ws.rt.sides.AppendExecReject(execRejectRecord{
			Engine:      ws.rt.ec.Name,
			Source:      source,
			Statement:   stmt,
			SQLModeName: readback,
			Error:       err.Error(),
		})
		if item != nil {
			_ = completeQueueItem(ws.rt.stateDir, *item, queueResult{Sig: item.Sig, Result: "exec-reject", Details: err.Error()})
			ws.rt.stats.IncQueueDone(ws.rt.ec.Name)
		}
		return
	}

	afterTables, err := readTables(ws.conn, ws.schema)
	if err != nil {
		ws.rt.reportHarness(fmt.Errorf("worker %d after tables: %w", ws.id, err))
		return
	}
	after, err := readSnapshot(ws.conn, ws.schema, fixtureTable)
	if err != nil {
		ws.rt.reportHarness(fmt.Errorf("worker %d after snapshot: %w", ws.id, err))
		return
	}
	exp := caseExpect{
		Kind:            expectDDL,
		WorkerID:        ws.id,
		CaseID:          caseID,
		Submitted:       stmt,
		SQLModeName:     readback,
		SQLModeRelevant: relevant,
		Before:          before,
		After:           after,
		BeforeTables:    beforeTables,
		AfterTables:     afterTables,
		Source:          source,
		Queue:           item,
	}
	q.Push(exp)
	ws.rt.stats.IncCases(ws.rt.ec.Name)
	ws.rt.stats.HitPalette(ws.rt.ec.Name, modeEntry)
	ws.casesSinceReset++

	ws.updateCurrentTable(beforeTables, afterTables)
	if decision := shouldReset(after, ws.currentTable, ws.casesSinceReset); decision.Needed {
		if err := ws.resetWithExpectations(q); err != nil {
			ws.rt.reportHarness(fmt.Errorf("worker %d reset (%s): %w", ws.id, decision.Why, err))
			return
		}
	}
}

func (ws *workerState) paletteEntry(seq uint64, item *queueItem) string {
	if item != nil && item.SQLModeName != "" {
		return item.SQLModeName
	}
	palette := sqlModePalette(ws.rt.ec.IsMariaDB)
	if len(palette) == 0 {
		return ""
	}
	idx := int((seq - 1) % uint64(len(palette)))
	return palette[idx]
}

func (ws *workerState) sqlModeReadback() (string, error) {
	rs, err := ws.conn.Execute("SELECT @@SESSION.sql_mode")
	if err != nil {
		return "", err
	}
	if rs == nil || rs.Resultset == nil || rs.RowNumber() == 0 {
		return "", nil
	}
	defer rs.Close()
	mode, err := rs.GetString(0, 0)
	if err != nil {
		return "", err
	}
	return strings.Clone(mode), nil
}

func (ws *workerState) statement(seq uint64, before snapshot, item *queueItem, modeEntry string) (string, string) {
	if item != nil {
		return item.Statement, "queue"
	}
	if ws.rt.smoke && seq <= uint64(len(sqlModePalette(ws.rt.ec.IsMariaDB))) {
		return simpleFallbackDDLForMode(seq, before, modeEntry), "gen"
	}
	choice := ws.rng.IntN(100)
	if choice < 70 {
		stmt, panicked := ws.generateConstrained(before)
		if panicked {
			ws.rt.stats.MarkGeneratorPanic(ws.rt.ec.Name)
			return simpleFallbackDDLForMode(seq, before, modeEntry), "gen"
		}
		if strings.TrimSpace(stmt) != "" {
			return stmt, "gen"
		}
		return simpleFallbackDDLForMode(seq, before, modeEntry), "gen"
	}
	if choice < 95 {
		if stmt, ok := pickCorpusRewrite(ws.rt.corpus, ws.rt.ec, ws.rng, before); ok {
			return stmt, "corpus"
		}
		return simpleFallbackDDLForMode(seq, before, modeEntry), "gen"
	}
	return generatedRenameSQL(seq), "gen"
}

func (ws *workerState) generateConstrained(before snapshot) (stmt string, panicked bool) {
	defer func() {
		if recover() != nil {
			panicked = true
			stmt = ""
		}
	}()
	v := gen.Vocab{
		Table:      fixtureTable,
		Columns:    canonicalColumns(before),
		FreshNames: FreshNames,
		Types:      typeVocab(ws.rt.ec.IsMariaDB),
		IsMariaDB:  ws.rt.ec.IsMariaDB,
	}
	p := gen.Profile{
		HeadsAlterOnly:    true,
		NoAlterRenameTo:   true,
		NoConvertCharset:  true,
		RenameTableWeight: 0.05,
		MaxSpecs:          3,
	}
	return gen.GenerateConstrained(ws.rng, v, p), false
}

func (ws *workerState) updateCurrentTable(before, after map[string]bool) {
	if after[fixtureTable] {
		ws.currentTable = fixtureTable
		return
	}
	dropped, added := tableSetDelta(before, after)
	if len(dropped) == 1 && dropped[0] == fixtureTable && len(added) == 1 {
		ws.currentTable = added[0]
	}
}

func (ws *workerState) resetWithExpectations(q *expectQueue) error {
	if _, err := ws.conn.Execute("SET SESSION sql_mode = ''"); err != nil {
		return err
	}
	dropCurrent := controlDropSQL(ws.currentTable)
	q.Push(caseExpect{Kind: expectControl, WorkerID: ws.id, Submitted: dropCurrent})
	if _, err := ws.conn.Execute(dropCurrent); err != nil {
		return err
	}
	if ws.currentTable != fixtureTable {
		dropFixture := controlDropSQL(fixtureTable)
		q.Push(caseExpect{Kind: expectControl, WorkerID: ws.id, Submitted: dropFixture})
		if _, err := ws.conn.Execute(dropFixture); err != nil {
			return err
		}
	}
	create := fixtureCreateSQL(ws.rt.ec.IsMariaDB)
	q.Push(caseExpect{Kind: expectControl, WorkerID: ws.id, Submitted: create})
	if _, err := ws.conn.Execute(create); err != nil {
		return err
	}
	ws.currentTable = fixtureTable
	ws.casesSinceReset = 0
	ws.rt.stats.IncReset(ws.rt.ec.Name)
	return nil
}
