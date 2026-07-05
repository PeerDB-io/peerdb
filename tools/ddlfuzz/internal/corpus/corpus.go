package corpus

import (
	"crypto/sha1"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/run"
	_ "modernc.org/sqlite"
)

// Store is safe for concurrent use: multiple oracle-proc goroutines (across
// both engines) call Add and Count.
//
// Budget (bytes, 0 = unlimited) caps the SQLite corpus file set
// (corpus.db plus SQLite auxiliary files). Over budget, Add evicts noise-tier
// rows first, then largest-per-feature duplicates; it refuses the new entry
// only when eviction cannot free enough (retention is a growth heuristic, not
// correctness -- coverage accumulation and findings are unaffected).
type Store struct {
	Path   string
	Budget int64

	db   *sql.DB
	mu   sync.Mutex
	full uint64
	// freedCredit is logical bytes deleted by eviction: SQLite reuses their
	// pages before growing the file, so they offset the on-disk size until
	// inserts consume them.
	freedCredit  int64
	evictedRows  uint64
	evictedBytes uint64
}

// FeatureKeyFunc maps a stored corpus row to its coarse behavior feature.
// internal/compare owns the implementation; corpus takes it as a parameter so
// the dependency points outward.
type FeatureKeyFunc func(engine uint8, sqlMode uint64, sqlText []byte) uint64

func Open(path string, budget int64) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &Store{Path: path, Budget: budget, db: db}
	if err := s.init(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) init() error {
	for _, stmt := range []string{
		`PRAGMA busy_timeout = 5000`,
		`PRAGMA journal_mode = WAL`,
		`PRAGMA synchronous = NORMAL`,
		`CREATE TABLE IF NOT EXISTS corpus (
				id INTEGER PRIMARY KEY,
				engine TEXT NOT NULL,
				hash TEXT NOT NULL,
				sql_mode TEXT NOT NULL,
				origin TEXT,
				signal TEXT NOT NULL DEFAULT 'noise',
				feature TEXT,
				added_at TEXT NOT NULL,
				sql BLOB NOT NULL,
				UNIQUE(engine, hash)
			)`,
		`CREATE INDEX IF NOT EXISTS corpus_engine_id ON corpus(engine, id)`,
	} {
		if _, err := s.db.Exec(stmt); err != nil {
			return fmt.Errorf("corpus init %q: %w", stmt, err)
		}
	}
	if err := s.ensureColumn("signal", `ALTER TABLE corpus ADD COLUMN signal TEXT NOT NULL DEFAULT 'noise'`); err != nil {
		return err
	}
	if err := s.ensureColumn("feature", `ALTER TABLE corpus ADD COLUMN feature TEXT`); err != nil {
		return err
	}
	return nil
}

func (s *Store) ensureColumn(column, addDDL string) error {
	rows, err := s.db.Query(`PRAGMA table_info(corpus)`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, typ string
		var notnull, pk int
		var dflt any
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			return err
		}
		if name == column {
			return rows.Err()
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	_, err = s.db.Exec(addDDL)
	return err
}

func (s *Store) Close() error {
	return s.db.Close()
}

func Hash(sql []byte, sqlMode uint64) string {
	h := sha1.New()
	_, _ = h.Write(sql)
	_, _ = h.Write([]byte{0})
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], sqlMode)
	_, _ = h.Write(buf[:])
	return hex.EncodeToString(h.Sum(nil))
}

func (s *Store) Add(c run.Case) (bool, error) {
	return s.AddSignal(c, c.Signal)
}

func (s *Store) AddSignal(c run.Case, signal uint8) (bool, error) {
	return s.AddSignalFeature(c, signal, "")
}

// AddSignalFeature stamps the row's coarse behavior feature (decimal text of
// the uint64 key) so the budget-eviction duplicate tier can partition rows;
// "" leaves the column NULL, which eviction treats as never-a-duplicate.
func (s *Store) AddSignalFeature(c run.Case, signal uint8, feature string) (bool, error) {
	engine := run.EngineName(c.Engine)
	hash := Hash(c.SQL, c.SQLMode)
	s.mu.Lock()
	defer s.mu.Unlock()

	exists, err := s.existsLocked(engine, hash)
	if err != nil {
		return false, err
	}
	if exists {
		return false, nil
	}
	if s.Budget > 0 {
		bytes, err := s.bytesLocked()
		if err != nil {
			return false, err
		}
		if over := bytes - s.freedCredit + int64(len(c.SQL)) - s.Budget; over > 0 {
			freedEnough, err := s.evictLocked(over)
			if err != nil {
				return false, err
			}
			if !freedEnough {
				s.full++
				return false, nil
			}
		}
	}

	var featureVal any
	if feature != "" {
		featureVal = feature
	}
	res, err := s.db.Exec(
		`INSERT OR IGNORE INTO corpus(engine, hash, sql_mode, origin, signal, feature, added_at, sql)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		engine,
		hash,
		strconv.FormatUint(c.SQLMode, 10),
		run.OriginName(c.Origin),
		run.SignalName(signal),
		featureVal,
		time.Now().UTC().Format(time.RFC3339),
		c.SQL,
	)
	if err != nil {
		return false, err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return true, nil
	}
	if rows > 0 && s.freedCredit > 0 {
		s.freedCredit = max(0, s.freedCredit-int64(len(c.SQL)))
	}
	return rows > 0, nil
}

// Eviction candidate order: the whole noise tier goes first, largest rows
// first; then rows that duplicate a smaller retained row's
// feature. Rows with an unknown feature are never duplicates. Selection
// completes before any delete, so an unsatisfiable need evicts nothing and
// the Add is refused instead.
const (
	evictNoiseQuery = `SELECT id, length(sql) FROM corpus WHERE signal = 'noise' ORDER BY length(sql) DESC, id`
	evictDupQuery   = `SELECT id, len FROM (
			SELECT id, length(sql) AS len,
			       row_number() OVER (PARTITION BY engine, feature ORDER BY length(sql), id) AS rn
			FROM corpus WHERE feature IS NOT NULL AND signal != 'noise'
		) WHERE rn > 1 ORDER BY len DESC, id`
)

func (s *Store) evictLocked(need int64) (bool, error) {
	var ids []int64
	var freed int64
	for _, query := range []string{evictNoiseQuery, evictDupQuery} {
		if freed >= need {
			break
		}
		var err error
		ids, freed, err = s.evictCandidatesLocked(query, ids, need, freed)
		if err != nil {
			return false, err
		}
	}
	if freed < need {
		return false, nil
	}
	if err := s.deleteIDsLocked(ids); err != nil {
		return false, err
	}
	s.freedCredit += freed
	s.evictedRows += uint64(len(ids))
	s.evictedBytes += uint64(freed)
	return true, nil
}

func (s *Store) evictCandidatesLocked(query string, ids []int64, need, freed int64) ([]int64, int64, error) {
	rows, err := s.db.Query(query)
	if err != nil {
		return ids, freed, err
	}
	defer rows.Close()
	for freed < need && rows.Next() {
		var id, size int64
		if err := rows.Scan(&id, &size); err != nil {
			return ids, freed, err
		}
		ids = append(ids, id)
		freed += size
	}
	return ids, freed, rows.Err()
}

func (s *Store) deleteIDsLocked(ids []int64) error {
	for start := 0; start < len(ids); start += 256 {
		batch := ids[start:min(start+256, len(ids))]
		args := make([]any, len(batch))
		for i, id := range batch {
			args[i] = id
		}
		placeholders := strings.Repeat(",?", len(batch))[1:]
		if _, err := s.db.Exec(`DELETE FROM corpus WHERE id IN (`+placeholders+`)`, args...); err != nil {
			return err
		}
	}
	return nil
}

// Evicted reports rows and logical bytes removed by budget eviction.
func (s *Store) Evicted() (rows, bytes uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.evictedRows, s.evictedBytes
}

func (s *Store) existsLocked(engine, hash string) (bool, error) {
	var one int
	err := s.db.QueryRow(`SELECT 1 FROM corpus WHERE engine = ? AND hash = ?`, engine, hash).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// Bytes returns the current on-disk size of the SQLite corpus file set.
func (s *Store) Bytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	bytes, _ := s.bytesLocked()
	return bytes
}

func (s *Store) bytesLocked() (int64, error) {
	var total int64
	for _, suffix := range []string{"", "-wal", "-shm"} {
		info, err := os.Stat(s.Path + suffix)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return 0, err
		}
		total += info.Size()
	}
	return total, nil
}

// SkippedFull returns how many Adds were refused by the budget.
func (s *Store) SkippedFull() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.full
}

func (s *Store) Count(engine string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int
	if err := s.db.QueryRow(`SELECT count(*) FROM corpus WHERE engine = ?`, engine).Scan(&n); err != nil {
		return 0
	}
	return n
}

func (s *Store) AllCases(engine string) ([]run.Case, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query(`SELECT sql, sql_mode, origin, signal FROM corpus WHERE engine = ? ORDER BY id`, engine)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	engineID, err := run.EngineID(engine)
	if err != nil {
		return nil, err
	}
	var out []run.Case
	for rows.Next() {
		var c run.Case
		var mode, origin, signal string
		if err := rows.Scan(&c.SQL, &mode, &origin, &signal); err != nil {
			return nil, err
		}
		c.SQLMode, _ = strconv.ParseUint(mode, 10, 64)
		c.Engine = engineID
		c.Origin = originID(origin)
		c.BaseTier = run.BaseTierOld
		c.Signal = run.SignalID(signal)
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *Store) StampExistingNoise() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`UPDATE corpus SET signal = 'noise' WHERE signal IS NULL OR signal = ''`)
	return err
}

type DistillStats struct {
	Scanned    int   `json:"scanned"`
	Kept       int   `json:"kept"`
	Deleted    int   `json:"deleted"`
	Skipped    int   `json:"skipped"` // unparseable engine/sql_mode text; row left in place
	BytesFreed int64 `json:"bytes_freed"`
}

// DistillNoise rewrites the noise tier: per (engine, feature) the smallest
// row survives (ties: lowest id) and the rest are deleted.
// Survivors get their feature stamped, recording the distill key alongside
// the AddSignalFeature stamps on new arrivals. Behavior tiers are untouched.
// Call Vacuum afterwards to shrink the file.
func (s *Store) DistillNoise(fn FeatureKeyFunc) (DistillStats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	type rowMeta struct {
		id   int64
		size int64
		key  string
	}
	var stats DistillStats
	var metas []rowMeta
	best := map[string]int{}

	rows, err := s.db.Query(`SELECT id, engine, sql_mode, sql FROM corpus WHERE signal = 'noise' ORDER BY id`)
	if err != nil {
		return stats, err
	}
	for rows.Next() {
		var id int64
		var engineText, modeText string
		var sqlBytes []byte
		if err := rows.Scan(&id, &engineText, &modeText, &sqlBytes); err != nil {
			rows.Close()
			return stats, err
		}
		stats.Scanned++
		mode, modeErr := strconv.ParseUint(modeText, 10, 64)
		engine, engineErr := run.EngineID(engineText)
		if modeErr != nil || engineErr != nil {
			stats.Skipped++
			continue
		}
		key := engineText + "\x00" + strconv.FormatUint(fn(engine, mode, sqlBytes), 10)
		metas = append(metas, rowMeta{id: id, size: int64(len(sqlBytes)), key: key})
		if i, ok := best[key]; !ok || metas[i].size > int64(len(sqlBytes)) {
			best[key] = len(metas) - 1
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return stats, err
	}
	rows.Close()

	keepers := make(map[int64]string, len(best))
	for key, i := range best {
		keepers[metas[i].id] = key[strings.IndexByte(key, 0)+1:]
	}
	var doomed []int64
	for _, m := range metas {
		if _, ok := keepers[m.id]; ok {
			continue
		}
		doomed = append(doomed, m.id)
		stats.BytesFreed += m.size
	}
	if err := s.deleteIDsLocked(doomed); err != nil {
		return stats, err
	}
	stats.Deleted = len(doomed)
	stats.Kept = len(keepers)
	for id, feature := range keepers {
		if _, err := s.db.Exec(`UPDATE corpus SET feature = ? WHERE id = ?`, feature, id); err != nil {
			return stats, err
		}
	}
	return stats, nil
}

// Vacuum rebuilds the database file so distill deletions actually shrink it.
func (s *Store) Vacuum() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`VACUUM`)
	return err
}

func originID(origin string) uint8 {
	switch origin {
	case "gen":
		return run.OriginGen
	case "mut":
		return run.OriginMut
	case "golden":
		return run.OriginGolden
	case "replay":
		return run.OriginReplay
	default:
		return run.OriginCorpus
	}
}

// RandomSQL returns one retained SQL input for engine, selected by random id.
func (s *Store) RandomSQL(engine string, rng *rand.Rand) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var minID, maxID sql.NullInt64
	if err := s.db.QueryRow(`SELECT min(id), max(id) FROM corpus WHERE engine = ?`, engine).Scan(&minID, &maxID); err != nil {
		return nil, false, err
	}
	if !minID.Valid || !maxID.Valid {
		return nil, false, nil
	}

	start := minID.Int64
	if rng != nil && maxID.Int64 > minID.Int64 {
		span := uint64(maxID.Int64 - minID.Int64 + 1)
		start += int64(rng.Uint64() % span)
	}
	sqlBytes, ok, err := s.sqlAtOrAfterLocked(engine, start)
	if err != nil || ok {
		return sqlBytes, ok, err
	}
	return s.sqlAtOrAfterLocked(engine, minID.Int64)
}

func (s *Store) sqlAtOrAfterLocked(engine string, id int64) ([]byte, bool, error) {
	var sqlBytes []byte
	err := s.db.QueryRow(
		`SELECT sql FROM corpus WHERE engine = ? AND id >= ? ORDER BY id LIMIT 1`,
		engine,
		id,
	).Scan(&sqlBytes)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return sqlBytes, true, nil
}
