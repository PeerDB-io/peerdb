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
	"sync"
	"time"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/run"
	_ "modernc.org/sqlite"
)

// Store is safe for concurrent use: multiple oracle-proc goroutines (across
// both engines) call Add and Count.
//
// Budget (bytes, 0 = unlimited) caps the SQLite corpus file set
// (corpus.db plus SQLite auxiliary files). Over budget, Add refuses new entries
// (retention is a growth heuristic, not correctness -- coverage accumulation
// and findings are unaffected).
type Store struct {
	Path   string
	Budget int64

	db   *sql.DB
	mu   sync.Mutex
	full uint64
}

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
	return nil
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
		if bytes+int64(len(c.SQL)) > s.Budget {
			s.full++
			return false, nil
		}
	}

	res, err := s.db.Exec(
		`INSERT OR IGNORE INTO corpus(engine, hash, sql_mode, origin, added_at, sql)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		engine,
		hash,
		strconv.FormatUint(c.SQLMode, 10),
		run.OriginName(c.Origin),
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
	return rows > 0, nil
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
	rows, err := s.db.Query(`SELECT sql, sql_mode, origin FROM corpus WHERE engine = ? ORDER BY id`, engine)
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
		var mode, origin string
		if err := rows.Scan(&c.SQL, &mode, &origin); err != nil {
			return nil, err
		}
		c.SQLMode, _ = strconv.ParseUint(mode, 10, 64)
		c.Engine = engineID
		c.Origin = originID(origin)
		c.BaseTier = run.BaseTierOld
		out = append(out, c)
	}
	return out, rows.Err()
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
