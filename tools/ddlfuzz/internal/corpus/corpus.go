package corpus

import (
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/run"
)

// Store is safe for concurrent use: multiple oracle-proc goroutines (across
// both engines) call Add and Count.
type Store struct {
	Root  string
	mu    sync.Mutex
	index map[string]struct{}
}

type Meta struct {
	SQLMode uint64 `json:"sql_mode"`
	Origin  string `json:"origin,omitempty"`
	AddedAt string `json:"added_at,omitempty"`
}

func Open(root string) (*Store, error) {
	s := &Store{Root: root, index: map[string]struct{}{}}
	for _, engine := range []string{"mysql", "mariadb"} {
		dir := filepath.Join(root, engine)
		entries, err := os.ReadDir(dir)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		for _, ent := range entries {
			if ent.Type().IsRegular() && len(ent.Name()) == 40 {
				s.index[engine+"/"+ent.Name()] = struct{}{}
			}
		}
	}
	return s, nil
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
	key := engine + "/" + hash
	s.mu.Lock()
	if _, ok := s.index[key]; ok {
		s.mu.Unlock()
		return false, nil
	}
	s.index[key] = struct{}{}
	s.mu.Unlock()
	dir := filepath.Join(s.Root, engine)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return false, err
	}
	if err := writeAtomic(filepath.Join(dir, hash), c.SQL); err != nil {
		return false, err
	}
	meta := Meta{SQLMode: c.SQLMode, Origin: run.OriginName(c.Origin), AddedAt: time.Now().UTC().Format(time.RFC3339)}
	b, _ := json.MarshalIndent(meta, "", "  ")
	b = append(b, '\n')
	if err := writeAtomic(filepath.Join(dir, hash+".meta.json"), b); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) Count(engine string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for key := range s.index {
		if len(key) > len(engine)+1 && key[:len(engine)+1] == engine+"/" {
			n++
		}
	}
	return n
}

func writeAtomic(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		_ = os.Remove(name)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(name)
		return err
	}
	return os.Rename(name, path)
}
