package e2eshared

import (
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/PeerDB-io/peer-flow/model"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Suite interface {
	T() *testing.T
	Pool() *pgxpool.Pool
	Suffix() string
}

type RowSource interface {
	Suite
	GetRows(table, cols string) (*model.QRecordBatch, error)
}

func RunSuite[T any](t *testing.T, setup func(t *testing.T) T, teardown func(T)) {
	t.Helper()
	t.Parallel()

	// can be replaced with reflect.TypeFor[T]() in go 1.22
	typ := reflect.TypeOf((*T)(nil)).Elem()
	mcount := typ.NumMethod()
	for i := 0; i < mcount; i++ {
		m := typ.Method(i)
		if strings.HasPrefix(m.Name, "Test") {
			if m.Type.NumIn() == 1 && m.Type.NumOut() == 0 {
				t.Run(m.Name, func(subtest *testing.T) {
					subtest.Parallel()
					suite := setup(subtest)
					subtest.Cleanup(func() {
						teardown(suite)
					})
					m.Func.Call([]reflect.Value{reflect.ValueOf(suite)})
				})
			}
		}
	}
}

// ReadFileToBytes reads a file to a byte array.
func ReadFileToBytes(path string) ([]byte, error) {
	var ret []byte

	f, err := os.Open(path)
	if err != nil {
		return ret, fmt.Errorf("failed to open file: %w", err)
	}

	defer f.Close()

	ret, err = io.ReadAll(f)
	if err != nil {
		return ret, fmt.Errorf("failed to read file: %w", err)
	}

	return ret, nil
}

// checks if two QRecords are identical
func CheckQRecordEquality(t *testing.T, q model.QRecord, other model.QRecord) bool {
	t.Helper()

	if q.NumEntries != other.NumEntries {
		t.Logf("unequal entry count: %d != %d", q.NumEntries, other.NumEntries)
		return false
	}

	for i, entry := range q.Entries {
		otherEntry := other.Entries[i]
		if !entry.Equals(otherEntry) {
			t.Logf("entry %d: %v != %v", i, entry, otherEntry)
			return false
		}
	}

	return true
}

// Equals checks if two QRecordBatches are identical.
func CheckEqualRecordBatches(t *testing.T, q *model.QRecordBatch, other *model.QRecordBatch) bool {
	t.Helper()

	if q == nil || other == nil {
		t.Logf("q nil? %v, other nil? %v", q == nil, other == nil)
		return q == nil && other == nil
	}

	// First check simple attributes
	if q.NumRecords != other.NumRecords {
		// print num records
		t.Logf("q.NumRecords: %d", q.NumRecords)
		t.Logf("other.NumRecords: %d", other.NumRecords)
		return false
	}

	// Compare column names
	if !q.Schema.EqualNames(other.Schema) {
		t.Log("Column names are not equal")
		t.Logf("Schema 1: %v", q.Schema.GetColumnNames())
		t.Logf("Schema 2: %v", other.Schema.GetColumnNames())
		return false
	}

	// Compare records
	for i, record := range q.Records {
		if !CheckQRecordEquality(t, record, other.Records[i]) {
			t.Logf("Record %d is not equal", i)
			return false
		}
	}

	return true
}
