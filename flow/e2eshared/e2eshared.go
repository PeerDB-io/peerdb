package e2eshared

import (
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

type Suite interface {
	Teardown()
}

func RunSuite[T Suite](t *testing.T, setup func(t *testing.T) T) {
	t.Helper()
	t.Parallel()

	typ := reflect.TypeFor[T]()
	mcount := typ.NumMethod()
	for i := range mcount {
		m := typ.Method(i)
		if strings.HasPrefix(m.Name, "Test") {
			if m.Type.NumIn() == 1 && m.Type.NumOut() == 0 {
				t.Run(m.Name, func(subtest *testing.T) {
					subtest.Parallel()
					suite := setup(subtest)
					subtest.Cleanup(func() {
						suite.Teardown()
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
func CheckQRecordEquality(t *testing.T, q []qvalue.QValue, other []qvalue.QValue) bool {
	t.Helper()

	if len(q) != len(other) {
		t.Logf("unequal entry count: %d != %d", len(q), len(other))
		return false
	}

	for i, entry := range q {
		otherEntry := other[i]
		if !entry.Equals(otherEntry) {
			t.Logf("entry %d: %T %v != %T %v", i, entry.Value, entry, otherEntry.Value, otherEntry)
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
	if len(q.Records) != len(other.Records) {
		// print num records
		t.Logf("q.NumRecords: %d", len(q.Records))
		t.Logf("other.NumRecords: %d", len(other.Records))
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
