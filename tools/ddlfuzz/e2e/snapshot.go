package e2e

import (
	"fmt"
	"strings"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/e2echeck"
	"github.com/go-mysql-org/go-mysql/client"
)

type colRow = e2echeck.ColRow
type snapshot = e2echeck.Snapshot
type columnChange = e2echeck.ColumnChange
type delta = e2echeck.Delta
type renameSummary = e2echeck.RenameSummary

func readSnapshot(conn *client.Conn, schema, table string) (snapshot, error) {
	query := fmt.Sprintf(
		"SELECT column_name, ordinal_position, column_type, is_nullable, column_key, numeric_precision, numeric_scale "+
			"FROM information_schema.columns WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
		quoteLiteral(schema), quoteLiteral(table),
	)
	rs, err := conn.Execute(query)
	if err != nil {
		return nil, err
	}
	if rs == nil || rs.Resultset == nil {
		return snapshot{}, nil
	}
	defer rs.Close()

	out := make(snapshot, rs.RowNumber())
	for i := range rs.RowNumber() {
		name, err := rs.GetString(i, 0)
		if err != nil {
			return nil, err
		}
		name = strings.Clone(name)
		ordinal, err := rs.GetInt(i, 1)
		if err != nil {
			return nil, err
		}
		columnType, err := rs.GetString(i, 2)
		if err != nil {
			return nil, err
		}
		columnType = strings.Clone(columnType)
		isNullable, err := rs.GetString(i, 3)
		if err != nil {
			return nil, err
		}
		isNullable = strings.Clone(isNullable)
		columnKey, err := rs.GetString(i, 4)
		if err != nil {
			return nil, err
		}
		columnKey = strings.Clone(columnKey)
		prec, err := nullInt(rs.Resultset, i, 5)
		if err != nil {
			return nil, err
		}
		scale, err := nullInt(rs.Resultset, i, 6)
		if err != nil {
			return nil, err
		}
		out[name] = colRow{
			Name:       name,
			Ordinal:    int(ordinal),
			ColumnType: columnType,
			IsNullable: isNullable,
			ColumnKey:  columnKey,
			NumPrec:    prec,
			NumScale:   scale,
		}
	}
	return out, nil
}

func readTables(conn *client.Conn, schema string) (map[string]bool, error) {
	query := fmt.Sprintf("SELECT table_name FROM information_schema.tables WHERE table_schema = %s", quoteLiteral(schema))
	rs, err := conn.Execute(query)
	if err != nil {
		return nil, err
	}
	if rs == nil || rs.Resultset == nil {
		return map[string]bool{}, nil
	}
	defer rs.Close()
	out := make(map[string]bool, rs.RowNumber())
	for i := range rs.RowNumber() {
		name, err := rs.GetString(i, 0)
		if err != nil {
			return nil, err
		}
		name = strings.Clone(name)
		out[name] = true
	}
	return out, nil
}

func nullInt(rs interface {
	IsNull(int, int) (bool, error)
	GetInt(int, int) (int64, error)
}, row, col int) (*int64, error) {
	isNull, err := rs.IsNull(row, col)
	if err != nil {
		return nil, err
	}
	if isNull {
		return nil, nil
	}
	v, err := rs.GetInt(row, col)
	if err != nil {
		return nil, err
	}
	return &v, nil
}

func diffSnapshots(before, after snapshot) delta {
	return e2echeck.DiffSnapshots(before, after)
}

func columnsByOrdinal(s snapshot) []string {
	return s.ColumnsByOrdinal()
}

func tableSetDelta(before, after map[string]bool) (dropped, added []string) {
	return e2echeck.TableSetDelta(before, after)
}
