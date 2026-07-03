package e2e

import (
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"github.com/go-mysql-org/go-mysql/client"
)

type colRow struct {
	Name       string        `json:"name"`
	Ordinal    int           `json:"ordinal_position"`
	ColumnType string        `json:"column_type"`
	IsNullable string        `json:"is_nullable"`
	ColumnKey  string        `json:"column_key"`
	NumPrec    sql.NullInt64 `json:"numeric_precision"`
	NumScale   sql.NullInt64 `json:"numeric_scale"`
}

type snapshot map[string]colRow

type columnChange struct {
	Name   string `json:"name"`
	Before colRow `json:"before"`
	After  colRow `json:"after"`
}

type delta struct {
	Added   []colRow        `json:"added"`
	Dropped []colRow        `json:"dropped"`
	Changed []columnChange  `json:"changed"`
	Renamed []renameSummary `json:"renamed,omitempty"`
}

type renameSummary struct {
	Old string `json:"old"`
	New string `json:"new"`
}

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
}, row, col int) (sql.NullInt64, error) {
	isNull, err := rs.IsNull(row, col)
	if err != nil {
		return sql.NullInt64{}, err
	}
	if isNull {
		return sql.NullInt64{}, nil
	}
	v, err := rs.GetInt(row, col)
	if err != nil {
		return sql.NullInt64{}, err
	}
	return sql.NullInt64{Int64: v, Valid: true}, nil
}

func diffSnapshots(before, after snapshot) delta {
	var d delta
	for name, row := range after {
		prev, ok := before[name]
		if !ok {
			d.Added = append(d.Added, row)
			continue
		}
		if prev.ColumnType != row.ColumnType || prev.IsNullable != row.IsNullable ||
			prev.Ordinal != row.Ordinal || prev.ColumnKey != row.ColumnKey ||
			prev.NumPrec != row.NumPrec || prev.NumScale != row.NumScale {
			d.Changed = append(d.Changed, columnChange{Name: name, Before: prev, After: row})
		}
	}
	for name, row := range before {
		if _, ok := after[name]; !ok {
			d.Dropped = append(d.Dropped, row)
		}
	}
	slices.SortFunc(d.Added, func(a, b colRow) int { return a.Ordinal - b.Ordinal })
	slices.SortFunc(d.Dropped, func(a, b colRow) int { return a.Ordinal - b.Ordinal })
	slices.SortFunc(d.Changed, func(a, b columnChange) int { return a.After.Ordinal - b.After.Ordinal })
	return d
}

func (s snapshot) columnsByOrdinal() []string {
	rows := make([]colRow, 0, len(s))
	for _, row := range s {
		rows = append(rows, row)
	}
	slices.SortFunc(rows, func(a, b colRow) int { return a.Ordinal - b.Ordinal })
	out := make([]string, len(rows))
	for i, row := range rows {
		out[i] = row.Name
	}
	return out
}

func (d delta) empty() bool {
	return len(d.Added) == 0 && len(d.Dropped) == 0 && len(d.Changed) == 0 && len(d.Renamed) == 0
}

func tableSetDelta(before, after map[string]bool) (dropped, added []string) {
	for name := range before {
		if !after[name] {
			dropped = append(dropped, name)
		}
	}
	for name := range after {
		if !before[name] {
			added = append(added, name)
		}
	}
	slices.Sort(dropped)
	slices.Sort(added)
	return dropped, added
}
