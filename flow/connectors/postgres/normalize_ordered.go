package connpostgres

import (
	"fmt"
	"slices"
	"strings"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

func (n *normalizeStmtGenerator) generateOrderedStatement(dstTable string, batchID int64) string {
	schema, d := n.tableSchemaMapping[dstTable], n.dst[dstTable]
	parsed, _ := common.ParseTableIdentifier(dstTable)
	table := parsed.String()
	whole := schema.IsReplicaIdentityFull && !d.keyUnique || len(schema.PrimaryKeyColumns) == 0
	toast := map[string]bool{}
	for _, cols := range n.unchangedToastColumnsMap[dstTable] {
		for _, col := range strings.Split(cols, ",") {
			toast[col] = true
		}
	}

	sel := []string{"x._peerdb_record_type AS t"}
	var newDefs, oldDefs, cols, vals, sets, image, partial, keyNull []string
	var keyCols []int
	quoted := make([]string, len(schema.Columns))
	field := func(prefix string, i int) (string, string) {
		col := schema.Columns[i]
		typ, v := n.columnTypeToPg(schema, col), fmt.Sprintf("_peerdb_apply.r.%s%d", prefix, i)
		if col.Type == "json" || col.Type == "jsonb" {
			sel = append(sel, fmt.Sprintf("(%s.%s #>> '{}')::%s AS %s%d", prefix, quoted[i], col.Type, prefix, i))
			return quoted[i] + " jsonb", v
		}
		sel = append(sel, fmt.Sprintf("%s.%s AS %s%d", prefix, quoted[i], prefix, i))
		return quoted[i] + " " + typ, v
	}
	for i, col := range schema.Columns {
		q := common.QuoteIdentifier(col.Name)
		quoted[i] = q
		def, v := field("n", i)
		newDefs, cols, vals = append(newDefs, def), append(cols, q), append(vals, v)
		partial = append(partial, fmt.Sprintf("(%s IS NULL OR x.%s::text = %s::text)", v, q, v))
		if toast[col.Name] {
			sel = append(sel, fmt.Sprintf("x._peerdb_data ? %s AS p%d", utils.QuoteLiteral(col.Name), i))
			v = fmt.Sprintf("CASE WHEN _peerdb_apply.r.p%d THEN %s ELSE dst.%s END", i, v, q)
		}
		if !slices.Contains(d.identity, q) {
			sets = append(sets, q+" = "+v)
		}
		if whole || slices.Contains(schema.PrimaryKeyColumns, col.Name) {
			def, v := field("o", i)
			oldDefs, keyCols = append(oldDefs, def), append(keyCols, i)
			image = append(image, fmt.Sprintf("x.%s::text IS NOT DISTINCT FROM %s::text", q, v))
			keyNull = append(keyNull, v+" IS NULL")
		}
	}
	match := func(alias string, prefix string) string {
		m := make([]string, 0, len(keyCols))
		for _, i := range keyCols {
			m = append(m, fmt.Sprintf("%s.%s = _peerdb_apply.r.%s%d", alias, quoted[i], prefix, i))
		}
		return strings.Join(m, " AND ")
	}
	one := func(where []string) string {
		return fmt.Sprintf("(dst.tableoid, dst.ctid) = (SELECT x.tableoid, x.ctid FROM %s x WHERE %s LIMIT 1)",
			table, strings.Join(where, " AND "))
	}

	deleted := ""
	if c := n.peerdbCols.SyncedAtColName; c != "" {
		cols, vals = append(cols, common.QuoteIdentifier(c)), append(vals, "CURRENT_TIMESTAMP")
		sets, deleted = append(sets, common.QuoteIdentifier(c)+" = CURRENT_TIMESTAMP"), ","+common.QuoteIdentifier(c)+" = CURRENT_TIMESTAMP"
	}
	insert := func(soft string, where string) string {
		v := vals
		if c := n.peerdbCols.SoftDeleteColName; c != "" {
			v = append(slices.Clone(vals), soft)
		}
		ins := fmt.Sprintf("INSERT INTO %s (%s) OVERRIDING SYSTEM VALUE SELECT %s", table, strings.Join(cols, ","), strings.Join(v, ","))
		if where != "" {
			ins += fmt.Sprintf(" WHERE NOT EXISTS (SELECT FROM %s x WHERE %s)", table, where)
		}
		return ins + ";"
	}
	update := func(where string) string {
		return fmt.Sprintf("UPDATE %s dst SET %s WHERE %s;", table, strings.Join(sets, ","), where)
	}
	del := func(where string) string {
		return fmt.Sprintf("DELETE FROM %s dst WHERE %s;", table, where)
	}
	if c := n.peerdbCols.SoftDeleteColName; c != "" {
		cols, sets = append(cols, common.QuoteIdentifier(c)), append(sets, common.QuoteIdentifier(c)+" = FALSE")
		del = func(where string) string {
			return fmt.Sprintf("UPDATE %s dst SET %s = TRUE%s WHERE %s; IF NOT FOUND THEN %s END IF;", table,
				common.QuoteIdentifier(c), deleted, where, insert("TRUE", ""))
		}
	}

	body := fmt.Sprintf("IF r.t = 2 THEN %s ELSE IF r.t = 1 THEN %s END IF; IF r.t = 0 OR NOT FOUND THEN %s END IF; END IF;",
		del(one(image)), update(one(image)), insert("FALSE", ""))
	if !whole {
		body = fmt.Sprintf("IF r.t = 2 THEN IF %s THEN %s ELSE %s END IF; ELSE IF r.t = 1 THEN %s END IF; "+
			"IF r.t = 0 OR NOT FOUND THEN %s IF NOT FOUND THEN %s END IF; END IF; END IF;",
			strings.Join(keyNull, " OR "), del(one(partial)), del(match("dst", "o")), update(match("dst", "o")),
			insert("FALSE", match("x", "n")), update(match("dst", "n")))
	}
	block := fmt.Sprintf(`#variable_conflict use_column
<<_peerdb_apply>> DECLARE r record; BEGIN
FOR r IN SELECT %s FROM %s.%s x, jsonb_to_record(x._peerdb_data) n(%s),
jsonb_to_record(x._peerdb_data || coalesce(x._peerdb_match_data, '{}')) o(%s)
WHERE x._peerdb_batch_id = %d AND x._peerdb_destination_table_name = %s ORDER BY x._peerdb_timestamp
LOOP %s END LOOP; END`, strings.Join(sel, ","),
		n.metadataSchema, n.rawTableName, strings.Join(newDefs, ","), strings.Join(oldDefs, ","),
		batchID, utils.QuoteLiteral(dstTable), body)
	tag := "$_peerdb_apply$"
	for strings.Contains(block, tag) {
		tag = "$_" + tag[1:]
	}
	return "DO " + tag + block + tag
}
