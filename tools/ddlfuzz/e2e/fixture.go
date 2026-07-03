//go:build ddlfuzz

package e2e

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/go-mysql-org/go-mysql/client"
)

const fixtureMySQL = "CREATE TABLE `fixture` (\n" +
	"  `id` bigint NOT NULL,\n" +
	"  `c_int` int,\n" +
	"  `c_tiny` tinyint(1),\n" +
	"  `c_dec` decimal(12,4),\n" +
	"  `c_dbl` double,\n" +
	"  `c_vchar` varchar(64),\n" +
	"  `c_text` text,\n" +
	"  `c_vbin` varbinary(32),\n" +
	"  `c_blob` blob,\n" +
	"  `c_date` date,\n" +
	"  `c_dt` datetime(3),\n" +
	"  `c_ts` timestamp(6) NULL,\n" +
	"  `c_time` time,\n" +
	"  `c_year` year,\n" +
	"  `c_enum` enum('a','b'),\n" +
	"  `c_set` set('x','y'),\n" +
	"  `c_json` json,\n" +
	"  `c_bit` bit(8),\n" +
	"  `c_geom` geometry,\n" +
	"  `first` int,\n" +
	"  `after` int,\n" +
	"  `period` int,\n" +
	"  `system` int,\n" +
	"  `vector` int,\n" +
	"  `back``tick` int,\n" +
	"  `имя_utf8` int,\n" +
	"  PRIMARY KEY (`id`)\n" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"

const fixtureMariaDB = "CREATE TABLE `fixture` (\n" +
	"  `id` bigint NOT NULL,\n" +
	"  `c_int` int,\n" +
	"  `c_tiny` tinyint(1),\n" +
	"  `c_dec` decimal(12,4),\n" +
	"  `c_dbl` double,\n" +
	"  `c_vchar` varchar(64),\n" +
	"  `c_text` text,\n" +
	"  `c_vbin` varbinary(32),\n" +
	"  `c_blob` blob,\n" +
	"  `c_date` date,\n" +
	"  `c_dt` datetime(3),\n" +
	"  `c_ts` timestamp(6) NULL,\n" +
	"  `c_time` time,\n" +
	"  `c_year` year,\n" +
	"  `c_enum` enum('a','b'),\n" +
	"  `c_set` set('x','y'),\n" +
	"  `c_json` json,\n" +
	"  `c_bit` bit(8),\n" +
	"  `c_geom` geometry,\n" +
	"  `first` int,\n" +
	"  `after` int,\n" +
	"  `period` int,\n" +
	"  `system` int,\n" +
	"  `vector` int,\n" +
	"  `back``tick` int,\n" +
	"  `имя_utf8` int,\n" +
	"  `c_uuid` uuid,\n" +
	"  `c_inet4` inet4,\n" +
	"  `c_inet6` inet6,\n" +
	"  `c_vec` vector(4),\n" +
	"  PRIMARY KEY (`id`)\n" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"

var FreshNames = []string{
	"n1", "n2", "n3", "n4", "n5", "n6", "n7", "n8",
	"first2", "after2", "period2", "system2", "vector2",
	"new`tick", "spelled name", "имя2",
}

var baseTypeVocabMySQL = []string{
	"int", "bigint", "tinyint(1)", "decimal(10,2)", "double", "varchar(32)", "text",
	"varbinary(16)", "blob", "date", "datetime(3)", "timestamp(6) NULL", "time", "year",
	"enum('a','b')", "set('x','y')", "json", "bit(8)", "geometry",
}

var baseTypeVocabMariaDB = append(slices.Clone(baseTypeVocabMySQL), "uuid", "inet4", "inet6", "vector(4)")

type resetDecision struct {
	Needed bool
	Why    string
}

func fixtureCreateSQL(isMariaDB bool) string {
	if isMariaDB {
		return fixtureMariaDB
	}
	return fixtureMySQL
}

func typeVocab(isMariaDB bool) []string {
	if isMariaDB {
		return slices.Clone(baseTypeVocabMariaDB)
	}
	return slices.Clone(baseTypeVocabMySQL)
}

func setupEngineSchemas(ctx context.Context, ec engineConfig, workers int) error {
	admin, err := client.Connect(ec.Addr, rootUser, rootPassword, "")
	if err != nil {
		return err
	}
	defer admin.Close()

	for i := 1; i <= workers; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		schema := schemaName(i)
		if _, err := admin.Execute("DROP DATABASE IF EXISTS " + quoteIdent(schema)); err != nil {
			return fmt.Errorf("drop schema %s: %w", schema, err)
		}
		if _, err := admin.Execute("CREATE DATABASE " + quoteIdent(schema) + " DEFAULT CHARACTER SET utf8mb4"); err != nil {
			return fmt.Errorf("create schema %s: %w", schema, err)
		}
		conn, err := client.Connect(ec.Addr, rootUser, rootPassword, schema)
		if err != nil {
			return fmt.Errorf("connect schema %s: %w", schema, err)
		}
		if _, err := conn.Execute(fixtureCreateSQL(ec.IsMariaDB)); err != nil {
			_ = conn.Close()
			return fmt.Errorf("create fixture %s: %w", schema, err)
		}
		_ = conn.Close()
	}
	return nil
}

func resetFixture(conn *client.Conn, isMariaDB bool, currentTable string) error {
	if currentTable == "" {
		currentTable = fixtureTable
	}
	if _, err := conn.Execute("DROP TABLE IF EXISTS " + quoteIdent(currentTable)); err != nil {
		return err
	}
	if currentTable != fixtureTable {
		if _, err := conn.Execute("DROP TABLE IF EXISTS " + quoteIdent(fixtureTable)); err != nil {
			return err
		}
	}
	_, err := conn.Execute(fixtureCreateSQL(isMariaDB))
	return err
}

func shouldReset(after snapshot, currentTable string, casesSinceReset int) resetDecision {
	if currentTable != fixtureTable {
		return resetDecision{Needed: true, Why: "table-renamed"}
	}
	if len(after) > 60 {
		return resetDecision{Needed: true, Why: "too-many-columns"}
	}
	if len(after) < 15 {
		return resetDecision{Needed: true, Why: "too-few-columns"}
	}
	if casesSinceReset >= 32 {
		return resetDecision{Needed: true, Why: "periodic"}
	}
	lost := 0
	for _, name := range []string{"first", "after", "period", "system", "vector", "back`tick", "имя_utf8"} {
		if _, ok := after[name]; !ok {
			lost++
		}
	}
	if lost >= 3 {
		return resetDecision{Needed: true, Why: "adversarial-columns-lost"}
	}
	return resetDecision{}
}

func markerSQL(caseID string) string {
	return "DROP TABLE IF EXISTS " + quoteIdent("__m_"+caseID)
}

func controlDropSQL(table string) string {
	if table == "" {
		table = fixtureTable
	}
	return "DROP TABLE IF EXISTS " + quoteIdent(table)
}

func canonicalColumns(s snapshot) []string {
	cols := columnsByOrdinal(s)
	if len(cols) == 0 {
		return []string{"id", "c_int", "c_dec"}
	}
	return cols
}

func generatedRenameSQL(seq uint64) string {
	return fmt.Sprintf("RENAME TABLE %s TO %s", quoteIdent(fixtureTable), quoteIdent(fmt.Sprintf("fixture_r_%d", seq)))
}

func simpleFallbackDDL(seq uint64, s snapshot) string {
	name := fallbackFreshName(seq, s)
	if seq%5 == 0 {
		cols := canonicalColumns(s)
		if len(cols) > 15 {
			return "ALTER TABLE " + quoteIdent(fixtureTable) + " DROP COLUMN " + quoteIdent(cols[len(cols)-1])
		}
	}
	var suffix string
	if seq%3 == 0 {
		suffix = " AFTER " + quoteIdent("first")
	}
	return "ALTER TABLE " + quoteIdent(fixtureTable) + " ADD COLUMN " + quoteIdent(name) + " int" + suffix
}

func simpleFallbackDDLForMode(seq uint64, s snapshot, modeEntry string) string {
	up := strings.ToUpper(modeEntry)
	if strings.Contains(up, "ORACLE") || strings.Contains(up, "MSSQL") {
		return "ALTER TABLE fixture ADD COLUMN " + fallbackFreshName(seq, s) + " int"
	}
	return simpleFallbackDDL(seq, s)
}

func fallbackFreshName(seq uint64, s snapshot) string {
	name := FreshNames[int(seq)%len(FreshNames)]
	for {
		if _, exists := s[name]; !exists {
			break
		}
		name = fmt.Sprintf("n%d_%d", int(seq)%8+1, seq)
		if _, exists := s[name]; !exists {
			break
		}
	}
	return name
}

func isFixtureRename(stmt string) bool {
	up := strings.ToUpper(strings.TrimSpace(stmt))
	return strings.HasPrefix(up, "RENAME TABLE")
}
