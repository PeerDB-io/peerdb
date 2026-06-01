package exceptions

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// InvalidSequenceRe go-mysql-org returns "invalid sequence X != Y" (or "invalid compressed sequence X != Y"
// for the compressed protocol) when the packet sequence byte is out of sync — always transient, safe to retry.
var InvalidSequenceRe = regexp.MustCompile(`invalid (compressed )?sequence \d+ != \d+`)

type MySQLIncompatibleColumnTypeError struct {
	TableName  string
	ColumnName string
	dataType   string
	qkind      string
	columnType byte
}

func NewMySQLIncompatibleColumnTypeError(
	tableName string, columnName string, columnType byte, dataType string, qkind string,
) *MySQLIncompatibleColumnTypeError {
	return &MySQLIncompatibleColumnTypeError{
		TableName:  tableName,
		ColumnName: columnName,
		dataType:   dataType,
		qkind:      qkind,
		columnType: columnType,
	}
}

func (e *MySQLIncompatibleColumnTypeError) Error() string {
	return fmt.Sprintf("Incompatible type for column %s in table %s, expect qkind %s but data is %s (mysql type %d)",
		e.ColumnName, e.TableName, e.qkind, e.dataType, e.columnType)
}

type MySQLUnsupportedBinlogRowMetadataError struct {
	SchemaName string
	TableName  string
}

func NewMySQLUnsupportedBinlogRowMetadataError(schema string, table string) *MySQLUnsupportedBinlogRowMetadataError {
	return &MySQLUnsupportedBinlogRowMetadataError{SchemaName: schema, TableName: table}
}

func (e *MySQLUnsupportedBinlogRowMetadataError) Error() string {
	return fmt.Sprintf("Detected binlog_row_metadata change from FULL to MINIMAL while processing %s.%s",
		e.SchemaName, e.TableName)
}

type MySQLUnsupportedDDLError struct {
	TableName string
}

func NewMySQLUnsupportedDDLError(tableName string) *MySQLUnsupportedDDLError {
	return &MySQLUnsupportedDDLError{TableName: tableName}
}

func (e *MySQLUnsupportedDDLError) Error() string {
	return fmt.Sprintf(
		"Detected position-shifting DDL on table %s but binlog_row_metadata is not supported by this MySQL version.", e.TableName)
}

// MySQLGeometryParseError wraps go-geos WKB parse failures so they can be
// classified as MySQL-source errors without string-matching at the alerting layer.
// The underlying message comes from go-geos C code and is not unique to MySQL on its own.
type MySQLGeometryParseError struct {
	error
}

func NewMySQLGeometryParseError(err error) *MySQLGeometryParseError {
	return &MySQLGeometryParseError{err}
}

func (e *MySQLGeometryParseError) Error() string {
	return "failed to parse MySQL geometry WKB: " + e.error.Error()
}

func (e *MySQLGeometryParseError) Unwrap() error {
	return e.error
}

type MySQLExecuteError struct {
	error
	Retryable bool
}

func NewMySQLExecuteError(err error) *MySQLExecuteError {
	if errors.Is(err, context.DeadlineExceeded) {
		return &MySQLExecuteError{err, true}
	}

	if recordHeaderError, ok := errors.AsType[tls.RecordHeaderError](err); ok {
		if recordHeaderError.Msg == "first record does not look like a TLS handshake" {
			return &MySQLExecuteError{err, true}
		}
	}

	if strings.Contains(err.Error(), mysql.ErrBadConn.Error()) {
		return &MySQLExecuteError{err, true}
	}

	if InvalidSequenceRe.MatchString(err.Error()) {
		return &MySQLExecuteError{err, true}
	}

	return &MySQLExecuteError{err, false}
}

func (e *MySQLExecuteError) Error() string {
	return "MySQL execute error: " + e.error.Error()
}

func (e *MySQLExecuteError) Unwrap() error {
	return e.error
}

// MySQLStaleConnectionError indicates that no events (rows or heartbeats) have arrived
// from the MySQL master in longer than the configured staleness window.
type MySQLStaleConnectionError struct {
	Since           time.Duration
	HeartbeatPeriod time.Duration
}

func NewMySQLStaleConnectionError(since, heartbeatPeriod time.Duration) *MySQLStaleConnectionError {
	return &MySQLStaleConnectionError{Since: since, HeartbeatPeriod: heartbeatPeriod}
}

func (e *MySQLStaleConnectionError) Error() string {
	return fmt.Sprintf("MySQL connection is stale: no events received in %v (heartbeat=%v)",
		e.Since, e.HeartbeatPeriod)
}
