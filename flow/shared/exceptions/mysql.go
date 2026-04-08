package exceptions

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
)

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

type MySQLStreamingError struct {
	error
	Retryable bool
}

func NewMySQLStreamingError(err error) *MySQLStreamingError {
	if errors.Is(err, context.DeadlineExceeded) {
		return &MySQLStreamingError{err, true}
	}

	var recordHeaderError tls.RecordHeaderError
	if errors.As(err, &recordHeaderError) {
		if recordHeaderError.Msg == "first record does not look like a TLS handshake" {
			return &MySQLStreamingError{err, true}
		}
	}

	if strings.Contains(err.Error(), mysql.ErrBadConn.Error()) {
		return &MySQLStreamingError{err, true}
	}

	return &MySQLStreamingError{err, false}
}

func (e *MySQLStreamingError) Error() string {
	return "MySQL streaming error: " + e.error.Error()
}

func (e *MySQLStreamingError) Unwrap() error {
	return e.error
}
