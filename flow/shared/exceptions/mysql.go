package exceptions

import (
	"context"
	"errors"
	"fmt"
	"strings"
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

type MySQLStreamingTransientError struct {
	error
}

func (e *MySQLStreamingTransientError) Error() string {
	return "MySQL streaming transient error: " + e.error.Error()
}

func (e *MySQLStreamingTransientError) Unwrap() error {
	return e.error
}

func AsMySQLStreamingTransientError(err error) *MySQLStreamingTransientError {
	if err == nil {
		return nil
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return &MySQLStreamingTransientError{err}
	}

	if strings.Contains(err.Error(), "first record does not look like a TLS handshake") {
		return &MySQLStreamingTransientError{err}
	}

	return nil
}
