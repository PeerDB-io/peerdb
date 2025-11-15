package exceptions

import (
	"fmt"
)

type MySQLIncompatibleColumnTypeError struct {
	error
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
	error
}

func NewMySQLUnsupportedBinlogRowMetadataError() *MySQLUnsupportedBinlogRowMetadataError {
	return &MySQLUnsupportedBinlogRowMetadataError{}
}

func (e *MySQLUnsupportedBinlogRowMetadataError) Error() string {
	return fmt.Sprintf("Detected binlog_row_metadata change from FULL to MINIMAL")
}
