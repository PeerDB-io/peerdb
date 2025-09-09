package exceptions

import (
	"fmt"
)

type MySQLIncompatibleColumnTypeError struct {
	TableName  string
	ColumnName string
	dataType   string
	columnType byte
	qkind      string
}

func NewMySQLIncompatibleColumnTypeError(
	tableName string, columnName string, columnType byte, dataType string, qkind string,
) *MySQLIncompatibleColumnTypeError {
	return &MySQLIncompatibleColumnTypeError{
		TableName:  tableName,
		ColumnName: columnName,
		dataType:   dataType,
		columnType: columnType,
		qkind:      qkind,
	}
}

func (e MySQLIncompatibleColumnTypeError) Error() string {
	return fmt.Sprintf("Incompatible type for column %s in table %s, expect qkind %s but data is %s (mysql type %d)",
		e.ColumnName, e.TableName, e.qkind, e.dataType, e.columnType)
}
