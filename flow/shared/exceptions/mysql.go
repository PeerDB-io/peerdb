package exceptions

import "fmt"

type MySQLIncompatibleColumnTypeError struct {
	TableName  string
	ColumnName string
	columnType byte
	dataType   string
}

func NewIncompatibleColumnTypeError(
	tableName string, columnName string, columnType byte, dataType string) MySQLIncompatibleColumnTypeError {
	return MySQLIncompatibleColumnTypeError{
		TableName:  tableName,
		ColumnName: columnName,
		columnType: columnType,
		dataType:   dataType,
	}
}

func (e MySQLIncompatibleColumnTypeError) Error() string {
	return fmt.Sprintf("Incompatible type for column %s in table %s, expect mysql type %d but data is %s",
		e.ColumnName, e.TableName, e.columnType, e.dataType)
}
