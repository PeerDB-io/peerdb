package exceptions

import "fmt"

type MySQLIncompatibleColumnTypeError struct {
	TableName  string
	ColumnName string
	dataType   string
	columnType byte
}

func NewIncompatibleColumnTypeError(
	tableName string, columnName string, columnType byte, dataType string,
) *MySQLIncompatibleColumnTypeError {
	return &MySQLIncompatibleColumnTypeError{
		TableName:  tableName,
		ColumnName: columnName,
		dataType:   dataType,
		columnType: columnType,
	}
}

func (e MySQLIncompatibleColumnTypeError) Error() string {
	return fmt.Sprintf("Incompatible type for column %s in table %s, expect mysql type %d but data is %s",
		e.ColumnName, e.TableName, e.columnType, e.dataType)
}
