package exceptions

import "fmt"

type PrimaryKeyModifiedError struct {
	error
	TableName  string
	ColumnName string
}

func NewPrimaryKeyModifiedError(err error, tableName, columnName string) *PrimaryKeyModifiedError {
	return &PrimaryKeyModifiedError{err, tableName, columnName}
}

func (e *PrimaryKeyModifiedError) Unwrap() error {
	return e.error
}

func (e *PrimaryKeyModifiedError) Error() string {
	return fmt.Sprintf("cannot locate primary key column '%s' value for table '%s': %v", e.ColumnName, e.TableName, e.error.Error())
}
