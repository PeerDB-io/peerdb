package exceptions

import "fmt"

type BigQueryError struct {
	error
}

func NewBigQueryError(err error) *BigQueryError {
	return &BigQueryError{err}
}

func (e *BigQueryError) Error() string {
	return "BigQuery Error: " + e.error.Error()
}

func (e *BigQueryError) Unwrap() error {
	return e.error
}

type BigQueryWatermarkColumnMissingError struct {
	error
	TableName  string
	ColumnName string
}

func NewBigQueryWatermarkColumnMissingError(
	err error, tableName, columnName string,
) *BigQueryWatermarkColumnMissingError {
	return &BigQueryWatermarkColumnMissingError{
		error:      err,
		TableName:  tableName,
		ColumnName: columnName,
	}
}

func (e *BigQueryWatermarkColumnMissingError) Error() string {
	return fmt.Sprintf("BigQuery query CDC watermark column %q no longer exists on source table %q: %v",
		e.ColumnName, e.TableName, e.error)
}

func (e *BigQueryWatermarkColumnMissingError) Unwrap() error {
	return e.error
}
