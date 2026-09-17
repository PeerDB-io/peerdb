package exceptions

import (
	"fmt"
)

type MongoInvalidIdValueError struct {
	Table string
}

func NewInvalidIdValueError(table string) *MongoInvalidIdValueError {
	return &MongoInvalidIdValueError{table}
}

func (e *MongoInvalidIdValueError) Error() string {
	return fmt.Sprintf("_id field is missing or null in table %s; _id must be present and non-null", e.Table)
}

type MongoUnmappedFieldError struct {
	Table string
	Field string
}

func NewMongoUnmappedFieldError(table string, field string) *MongoUnmappedFieldError {
	return &MongoUnmappedFieldError{Table: table, Field: field}
}

func (e *MongoUnmappedFieldError) Error() string {
	return fmt.Sprintf("field %q in table %s is not among the configured Postgres columns; "+
		"please add the column on Postgres with the appropriate data type", e.Field, e.Table)
}
