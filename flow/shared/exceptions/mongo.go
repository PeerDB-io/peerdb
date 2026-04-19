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

type MongoCreateChangeStreamError struct {
	error
}

func NewMongoCreateChangeStreamError(err error) *MongoCreateChangeStreamError {
	return &MongoCreateChangeStreamError{err}
}

func (e *MongoCreateChangeStreamError) Error() string {
	return e.error.Error()
}

func (e *MongoCreateChangeStreamError) Unwrap() error {
	return e.error
}
