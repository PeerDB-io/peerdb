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

const MongoIAMAuthErrorPrefix = "MongoDB IAM auth error: "

type MongoIAMAuthError struct {
	error
}

func NewMongoIAMAuthError(err error) *MongoIAMAuthError {
	return &MongoIAMAuthError{err}
}

func (e *MongoIAMAuthError) Error() string {
	return MongoIAMAuthErrorPrefix + e.error.Error()
}

func (e *MongoIAMAuthError) Unwrap() error {
	return e.error
}
