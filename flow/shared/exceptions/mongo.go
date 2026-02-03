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
	return fmt.Sprintf("identified _id with null value in table %s; _id cannot be null", e.Table)
}
