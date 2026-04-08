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
