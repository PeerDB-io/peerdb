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

type ReplicaIdentityIndexError struct {
	Table string
}

func NewReplicaIdentityIndexError(table string) error {
	return &ReplicaIdentityIndexError{Table: table}
}

func (e *ReplicaIdentityIndexError) Error() string {
	return fmt.Sprintf("table %s has no replica identity index", e.Table)
}

type ReplicaIdentityNothingError struct {
	Table string
}

func NewReplicaIdentityNothingError(table string, cause error) error {
	return &ReplicaIdentityNothingError{Table: table}
}

func (e *ReplicaIdentityNothingError) Error() string {
	return fmt.Sprintf("table %s has replica identity 'n'/NOTHING", e.Table)
}

type MissingPrimaryKeyError struct {
	Table string
}

func NewMissingPrimaryKeyError(table string) error {
	return &MissingPrimaryKeyError{Table: table}
}

func (e *MissingPrimaryKeyError) Error() string {
	return fmt.Sprintf("table %s has no primary keys and does not have REPLICA IDENTITY FULL", e.Table)
}
