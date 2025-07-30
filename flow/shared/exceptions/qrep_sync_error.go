package exceptions

import (
	"fmt"

	chproto "github.com/ClickHouse/ch-go/proto"
	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
)

type QRepSyncError struct {
	error
	DestinationTable    string
	DestinationDatabase string
}

func NewQRepSyncError(err error, destinationTable string, destinationDatabase string) *QRepSyncError {
	return &QRepSyncError{err, destinationTable, destinationDatabase}
}

func (e *QRepSyncError) Error() string {
	return "QRepSync Error: " + e.error.Error()
}

func (e *QRepSyncError) Unwrap() error {
	return e.error
}

func (e *QRepSyncError) SanitizeClickHouseError(chException *clickhouse.Exception) string {
	if chException.Code == int32(chproto.ErrIncorrectData) {
		return fmt.Sprintf("QRepSync Error: %s, Destination Table: %s, Destination Database: %s",
			"Failed to parse data during insertion", e.DestinationTable, e.DestinationDatabase)
	}
	return fmt.Sprintf("QRepSync Error: %s, Destination Table: %s, Destination Database: %s",
		e.error.Error(), e.DestinationTable, e.DestinationDatabase)
}
