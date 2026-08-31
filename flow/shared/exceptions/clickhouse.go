package exceptions

type ClickHouseQRepSyncError struct {
	error
	DestinationTable    string
	DestinationDatabase string
}

func NewClickHouseQRepSyncError(err error, destinationTable string, destinationDatabase string) *ClickHouseQRepSyncError {
	return &ClickHouseQRepSyncError{err, destinationTable, destinationDatabase}
}

func (e *ClickHouseQRepSyncError) Error() string {
	return "QRepSync Error: " + e.error.Error()
}

func (e *ClickHouseQRepSyncError) Unwrap() error {
	return e.error
}

type ClickHouseNormalizedTableCreationError struct {
	err              error
	DestinationTable string
}

func NewClickHouseNormalizedTableCreationError(err error, destinationTable string) *ClickHouseNormalizedTableCreationError {
	return &ClickHouseNormalizedTableCreationError{err, destinationTable}
}

func (e *ClickHouseNormalizedTableCreationError) Error() string {
	return e.err.Error()
}

func (e *ClickHouseNormalizedTableCreationError) Unwrap() error {
	return e.err
}
