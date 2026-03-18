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
