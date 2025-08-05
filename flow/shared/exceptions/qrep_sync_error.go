package exceptions

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
