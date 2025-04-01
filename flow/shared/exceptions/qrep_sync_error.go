package exceptions

type QRepSyncError struct {
	DestinationTable    string
	DestinationDatabase string
	error
}

func (e *QRepSyncError) Error() string {
	return "QRepSync Error: " + e.error.Error()
}

func (e *QRepSyncError) Unwrap() error {
	return e.error
}

func NewQRepSyncError(destinationTable string, destinationDatabase string, err error) *QRepSyncError {
	return &QRepSyncError{destinationTable, destinationDatabase, err}
}
