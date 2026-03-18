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

type ClickHouseMVError struct {
	error
}

func NewClickHouseMVError(err error) *ClickHouseMVError {
	return &ClickHouseMVError{err}
}

func (e *ClickHouseMVError) Error() string {
	return "ClickHouse MV Error: " + e.error.Error()
}

func (e *ClickHouseMVError) Unwrap() error {
	return e.error
}
