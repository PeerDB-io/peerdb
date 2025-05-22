package exceptions

import "errors"

var UnderMaintenanceError = errors.New("PeerDB is under maintenance. Please retry in a few minutes")
