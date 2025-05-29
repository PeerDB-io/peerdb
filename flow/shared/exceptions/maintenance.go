package exceptions

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ErrUnderMaintenance = status.Error(
	codes.Unavailable,
	"PeerDB is under maintenance. Please retry in a few minutes",
)
