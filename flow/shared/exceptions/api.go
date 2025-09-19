package exceptions

import (
	"errors"
	"log/slog"

	"github.com/gogo/googleapis/google/rpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/protoadapt"
)

var ErrUnderMaintenance = NewUnavailableApiError(errors.New("PeerDB is under maintenance. Please retry in a few minutes"))

func convertToStatus(c codes.Code, err error, details ...*rpc.ErrorInfo) error {
	errorStatus := status.New(c, err.Error())
	if len(details) == 0 {
		return errorStatus.Err()
	}
	convertedDetails := make([]protoadapt.MessageV1, len(details))
	for i, detail := range details {
		convertedDetails[i] = detail
	}
	richStatus, err := errorStatus.WithDetails(convertedDetails...)
	if err != nil {
		// This cannot happen because we control all calls to convertToStatus and only pass code != OK and allow only rpc.ErrorInfo in details
		slog.Error("Failed to convert to grpc proto error", "error", err) //nolint:sloglint // No context in conversion helper
		return errorStatus.Err()
	}
	return richStatus.Err()
}

func NewInvalidArgumentApiError(err error, details ...*rpc.ErrorInfo) error {
	return convertToStatus(codes.InvalidArgument, err, details...)
}

func NewFailedPreconditionApiError(err error, details ...*rpc.ErrorInfo) error {
	return convertToStatus(codes.FailedPrecondition, err, details...)
}

func NewInternalApiError(err error, details ...*rpc.ErrorInfo) error {
	return convertToStatus(codes.Internal, err, details...)
}

func NewUnavailableApiError(err error, details ...*rpc.ErrorInfo) error {
	return convertToStatus(codes.Unavailable, err, details...)
}

func NewUnimplementedApiError(err error, details ...*rpc.ErrorInfo) error {
	return convertToStatus(codes.Unimplemented, err, details...)
}

func NewAlreadyExistsApiError(err error, details ...*rpc.ErrorInfo) error {
	return convertToStatus(codes.AlreadyExists, err, details...)
}

func NewNotFoundApiError(err error, details ...*rpc.ErrorInfo) error {
	return convertToStatus(codes.NotFound, err, details...)
}

// Below is an example of how to create and use rpc.ErrorInfo
// Follow the grpc guidelines and use constant keys for ErrorInfo fields and metadata keys
// E.g. NewClickHousePeerErrorInfo(map[string]string{
// 	ErrorMetadataDownstreamErrorCode: "1234",
// })

const (
	ErrorInfoReasonClickHousePeer = "CLICKHOUSE_PEER"
	ErrorInfoReasonMirror         = "MIRROR"
)

const (
	ErrorInfoDomain = "peerdb.io"
)

const (
	ErrorMetadataDownstreamErrorCode = "downstreamErrorCode"
	ErrorMetadataOffendingField      = "offendingField"
)

func NewClickHousePeerErrorInfo(metadata map[string]string) *rpc.ErrorInfo {
	return &rpc.ErrorInfo{
		Reason:   ErrorInfoReasonClickHousePeer,
		Domain:   ErrorInfoDomain,
		Metadata: metadata,
	}
}

func NewMirrorErrorInfo(metadata map[string]string) *rpc.ErrorInfo {
	return &rpc.ErrorInfo{
		Reason:   ErrorInfoReasonMirror,
		Domain:   ErrorInfoDomain,
		Metadata: metadata,
	}
}
