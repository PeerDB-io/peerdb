package exceptions

import (
	"log/slog"

	"github.com/gogo/googleapis/google/rpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/protoadapt"
)

var ErrUnderMaintenance = NewUnavailableApiError("PeerDB is under maintenance. Please retry in a few minutes")

func convertToStatus(c codes.Code, msg string, details ...*rpc.ErrorInfo) error {
	errorStatus := status.New(c, msg)
	if len(details) == 0 {
		return errorStatus.Err()
	}
	convertedDetails := make([]protoadapt.MessageV1, len(details))
	for i, detail := range details {
		convertedDetails[i] = detail
	}
	richStatus, err := errorStatus.WithDetails(convertedDetails...)
	if err != nil {
		slog.Error("Failed to convert to grpc proto error", "error", err)
		return errorStatus.Err()
	}
	return richStatus.Err()
}

func NewInvalidArgumentApiError(msg string, details ...*rpc.ErrorInfo) error {
	return convertToStatus(codes.InvalidArgument, msg, details...)
}

func NewFailedPreconditionApiError(msg string, details ...*rpc.ErrorInfo) error {
	return convertToStatus(codes.FailedPrecondition, msg, details...)
}

func NewInternalApiError(msg string, details ...*rpc.ErrorInfo) error {
	return convertToStatus(codes.Internal, msg, details...)
}

func NewUnavailableApiError(msg string, details ...*rpc.ErrorInfo) error {
	return convertToStatus(codes.Unavailable, msg, details...)
}

func NewUnimplementedApiError(msg string, details ...*rpc.ErrorInfo) error {
	return convertToStatus(codes.Unimplemented, msg, details...)
}

func NewAlreadyExistsApiError(msg string, details ...*rpc.ErrorInfo) error {
	return convertToStatus(codes.AlreadyExists, msg, details...)
}

func NewNotFoundApiError(msg string, details ...*rpc.ErrorInfo) error {
	return convertToStatus(codes.NotFound, msg, details...)
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
