package cmd

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/gogo/googleapis/google/rpc"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/protoadapt"

	"github.com/PeerDB-io/peerdb/flow/generated/grpc_handler"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

// APIError is a strongly-typed error that must be a gRPC status error.
// All handler methods should return this type instead of the generic error interface.
type APIError = grpc_handler.APIError

type apiError struct {
	status *status.Status
}

func newAPIError(s *status.Status) *apiError {
	return &apiError{status: s}
}

func NewInvalidArgumentApiError(err error, details ...protoadapt.MessageV1) *apiError {
	return newAPIError(convertToStatus(codes.InvalidArgument, err, details...))
}

func NewFailedPreconditionApiError(err error, details ...protoadapt.MessageV1) *apiError {
	return newAPIError(convertToStatus(codes.FailedPrecondition, err, details...))
}

func NewInternalApiError(err error, details ...protoadapt.MessageV1) *apiError {
	return newAPIError(convertToStatus(codes.Internal, err, details...))
}

func NewUnavailableApiError(err error, details ...protoadapt.MessageV1) *apiError {
	return newAPIError(convertToStatus(codes.Unavailable, err, details...))
}

func NewUnimplementedApiError(err error, details ...protoadapt.MessageV1) *apiError {
	return newAPIError(convertToStatus(codes.Unimplemented, err, details...))
}

func NewAlreadyExistsApiError(err error, details ...protoadapt.MessageV1) *apiError {
	return newAPIError(convertToStatus(codes.AlreadyExists, err, details...))
}

func NewNotFoundApiError(err error, details ...protoadapt.MessageV1) *apiError {
	return newAPIError(convertToStatus(codes.NotFound, err, details...))
}

func (e *apiError) Error() string {
	if e.status == nil {
		return "unknown error"
	}
	return e.status.Err().Error()
}

func (e *apiError) GRPCStatus() *status.Status {
	return e.status
}

func (e *apiError) Code() codes.Code {
	if e.status == nil {
		return codes.Unknown
	}
	return e.status.Code()
}

func convertToStatus(code codes.Code, err error, details ...protoadapt.MessageV1) *status.Status {
	errorStatus := status.New(code, err.Error())
	if len(details) == 0 {
		return errorStatus
	}
	richStatus, err := errorStatus.WithDetails(details...)
	if err != nil {
		// This cannot happen because we control all calls to convertToStatus and only pass code != OK and valid proto details
		slog.Error("Failed to convert to grpc proto error", slog.Any("error", err)) //nolint:sloglint // No context in conversion helper
		return errorStatus
	}

	return richStatus
}

// AsAPIError converts an error to APIError if it's a gRPC status error,
// otherwise wraps it as an Internal error
func AsAPIError(err error) APIError {
	if err == nil {
		return nil
	}

	if apiErr, ok := err.(APIError); ok {
		return apiErr
	}

	if s, ok := status.FromError(err); ok {
		return newAPIError(s)
	}

	return NewInternalApiError(err)
}

func NewMirrorErrorInfo(metadata map[string]string) *rpc.ErrorInfo {
	return &rpc.ErrorInfo{
		Reason:   common.ErrorInfoReasonMirror,
		Domain:   common.ErrorInfoDomain,
		Metadata: metadata,
	}
}

func NewSourceTableMissingErrorInfo() *rpc.ErrorInfo {
	return &rpc.ErrorInfo{
		Reason: common.ErrorInfoReasonSourceTableMissing,
		Domain: common.ErrorInfoDomain,
	}
}

// NewSourceTableMissingPreconditionFailure builds the per-table breakdown detail
// that accompanies the SOURCE_TABLE_MISSING ErrorInfo on FailedPrecondition.
func NewSourceTableMissingPreconditionFailure(tables []common.QualifiedTable) protoadapt.MessageV1 {
	violations := make([]*errdetails.PreconditionFailure_Violation, len(tables))
	for i, t := range tables {
		subject := fmt.Sprintf("%s.%s", t.Namespace, t.Table)
		violations[i] = &errdetails.PreconditionFailure_Violation{
			Type:        common.ErrorInfoReasonSourceTableMissing,
			Subject:     subject,
			Description: fmt.Sprintf("source table %s does not exist", subject),
		}
	}
	return protoadapt.MessageV1Of(&errdetails.PreconditionFailure{Violations: violations})
}

func NewTablesNotInPublicationErrorInfo(publication string) *rpc.ErrorInfo {
	return &rpc.ErrorInfo{
		Reason:   common.ErrorInfoReasonTablesNotInPublication,
		Domain:   common.ErrorInfoDomain,
		Metadata: map[string]string{common.ErrorMetadataPublication: publication},
	}
}

// NewTablesNotInPublicationPreconditionFailure builds the per-table breakdown detail
// that accompanies the TABLES_NOT_IN_PUBLICATION ErrorInfo on FailedPrecondition.
func NewTablesNotInPublicationPreconditionFailure(publication string, tables []common.QualifiedTable) protoadapt.MessageV1 {
	violations := make([]*errdetails.PreconditionFailure_Violation, len(tables))
	for i, t := range tables {
		subject := fmt.Sprintf("%s.%s", t.Namespace, t.Table)
		violations[i] = &errdetails.PreconditionFailure_Violation{
			Type:        common.ErrorInfoReasonTablesNotInPublication,
			Subject:     subject,
			Description: fmt.Sprintf("table %s is not in publication %q", subject, publication),
		}
	}
	return protoadapt.MessageV1Of(&errdetails.PreconditionFailure{Violations: violations})
}

var ErrUnderMaintenance = errors.New("PeerDB is under maintenance. Please retry in a few minutes")
