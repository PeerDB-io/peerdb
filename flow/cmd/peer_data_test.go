package cmd

import (
	"context"
	"errors"
	"fmt"
	"net"
	"syscall"
	"testing"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"

	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

func TestNewPeerConnectorApiError(t *testing.T) {
	t.Parallel()

	dialTimeout := &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ETIMEDOUT}
	peerCreate := func(err error) error {
		return fmt.Errorf("failed to get postgres connector: %w", exceptions.NewPeerCreateError(err))
	}

	for _, tc := range []struct {
		err                    error
		name                   string
		sourceConnectionFailed bool
	}{
		{
			name:                   "dial timeout",
			err:                    peerCreate(fmt.Errorf("failed to create connection: %w", dialTimeout)),
			sourceConnectionFailed: true,
		},
		{
			name: "connection refused",
			err: peerCreate(fmt.Errorf("failed to create connection: %w",
				&net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED})),
			sourceConnectionFailed: true,
		},
		{
			name: "dns",
			err: peerCreate(fmt.Errorf("failed to create connection: %w",
				&net.DNSError{Err: "no such host", Name: "db.example.com", IsNotFound: true})),
			sourceConnectionFailed: true,
		},
		{
			name: "ssh tunnel setup",
			err: peerCreate(fmt.Errorf("failed to create ssh tunnel: %w",
				exceptions.NewSSHTunnelSetupError(errors.New("ssh: handshake failed")))),
			sourceConnectionFailed: true,
		},
		{
			name: "bad password",
			err: peerCreate(fmt.Errorf("failed to create connection: %w",
				exceptions.NewAuthError(&pgconn.PgError{Code: pgerrcode.InvalidPassword, Message: "password authentication failed"}))),
			sourceConnectionFailed: true,
		},
		{
			name:                   "connect deadline",
			err:                    peerCreate(fmt.Errorf("failed to create connection: %w", context.DeadlineExceeded)),
			sourceConnectionFailed: true,
		},
		{
			name: "internal failure while building connector",
			err:  peerCreate(errors.New("failed to fetch CDC Store setting: unexpected value")),
		},
		{
			name: "unsupported peer type",
			err:  peerCreate(errors.ErrUnsupported),
		},
		{
			name: "catalog unreachable while loading peer",
			err:  fmt.Errorf("failed to get postgres connector: %w", dialTimeout),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			apiErr := newPeerConnectorApiError(t.Context(), tc.err)
			require.Equal(t, codes.FailedPrecondition, apiErr.Code())
			require.Contains(t, apiErr.Error(), tc.err.Error())

			details := apiErr.GRPCStatus().Details()
			if !tc.sourceConnectionFailed {
				require.Empty(t, details)
				return
			}
			require.Len(t, details, 1)
			info, ok := details[0].(*errdetails.ErrorInfo)
			require.True(t, ok)
			require.Equal(t, common.ErrorInfoDomain, info.Domain)
			require.Equal(t, common.ErrorInfoReasonSourceConnectionFailed, info.Reason)
		})
	}
}
