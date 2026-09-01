package shared

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"

	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

func TestWrapErrorKeepsIntermediateWraps(t *testing.T) {
	t.Parallel()

	// Mirrors the QRep partition-planning chain: the sentinel sits three wraps deep, and every
	// layer above it carries context the user needs -- above all the name of the missing table.
	err := WrapError("failed to get partitions from source",
		fmt.Errorf("failed to get partitions: %w",
			fmt.Errorf(`table "public"."whois_scans" not found in pg_class: %w`, ErrTableDoesNotExist)))

	message := fmt.Sprintf("%+v", err)
	assert.Contains(t, message, `"public"."whois_scans"`, "Error message lost the table name")
	require.ErrorIs(t, err, ErrTableDoesNotExist, "Error no longer unwraps to the sentinel")

	applicationErr, ok := errors.AsType[*temporal.ApplicationError](err)
	require.True(t, ok, "Expected an ApplicationError")
	assert.Equal(t, exceptions.ApplicationErrorTypeIrrecoverableMissingTables.String(), applicationErr.Type(),
		"Error type was not carried over from the wrapped sentinel")
	assert.True(t, applicationErr.NonRetryable(), "Expected the wrapped error to stay non-retryable")
}

func TestWrapErrorWithoutApplicationError(t *testing.T) {
	t.Parallel()

	cause := errors.New("connection reset by peer")
	err := WrapError("failed to sync records", cause)

	assert.Equal(t, "failed to sync records: connection reset by peer", err.Error(), "Unexpected error message")
	require.ErrorIs(t, err, cause, "Error no longer unwraps to its cause")
}
