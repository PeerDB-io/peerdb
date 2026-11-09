package connbigquery

import (
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"

	"github.com/PeerDB-io/peerdb/flow/alerting"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

func TestDownscopeTokenExchangeErrorClassification(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		expectedClass alerting.ErrorClass
		code          int
	}{
		{code: 503, expectedClass: alerting.ErrorRetryRecoverable},
		{code: 429, expectedClass: alerting.ErrorRetryRecoverable},
		{code: 400, expectedClass: alerting.ErrorOther},
	} {
		t.Run(strconv.Itoa(tc.code), func(t *testing.T) {
			t.Parallel()

			body := fmt.Sprintf("{\n  \"error\": {\n    \"code\": %d\n  }\n}", tc.code)
			rawErr := fmt.Errorf("downscope: unable to exchange token, %d: %s", tc.code, body)

			wrapped := wrapDownscopeError(rawErr)
			apiErr, ok := errors.AsType[*googleapi.Error](wrapped)
			require.True(t, ok)
			assert.Equal(t, tc.code, apiErr.Code)
			assert.Equal(t, body, apiErr.Body)
			require.ErrorIs(t, wrapped, rawErr)

			err := fmt.Errorf("failed to sync records: failed to insert batch: failed to get headers: %w",
				exceptions.NewGCSError(fmt.Errorf("failed to generate downscoped token: %w", wrapped)))
			errorClass, errInfo := alerting.GetErrorClass(t.Context(), err)
			assert.Equal(t, tc.expectedClass, errorClass)
			assert.Equal(t, alerting.ErrorInfo{
				Source: alerting.ErrorSourceGCS,
				Code:   strconv.Itoa(tc.code),
			}, errInfo)
		})
	}
}

func TestWrapDownscopeErrorLeavesOtherErrorsUntouched(t *testing.T) {
	t.Parallel()

	rawErr := errors.New("dial tcp: connection refused")
	assert.Same(t, rawErr, wrapDownscopeError(rawErr))
}
