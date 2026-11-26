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

	statusBody := func(code int) string {
		return fmt.Sprintf("{\n  \"error\": {\n    \"code\": %d\n  }\n}", code)
	}
	for _, tc := range []struct {
		name          string
		body          string
		expectedClass alerting.ErrorClass
		code          int
	}{
		{name: "503", code: 503, body: statusBody(503), expectedClass: alerting.ErrorRetryRecoverable},
		{name: "429", code: 429, body: statusBody(429), expectedClass: alerting.ErrorRetryRecoverable},
		{name: "400", code: 400, body: statusBody(400), expectedClass: alerting.ErrorOther},
		{
			name:          "400 UberMint",
			code:          400,
			body:          `{"error":"invalid_grant","error_description":"Error getting an UberMint"}`,
			expectedClass: alerting.ErrorRetryRecoverable,
		},
		{
			name:          "400 other invalid_grant",
			code:          400,
			body:          `{"error":"invalid_grant","error_description":"Invalid JWT Signature."}`,
			expectedClass: alerting.ErrorOther,
		},
		{
			name:          "400 UberMint without invalid_grant",
			code:          400,
			body:          `{"error":"invalid_request","error_description":"Error getting an UberMint"}`,
			expectedClass: alerting.ErrorOther,
		},
		{
			name:          "401 UberMint",
			code:          401,
			body:          `{"error":"invalid_grant","error_description":"Error getting an UberMint"}`,
			expectedClass: alerting.ErrorOther,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			body := tc.body
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
