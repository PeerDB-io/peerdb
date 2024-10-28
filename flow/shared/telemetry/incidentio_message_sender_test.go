package telemetry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestIncidentIoMessageSenderImpl_SendMessage(t *testing.T) {
	tests := []struct {
		serverResponse     IncidentIoResponse
		name               string
		subject            string
		body               string
		attributes         Attributes
		serverResponseCode int
		expectError        bool
	}{
		{
			name: "successful send with info alert",
			attributes: Attributes{
				DeploymentUID: uuid.New().String(),
				Level:         INFO,
				Tags:          []string{"tag1", "tag2"},
				Type:          "incident",
			},
			subject:            "Test Incident",
			body:               "This is a test incident",
			serverResponse:     IncidentIoResponse{Status: "success", Message: "Event accepted for processing", DeduplicationKey: "stonik"},
			serverResponseCode: http.StatusAccepted,
			expectError:        false,
		},
		{
			name: "successful send with warn alert",
			attributes: Attributes{
				DeploymentUID: uuid.New().String(),
				Level:         WARN,
				Tags:          []string{"tag1", "tag2"},
				Type:          "incident",
			},
			subject:            "Test Incident",
			body:               "This is a test incident",
			serverResponse:     IncidentIoResponse{Status: "success", Message: "Event accepted for processing", DeduplicationKey: "stonik"},
			serverResponseCode: http.StatusAccepted,
			expectError:        false,
		},
		{
			name: "successful send with error alert",
			attributes: Attributes{
				DeploymentUID: uuid.New().String(),
				Level:         ERROR,
				Tags:          []string{"tag1", "tag2"},
				Type:          "incident",
			},
			subject:            "Test Incident",
			body:               "This is a test incident",
			serverResponse:     IncidentIoResponse{Status: "success", Message: "Event accepted for processing", DeduplicationKey: "stonik"},
			serverResponseCode: http.StatusAccepted,
			expectError:        false,
		},
		{
			name: "successful send with critical alert",
			attributes: Attributes{
				DeploymentUID: uuid.New().String(),
				Level:         CRITICAL,
				Tags:          []string{"tag1", "tag2"},
				Type:          "incident",
			},
			subject:            "Test Incident",
			body:               "This is a test incident",
			serverResponse:     IncidentIoResponse{Status: "success", Message: "Event accepted for processing", DeduplicationKey: "stonik"},
			serverResponseCode: http.StatusAccepted,
			expectError:        false,
		},
		{
			name: "unauthenticated",
			attributes: Attributes{
				DeploymentUID: uuid.New().String(),
				Level:         "firing",
				Tags:          []string{"tag1", "tag2"},
				Type:          "incident",
			},
			subject:            "Test Incident",
			body:               "This is a test incident",
			serverResponse:     IncidentIoResponse{Status: "authentication_error"},
			serverResponseCode: http.StatusUnauthorized,
			expectError:        true,
		},
		{
			name: "not found",
			attributes: Attributes{
				DeploymentUID: uuid.New().String(),
				Level:         "firing",
				Tags:          []string{"tag1", "tag2"},
				Type:          "incident",
			},
			subject:            "Test Incident",
			body:               "This is a test incident",
			serverResponse:     IncidentIoResponse{Status: "not_found"},
			serverResponseCode: http.StatusNotFound,
			expectError:        true,
		},
		{
			name: "server error",
			attributes: Attributes{
				DeploymentUID: uuid.New().String(),
				Level:         "firing",
				Tags:          []string{"tag1", "tag2"},
				Type:          "incident",
			},
			subject:            "Test Incident",
			body:               "This is a test incident",
			serverResponse:     IncidentIoResponse{Status: "error", Message: "Failed to create incident", DeduplicationKey: "stonik"},
			serverResponseCode: http.StatusInternalServerError,
			expectError:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeIncidentIoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "application/json", r.Header.Get("Content-Type"))   //nolint:testifylint
				require.Equal(t, "Bearer test-token", r.Header.Get("Authorization")) //nolint:testifylint

				var alert IncidentIoAlert
				bodyBytes, err := io.ReadAll(r.Body)
				require.NoError(t, err) //nolint:testifylint

				err = json.Unmarshal(bodyBytes, &alert)
				require.NoError(t, err) //nolint:testifylint
				deduplicationString := strings.Join([]string{
					"deployID", tt.attributes.DeploymentUID,
					"subject", tt.subject,
					"runID", "",
					"activityName", "",
				}, " || ")

				h := sha256.New()
				h.Write([]byte(deduplicationString))
				deduplicationHash := hex.EncodeToString(h.Sum(nil))

				// Check deduplication hash was generated correctly
				require.Equal(t, deduplicationHash, alert.DeduplicationKey) //nolint:testifylint

				// Check level was successfully mapped
				require.Equal(t, string(ResolveIncidentIoLevels(tt.attributes.Level)), alert.Metadata["level"]) //nolint:testifylint

				// mock response
				w.WriteHeader(tt.serverResponseCode)
				err = json.NewEncoder(w).Encode(tt.serverResponse)
				if err != nil {
					require.Fail(t, "failed to mock response") //nolint:testifylint
				}
			}))
			defer fakeIncidentIoServer.Close()

			config := IncidentIoMessageSenderConfig{
				URL:   fakeIncidentIoServer.URL,
				Token: "test-token",
			}
			sender := &IncidentIoMessageSenderImpl{
				http:   &http.Client{Timeout: time.Second * 5},
				config: config,
			}

			ctx := context.Background()
			status, err := sender.SendMessage(ctx, tt.subject, tt.body, tt.attributes)

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.serverResponse.Status, status)
			}
		})
	}
}
