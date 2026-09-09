package utils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorkloadIdentityCredentialsJSON(t *testing.T) {
	config := workloadIdentityDeploymentConfig{ //nolint:gosec // Test-only fake account and token path.
		targetServiceAccount: "tenant@tenant-project.iam.gserviceaccount.com",
		tokenFile:            "/var/run/secrets/peerdb/gcp-token",
		projectID:            "platform-project",
		clusterLocation:      "us-central1",
		clusterName:          "clickpipes",
	}

	credentialsJSON, err := config.credentialsJSON()
	require.NoError(t, err)

	var document externalAccountCredentials
	require.NoError(t, json.Unmarshal(credentialsJSON, &document))
	require.Equal(t, "external_account", document.Type)
	require.Equal(t,
		"identitynamespace:platform-project.svc.id.goog:"+
			"https://container.googleapis.com/v1/projects/platform-project/locations/us-central1/clusters/clickpipes",
		document.Audience,
	)
	require.Equal(t, jwtSubjectTokenType, document.SubjectTokenType)
	require.Equal(t, googleSTSTokenURL, document.TokenURL)
	require.Equal(t,
		"https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/"+
			"tenant@tenant-project.iam.gserviceaccount.com:generateAccessToken",
		document.ServiceAccountImpersonationURL,
	)
	require.Equal(t, "/var/run/secrets/peerdb/gcp-token", document.CredentialSource.File)
	require.Equal(t, "text", document.CredentialSource.Format.Type)
}

func TestWorkloadIdentityCredentialsReloadProjectedToken(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "projected-token")
	require.NoError(t, os.WriteFile(tokenFile, []byte("projected-token-one"), 0o600))
	setWorkloadIdentityEnv(t, tokenFile)

	var stsSubjectTokens []string
	var iamAuthorizationHeaders []string
	var iamScopes [][]string
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		require.Equal(t, "https", request.URL.Scheme)
		switch request.URL.Host {
		case "sts.googleapis.com":
			body, err := io.ReadAll(request.Body)
			require.NoError(t, err)
			form, err := url.ParseQuery(string(body))
			require.NoError(t, err)
			stsSubjectTokens = append(stsSubjectTokens, form.Get("subject_token"))
			return jsonResponse(t, request, map[string]any{ //nolint:gosec // Test-only fake token.
				"access_token":      fmt.Sprintf("sts-access-token-%d", len(stsSubjectTokens)),
				"issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
				"token_type":        "Bearer",
				"expires_in":        1,
			}), nil
		case "iamcredentials.googleapis.com":
			iamAuthorizationHeaders = append(iamAuthorizationHeaders, request.Header.Get("Authorization"))
			var body struct {
				Scope []string `json:"scope"`
			}
			require.NoError(t, json.NewDecoder(request.Body).Decode(&body))
			iamScopes = append(iamScopes, body.Scope)
			return jsonResponse(t, request, map[string]any{
				"accessToken": fmt.Sprintf("tenant-access-token-%d", len(iamAuthorizationHeaders)),
				"expireTime":  time.Now().Add(time.Second).UTC().Format(time.RFC3339),
			}), nil
		default:
			return nil, fmt.Errorf("unexpected request URL %s", request.URL)
		}
	})

	creds, err := newGCPWorkloadIdentityCredentials(
		t.Context(),
		[]string{GCPCloudSQLLoginScope},
		&http.Client{Transport: transport},
	)
	require.NoError(t, err)

	firstToken, err := creds.Token(t.Context())
	require.NoError(t, err)
	require.Equal(t, "tenant-access-token-1", firstToken.Value)
	require.Equal(t, []string{"projected-token-one"}, stsSubjectTokens)
	require.Equal(t, []string{"Bearer sts-access-token-1"}, iamAuthorizationHeaders)
	require.Equal(t, [][]string{{GCPCloudSQLLoginScope}}, iamScopes)

	require.NoError(t, os.WriteFile(tokenFile, []byte("projected-token-two"), 0o600))
	time.Sleep(time.Until(firstToken.Expiry) + time.Second)

	secondToken, err := creds.Token(t.Context())
	require.NoError(t, err)
	require.Equal(t, "tenant-access-token-2", secondToken.Value)
	require.Equal(t, []string{"projected-token-one", "projected-token-two"}, stsSubjectTokens)
	require.Equal(t, []string{"Bearer sts-access-token-1", "Bearer sts-access-token-2"}, iamAuthorizationHeaders)
	require.Equal(t, [][]string{{GCPCloudSQLLoginScope}, {GCPCloudSQLLoginScope}}, iamScopes)
}

func TestResolveWorkloadIdentityDeploymentConfig(t *testing.T) {
	t.Setenv(workloadIdentityServiceAccountEnv, "tenant@tenant-project.iam.gserviceaccount.com")
	t.Setenv(workloadIdentityTokenFileEnv, "/token")
	t.Setenv(workloadIdentityProjectIDEnv, "")
	t.Setenv(workloadIdentityClusterLocationEnv, "")
	t.Setenv(workloadIdentityClusterNameEnv, "")

	metadataValues := map[string]string{
		"/computeMetadata/v1/project/project-id":                   "metadata-project",
		"/computeMetadata/v1/instance/attributes/cluster-location": "metadata-location",
		"/computeMetadata/v1/instance/attributes/cluster-name":     "metadata-cluster",
	}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		assert.Equal(t, "Google", request.Header.Get("Metadata-Flavor"))
		value, ok := metadataValues[request.URL.Path]
		assert.True(t, ok, "unexpected metadata path %s", request.URL.Path)
		_, _ = response.Write([]byte(value))
	}))
	defer server.Close()
	t.Setenv("GCE_METADATA_HOST", strings.TrimPrefix(server.URL, "http://"))

	config, err := resolveWorkloadIdentityDeploymentConfig(t.Context())
	require.NoError(t, err)
	require.Equal(t, "metadata-project", config.projectID)
	require.Equal(t, "metadata-location", config.clusterLocation)
	require.Equal(t, "metadata-cluster", config.clusterName)
}

func TestResolveWorkloadIdentityDeploymentConfigMissing(t *testing.T) {
	tests := []struct {
		name        string
		environment map[string]string
		wantError   string
	}{
		{name: "target service account", wantError: workloadIdentityServiceAccountEnv},
		{
			name: "token file",
			environment: map[string]string{
				workloadIdentityServiceAccountEnv: "tenant@tenant-project.iam.gserviceaccount.com",
			},
			wantError: workloadIdentityTokenFileEnv,
		},
		{
			name: "project context",
			environment: map[string]string{
				workloadIdentityServiceAccountEnv: "tenant@tenant-project.iam.gserviceaccount.com",
				workloadIdentityTokenFileEnv:      "/token",
			},
			wantError: workloadIdentityProjectIDEnv,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, name := range []string{
				workloadIdentityServiceAccountEnv,
				workloadIdentityTokenFileEnv,
				workloadIdentityProjectIDEnv,
				workloadIdentityClusterLocationEnv,
				workloadIdentityClusterNameEnv,
			} {
				t.Setenv(name, test.environment[name])
			}
			server := httptest.NewServer(http.NotFoundHandler())
			defer server.Close()
			t.Setenv("GCE_METADATA_HOST", strings.TrimPrefix(server.URL, "http://"))

			_, err := resolveWorkloadIdentityDeploymentConfig(t.Context())
			require.ErrorContains(t, err, test.wantError)
		})
	}
}

func setWorkloadIdentityEnv(t *testing.T, tokenFile string) {
	t.Helper()
	t.Setenv(workloadIdentityServiceAccountEnv, "tenant@tenant-project.iam.gserviceaccount.com")
	t.Setenv(workloadIdentityTokenFileEnv, tokenFile)
	t.Setenv(workloadIdentityProjectIDEnv, "platform-project")
	t.Setenv(workloadIdentityClusterLocationEnv, "us-central1")
	t.Setenv(workloadIdentityClusterNameEnv, "clickpipes")
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func jsonResponse(t *testing.T, request *http.Request, value any) *http.Response {
	t.Helper()
	body, err := json.Marshal(value)
	require.NoError(t, err)
	return &http.Response{
		StatusCode: http.StatusOK,
		Header: http.Header{
			"Content-Type": []string{"application/json"},
		},
		Body:    io.NopCloser(bytes.NewReader(body)),
		Request: request,
	}
}
