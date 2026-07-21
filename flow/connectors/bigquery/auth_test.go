package connbigquery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cloud.google.com/go/auth/credentials"
	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestBigQueryServiceAccountAuthTypeRemainsLegacy(t *testing.T) {
	var config protos.BigqueryConfig
	require.NoError(t, protojson.Unmarshal(
		[]byte(`{"authType":"service_account","projectId":"resource-project","datasetId":"dataset"}`),
		&config,
	))
	require.Equal(t, BigQueryAuthTypeServiceAccount, config.GetAuthType())

	config.PrivateKeyId = "key-id"
	config.PrivateKey = "private-key"
	config.ClientEmail = "legacy@example.com"
	config.ClientId = "client-id"
	config.AuthUri = "https://accounts.google.com/o/oauth2/auth"
	config.TokenUri = "https://oauth2.googleapis.com/token"
	config.AuthProviderX509CertUrl = "https://www.googleapis.com/oauth2/v1/certs"
	config.ClientX509CertUrl = "https://www.googleapis.com/robot/v1/metadata/x509/legacy"

	credentialConfig, err := newBigQueryCredentialConfig(t.Context(), &config, config.ProjectId)
	require.NoError(t, err)
	require.Equal(t, credentials.ServiceAccount, credentialConfig.credentialType)
	require.Equal(t, bigquery.DetectProjectID, credentialConfig.clientProjectID)

	var credentialsDocument map[string]any
	require.NoError(t, json.Unmarshal(credentialConfig.credentialsJSON, &credentialsDocument))
	require.Equal(t, "service_account", credentialsDocument["type"])
	require.Equal(t, "legacy@example.com", credentialsDocument["client_email"])
}

func TestBigQueryRejectsUnknownAuthType(t *testing.T) {
	tests := []struct {
		name     string
		authType string
	}{
		{name: "empty"},
		{name: "unknown", authType: "unknown"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := newBigQueryCredentialConfig(t.Context(), &protos.BigqueryConfig{AuthType: test.authType}, "project")
			require.ErrorContains(t, err, "unsupported BigQuery auth_type")
			require.ErrorContains(t, err, fmt.Sprintf("%q", test.authType))
		})
	}
}

func TestResolveBigQueryResource(t *testing.T) {
	tests := []struct {
		name          string
		projectID     string
		datasetID     string
		wantProjectID string
		wantDatasetID string
		wantErr       string
	}{
		{
			name:          "separate project and dataset",
			projectID:     "resource-project",
			datasetID:     "dataset",
			wantProjectID: "resource-project",
			wantDatasetID: "dataset",
		},
		{
			name:          "qualified dataset overrides project",
			projectID:     "configured-project",
			datasetID:     "qualified-project.dataset",
			wantProjectID: "qualified-project",
			wantDatasetID: "dataset",
		},
		{
			name:      "three part dataset is rejected",
			datasetID: "project.dataset.table",
			wantErr:   "invalid dataset ID",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			projectID, datasetID, err := resolveBigQueryResource(&protos.BigqueryConfig{
				ProjectId: test.projectID,
				DatasetId: test.datasetID,
			})
			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.wantProjectID, projectID)
			require.Equal(t, test.wantDatasetID, datasetID)
		})
	}
}

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

	_, err = credentials.NewCredentialsFromJSON(credentials.ExternalAccount, credentialsJSON, &credentials.DetectOptions{
		Scopes: []string{bigquery.Scope},
	})
	require.NoError(t, err)
}

func TestWorkloadIdentityCredentialsReloadProjectedToken(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "projected-token")
	require.NoError(t, os.WriteFile(tokenFile, []byte("projected-token-one"), 0o600))

	config := workloadIdentityDeploymentConfig{
		targetServiceAccount: "tenant@tenant-project.iam.gserviceaccount.com",
		tokenFile:            tokenFile,
		projectID:            "platform-project",
		clusterLocation:      "us-central1",
		clusterName:          "clickpipes",
	}
	credentialsJSON, err := config.credentialsJSON()
	require.NoError(t, err)

	var stsSubjectTokens []string
	var iamAuthorizationHeaders []string
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		require.Equal(t, "https", request.URL.Scheme)
		switch request.URL.Host {
		case "sts.googleapis.com":
			require.Equal(t, http.MethodPost, request.Method)
			require.Equal(t, "/v1/token", request.URL.Path)
			body, err := io.ReadAll(request.Body)
			require.NoError(t, err)
			form, err := url.ParseQuery(string(body))
			require.NoError(t, err)
			stsSubjectTokens = append(stsSubjectTokens, form.Get("subject_token"))
			//nolint:gosec // Test-only fake STS response values.
			return jsonResponse(t, request, map[string]any{
				"access_token":      fmt.Sprintf("sts-access-token-%d", len(stsSubjectTokens)),
				"issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
				"token_type":        "Bearer",
				"expires_in":        1,
			}), nil
		case "iamcredentials.googleapis.com":
			require.Equal(t, http.MethodPost, request.Method)
			require.Equal(t,
				"/v1/projects/-/serviceAccounts/tenant@tenant-project.iam.gserviceaccount.com:generateAccessToken",
				request.URL.Path,
			)
			iamAuthorizationHeaders = append(iamAuthorizationHeaders, request.Header.Get("Authorization"))
			return jsonResponse(t, request, map[string]any{
				"accessToken": fmt.Sprintf("tenant-access-token-%d", len(iamAuthorizationHeaders)),
				"expireTime":  time.Now().Add(time.Second).UTC().Format(time.RFC3339),
			}), nil
		default:
			return nil, fmt.Errorf("unexpected request URL %s", request.URL)
		}
	})

	creds, err := credentials.NewCredentialsFromJSON(
		credentials.ExternalAccount,
		credentialsJSON,
		&credentials.DetectOptions{
			Scopes: []string{bigquery.Scope},
			Client: &http.Client{Transport: transport},
		},
	)
	require.NoError(t, err)

	firstToken, err := creds.Token(t.Context())
	require.NoError(t, err)
	require.Equal(t, "tenant-access-token-1", firstToken.Value)
	require.Equal(t, []string{"projected-token-one"}, stsSubjectTokens)
	require.Equal(t, []string{"Bearer sts-access-token-1"}, iamAuthorizationHeaders)

	require.NoError(t, os.WriteFile(tokenFile, []byte("projected-token-two"), 0o600))
	time.Sleep(time.Until(firstToken.Expiry) + time.Second)

	secondToken, err := creds.Token(t.Context())
	require.NoError(t, err)
	require.Equal(t, "tenant-access-token-2", secondToken.Value)
	require.Equal(t, []string{"projected-token-one", "projected-token-two"}, stsSubjectTokens)
	require.Equal(t,
		[]string{"Bearer sts-access-token-1", "Bearer sts-access-token-2"},
		iamAuthorizationHeaders,
	)
}

func TestResolveWorkloadIdentityDeploymentConfig(t *testing.T) {
	environment := map[string]string{
		workloadIdentityServiceAccountEnv: "tenant@tenant-project.iam.gserviceaccount.com",
		workloadIdentityTokenFileEnv:      "/token",
	}
	config, err := resolveWorkloadIdentityDeploymentConfig(t.Context(), workloadIdentityConfigSource{
		lookupEnv: func(name string) (string, bool) {
			value, ok := environment[name]
			return value, ok
		},
		projectID: func(context.Context) (string, error) {
			return "metadata-project", nil
		},
		instanceAttributeValue: func(_ context.Context, name string) (string, error) {
			return map[string]string{
				"cluster-location": "metadata-location",
				"cluster-name":     "metadata-cluster",
			}[name], nil
		},
	})
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
		{
			name:      "target service account",
			wantError: workloadIdentityServiceAccountEnv,
		},
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
			_, err := resolveWorkloadIdentityDeploymentConfig(t.Context(), workloadIdentityConfigSource{
				lookupEnv: func(name string) (string, bool) {
					value, ok := test.environment[name]
					return value, ok
				},
				projectID: func(context.Context) (string, error) {
					return "", errors.New("metadata unavailable")
				},
				instanceAttributeValue: func(context.Context, string) (string, error) {
					return "", errors.New("metadata unavailable")
				},
			})
			require.ErrorContains(t, err, test.wantError)
		})
	}
}

func TestWorkloadIdentityUsesExplicitResourceProject(t *testing.T) {
	t.Setenv(workloadIdentityServiceAccountEnv, "tenant@tenant-project.iam.gserviceaccount.com")
	t.Setenv(workloadIdentityTokenFileEnv, "/token")
	t.Setenv(workloadIdentityProjectIDEnv, "platform-project")
	t.Setenv(workloadIdentityClusterLocationEnv, "us-central1")
	t.Setenv(workloadIdentityClusterNameEnv, "clickpipes")

	config := &protos.BigqueryConfig{
		ProjectId: "resource-project",
		DatasetId: "dataset",
		AuthType:  BigQueryAuthTypeServiceAccountWorkloadIdentity,
	}
	projectID, datasetID, err := resolveBigQueryResource(config)
	require.NoError(t, err)
	require.Equal(t, "resource-project", projectID)
	require.Equal(t, "dataset", datasetID)

	credentialConfig, err := newBigQueryCredentialConfig(t.Context(), config, projectID)
	require.NoError(t, err)
	require.Equal(t, credentials.ExternalAccount, credentialConfig.credentialType)
	require.Equal(t, "resource-project", credentialConfig.clientProjectID)
}

func TestWorkloadIdentityRequiresPeerProject(t *testing.T) {
	config := &protos.BigqueryConfig{
		DatasetId: "dataset",
		AuthType:  BigQueryAuthTypeServiceAccountWorkloadIdentity,
	}
	_, err := newBigQueryCredentialConfig(t.Context(), config, "")
	require.ErrorContains(t, err, "project ID must be set in the peer")
}

func TestWorkloadIdentityCredentialConfigReportsMissingDeploymentConfig(t *testing.T) {
	t.Setenv(workloadIdentityServiceAccountEnv, "")
	t.Setenv(workloadIdentityTokenFileEnv, "")
	t.Setenv(workloadIdentityProjectIDEnv, "")
	t.Setenv(workloadIdentityClusterLocationEnv, "")
	t.Setenv(workloadIdentityClusterNameEnv, "")

	config := &protos.BigqueryConfig{
		ProjectId: "resource-project",
		DatasetId: "dataset",
		AuthType:  BigQueryAuthTypeServiceAccountWorkloadIdentity,
	}
	_, err := newBigQueryCredentialConfig(t.Context(), config, config.ProjectId)
	require.ErrorContains(t, err, workloadIdentityServiceAccountEnv)
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
