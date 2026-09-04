package connbigquery

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

const (
	testWorkloadIdentityServiceAccountEnv = "PEERDB_GCP_WORKLOAD_IDENTITY_TARGET_SERVICE_ACCOUNT"
	//nolint:gosec // Environment variable name, not a credential.
	testWorkloadIdentityTokenFileEnv       = "PEERDB_GCP_WORKLOAD_IDENTITY_TOKEN_FILE"
	testWorkloadIdentityProjectIDEnv       = "PEERDB_GCP_PROJECT_ID"
	testWorkloadIdentityClusterLocationEnv = "PEERDB_GCP_CLUSTER_LOCATION"
	testWorkloadIdentityClusterNameEnv     = "PEERDB_GCP_CLUSTER_NAME"
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
	require.Nil(t, credentialConfig.authCredentials)
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
			name:          "project without default dataset",
			projectID:     "resource-project",
			wantProjectID: "resource-project",
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

func TestWorkloadIdentityUsesExplicitResourceProject(t *testing.T) {
	setBigQueryWorkloadIdentityEnv(t)

	config := &protos.BigqueryConfig{
		ProjectId: "resource-project",
		AuthType:  BigQueryAuthTypeServiceAccountWorkloadIdentity,
	}
	projectID, datasetID, err := resolveBigQueryResource(config)
	require.NoError(t, err)
	require.Equal(t, "resource-project", projectID)
	require.Empty(t, datasetID)

	credentialConfig, err := newBigQueryCredentialConfig(t.Context(), config, projectID)
	require.NoError(t, err)
	require.Equal(t, "resource-project", credentialConfig.clientProjectID)
	require.NotNil(t, credentialConfig.authCredentials)
}

func TestValidateBigQueryConnectionWithoutDefaultDataset(t *testing.T) {
	requestPaths := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		requestPaths <- request.URL.Path
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"kind":"bigquery#datasetList","datasets":[]}`))
	}))
	defer server.Close()

	client, err := bigquery.NewClient(
		t.Context(),
		"resource-project",
		option.WithEndpoint(server.URL),
		option.WithoutAuthentication(),
	)
	require.NoError(t, err)
	defer client.Close()

	require.NoError(t, validateBigQueryConnection(t.Context(), client, "resource-project", ""))
	require.Equal(t, "/projects/resource-project/datasets", <-requestPaths)
}

func TestValidateBigQueryConnectionWithoutDefaultDatasetReportsListingFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		http.Error(response, `{"error":{"code":403,"message":"permission denied"}}`, http.StatusForbidden)
	}))
	defer server.Close()

	client, err := bigquery.NewClient(
		t.Context(),
		"resource-project",
		option.WithEndpoint(server.URL),
		option.WithoutAuthentication(),
	)
	require.NoError(t, err)
	defer client.Close()

	err = validateBigQueryConnection(t.Context(), client, "resource-project", "")
	require.ErrorContains(t, err, "failed to list BigQuery datasets")
}

func TestWorkloadIdentityRequiresPeerProject(t *testing.T) {
	config := &protos.BigqueryConfig{
		AuthType: BigQueryAuthTypeServiceAccountWorkloadIdentity,
	}
	_, err := newBigQueryCredentialConfig(t.Context(), config, "")
	require.ErrorContains(t, err, "project ID must be set in the peer")
}

func TestWorkloadIdentityCredentialConfigReportsMissingDeploymentConfig(t *testing.T) {
	t.Setenv(testWorkloadIdentityServiceAccountEnv, "")
	t.Setenv(testWorkloadIdentityTokenFileEnv, "")
	t.Setenv(testWorkloadIdentityProjectIDEnv, "")
	t.Setenv(testWorkloadIdentityClusterLocationEnv, "")
	t.Setenv(testWorkloadIdentityClusterNameEnv, "")

	config := &protos.BigqueryConfig{
		ProjectId: "resource-project",
		AuthType:  BigQueryAuthTypeServiceAccountWorkloadIdentity,
	}
	_, err := newBigQueryCredentialConfig(t.Context(), config, config.ProjectId)
	require.ErrorContains(t, err, testWorkloadIdentityServiceAccountEnv)
}

func setBigQueryWorkloadIdentityEnv(t *testing.T) {
	t.Helper()
	t.Setenv(testWorkloadIdentityServiceAccountEnv, "tenant@tenant-project.iam.gserviceaccount.com")
	t.Setenv(testWorkloadIdentityTokenFileEnv, "/token")
	t.Setenv(testWorkloadIdentityProjectIDEnv, "platform-project")
	t.Setenv(testWorkloadIdentityClusterLocationEnv, "us-central1")
	t.Setenv(testWorkloadIdentityClusterNameEnv, "clickpipes")
}
