package connbigquery

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"cloud.google.com/go/auth"
	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

const (
	// BigQueryAuthTypeServiceAccount selects legacy service-account-key credentials.
	BigQueryAuthTypeServiceAccount = "service_account"
	// BigQueryAuthTypeServiceAccountWorkloadIdentity selects deployment-scoped workload identity credentials.
	BigQueryAuthTypeServiceAccountWorkloadIdentity = "service_account_workload_identity"
)

type bigQueryCredentialConfig struct {
	authCredentials *auth.Credentials
	clientProjectID string
	credentialsJSON []byte
}

func resolveBigQueryResource(config *protos.BigqueryConfig) (string, string, error) {
	datasetID := config.GetDatasetId()
	projectID := config.GetProjectId()
	projectPart, datasetPart, found := strings.Cut(datasetID, ".")
	if found && strings.Contains(datasetPart, ".") {
		return "", "", fmt.Errorf(
			"invalid dataset ID: %s. Ensure that it is just a single string or string1.string2",
			datasetID,
		)
	}
	if projectPart != "" && datasetPart != "" {
		datasetID = datasetPart
		projectID = projectPart
	}
	return projectID, datasetID, nil
}

func newBigQueryCredentialConfig(
	ctx context.Context,
	config *protos.BigqueryConfig,
	resourceProjectID string,
) (*bigQueryCredentialConfig, error) {
	authType := config.GetAuthType()
	if authType != BigQueryAuthTypeServiceAccount && authType != BigQueryAuthTypeServiceAccountWorkloadIdentity {
		return nil, fmt.Errorf(
			"unsupported BigQuery auth_type %q: expected %q or %q",
			authType,
			BigQueryAuthTypeServiceAccount,
			BigQueryAuthTypeServiceAccountWorkloadIdentity,
		)
	}

	if authType == BigQueryAuthTypeServiceAccount {
		serviceAccount, err := NewBigQueryServiceAccount(config)
		if err != nil {
			return nil, fmt.Errorf("failed to create BigQueryServiceAccount: %w", err)
		}
		serviceAccountJSON, err := json.Marshal(serviceAccount) //nolint:gosec // G117: credential struct marshaled for inline use
		if err != nil {
			return nil, fmt.Errorf("failed to marshal service account: %v", err)
		}
		return &bigQueryCredentialConfig{
			credentialsJSON: serviceAccountJSON,
			clientProjectID: bigquery.DetectProjectID,
		}, nil
	}

	if resourceProjectID == "" {
		return nil, fmt.Errorf("BigQuery project ID must be set in the peer when workload identity is selected")
	}

	authCredentials, err := utils.NewGCPWorkloadIdentityCredentials(ctx, []string{
		bigquery.Scope,
		storage.ScopeFullControl,
	})
	if err != nil {
		return nil, err
	}
	return &bigQueryCredentialConfig{
		authCredentials: authCredentials,
		clientProjectID: resourceProjectID,
	}, nil
}
