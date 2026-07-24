package connbigquery

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"

	"cloud.google.com/go/auth/credentials"
	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/compute/metadata"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

const (
	// BigQueryAuthTypeServiceAccount selects legacy service-account-key credentials.
	BigQueryAuthTypeServiceAccount = "service_account"
	// BigQueryAuthTypeServiceAccountWorkloadIdentity selects deployment-scoped workload identity credentials.
	BigQueryAuthTypeServiceAccountWorkloadIdentity = "service_account_workload_identity"

	workloadIdentityServiceAccountEnv = "PEERDB_GCP_WORKLOAD_IDENTITY_TARGET_SERVICE_ACCOUNT"
	//nolint:gosec // Environment variable name, not a credential.
	workloadIdentityTokenFileEnv       = "PEERDB_GCP_WORKLOAD_IDENTITY_TOKEN_FILE"
	workloadIdentityProjectIDEnv       = "PEERDB_GCP_PROJECT_ID"
	workloadIdentityClusterLocationEnv = "PEERDB_GCP_CLUSTER_LOCATION"
	workloadIdentityClusterNameEnv     = "PEERDB_GCP_CLUSTER_NAME"

	//nolint:gosec // Fixed public Google authentication endpoints, not credentials.
	googleSTSTokenURL = "https://sts.googleapis.com/v1/token"
	//nolint:gosec // Fixed public Google authentication endpoints, not credentials.
	googleIAMCredentialsURL = "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/"
	//nolint:gosec // OAuth subject-token type identifier, not a credential.
	jwtSubjectTokenType = "urn:ietf:params:oauth:token-type:jwt"
)

type bigQueryCredentialConfig struct {
	credentialType  credentials.CredType
	clientProjectID string
	credentialsJSON []byte
}

type workloadIdentityDeploymentConfig struct {
	targetServiceAccount string
	tokenFile            string
	projectID            string
	clusterLocation      string
	clusterName          string
}

type workloadIdentityConfigSource struct {
	lookupEnv              func(string) (string, bool)
	projectID              func(context.Context) (string, error)
	instanceAttributeValue func(context.Context, string) (string, error)
}

type externalAccountCredentials struct {
	Type                           string                          `json:"type"`
	Audience                       string                          `json:"audience"`
	SubjectTokenType               string                          `json:"subject_token_type"`
	TokenURL                       string                          `json:"token_url"`
	ServiceAccountImpersonationURL string                          `json:"service_account_impersonation_url"`
	CredentialSource               externalAccountCredentialSource `json:"credential_source"`
}

type externalAccountCredentialSource struct {
	File   string                                `json:"file"`
	Format externalAccountCredentialSourceFormat `json:"format"`
}

type externalAccountCredentialSourceFormat struct {
	Type string `json:"type"`
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
			credentialType:  credentials.ServiceAccount,
			clientProjectID: bigquery.DetectProjectID,
		}, nil
	}

	if resourceProjectID == "" {
		return nil, fmt.Errorf("BigQuery project ID must be set in the peer when workload identity is selected")
	}

	deploymentConfig, err := resolveWorkloadIdentityDeploymentConfig(ctx, workloadIdentityConfigSource{
		lookupEnv:              os.LookupEnv,
		projectID:              metadata.ProjectIDWithContext,
		instanceAttributeValue: metadata.InstanceAttributeValueWithContext,
	})
	if err != nil {
		return nil, err
	}
	credentialsJSON, err := deploymentConfig.credentialsJSON()
	if err != nil {
		return nil, err
	}
	return &bigQueryCredentialConfig{
		credentialsJSON: credentialsJSON,
		credentialType:  credentials.ExternalAccount,
		clientProjectID: resourceProjectID,
	}, nil
}

func resolveWorkloadIdentityDeploymentConfig(
	ctx context.Context,
	source workloadIdentityConfigSource,
) (*workloadIdentityDeploymentConfig, error) {
	targetServiceAccount := envValue(source.lookupEnv, workloadIdentityServiceAccountEnv)
	if targetServiceAccount == "" {
		return nil, fmt.Errorf(
			"BigQuery workload identity requires deployment environment variable %s",
			workloadIdentityServiceAccountEnv,
		)
	}
	tokenFile := envValue(source.lookupEnv, workloadIdentityTokenFileEnv)
	if tokenFile == "" {
		return nil, fmt.Errorf(
			"BigQuery workload identity requires deployment environment variable %s",
			workloadIdentityTokenFileEnv,
		)
	}

	projectID, err := envOrMetadata(
		ctx,
		source.lookupEnv,
		workloadIdentityProjectIDEnv,
		source.projectID,
	)
	if err != nil {
		return nil, err
	}
	clusterLocation, err := envOrMetadata(
		ctx,
		source.lookupEnv,
		workloadIdentityClusterLocationEnv,
		func(ctx context.Context) (string, error) {
			return source.instanceAttributeValue(ctx, "cluster-location")
		},
	)
	if err != nil {
		return nil, err
	}
	clusterName, err := envOrMetadata(
		ctx,
		source.lookupEnv,
		workloadIdentityClusterNameEnv,
		func(ctx context.Context) (string, error) {
			return source.instanceAttributeValue(ctx, "cluster-name")
		},
	)
	if err != nil {
		return nil, err
	}

	return &workloadIdentityDeploymentConfig{
		targetServiceAccount: targetServiceAccount,
		tokenFile:            tokenFile,
		projectID:            projectID,
		clusterLocation:      clusterLocation,
		clusterName:          clusterName,
	}, nil
}

func envValue(lookupEnv func(string) (string, bool), name string) string {
	value, _ := lookupEnv(name)
	return strings.TrimSpace(value)
}

func envOrMetadata(
	ctx context.Context,
	lookupEnv func(string) (string, bool),
	envName string,
	metadataValue func(context.Context) (string, error),
) (string, error) {
	if value := envValue(lookupEnv, envName); value != "" {
		return value, nil
	}
	value, err := metadataValue(ctx)
	if err != nil {
		return "", fmt.Errorf(
			"BigQuery workload identity requires %s or the corresponding GKE metadata: %w",
			envName,
			err,
		)
	}
	if strings.TrimSpace(value) == "" {
		return "", fmt.Errorf(
			"BigQuery workload identity requires %s or non-empty corresponding GKE metadata",
			envName,
		)
	}
	return strings.TrimSpace(value), nil
}

func (config *workloadIdentityDeploymentConfig) credentialsJSON() ([]byte, error) {
	pool := config.projectID + ".svc.id.goog"
	audience := fmt.Sprintf(
		"identitynamespace:%s:https://container.googleapis.com/v1/projects/%s/locations/%s/clusters/%s",
		pool,
		config.projectID,
		config.clusterLocation,
		config.clusterName,
	)
	credentialConfig := externalAccountCredentials{
		Type:             "external_account",
		Audience:         audience,
		SubjectTokenType: jwtSubjectTokenType,
		TokenURL:         googleSTSTokenURL,
		ServiceAccountImpersonationURL: googleIAMCredentialsURL +
			url.PathEscape(config.targetServiceAccount) + ":generateAccessToken",
		CredentialSource: externalAccountCredentialSource{
			// The cloud auth file provider reopens this path for every token exchange,
			// allowing Kubernetes to rotate the projected token in place.
			File: config.tokenFile,
			Format: externalAccountCredentialSourceFormat{
				Type: "text",
			},
		},
	}
	credentialsJSON, err := json.Marshal(credentialConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal BigQuery workload identity credentials: %w", err)
	}
	return credentialsJSON, nil
}
