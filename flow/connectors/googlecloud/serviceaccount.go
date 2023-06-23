package googlecloud

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"google.golang.org/api/option"
)

type GoogleCloudServiceAccount struct {
	Type                    string `json:"type"`
	ProjectID               string `json:"project_id"`
	PrivateKeyID            string `json:"private_key_id"`
	PrivateKey              string `json:"private_key"`
	ClientEmail             string `json:"client_email"`
	ClientID                string `json:"client_id"`
	AuthURI                 string `json:"auth_uri"`
	TokenURI                string `json:"token_uri"`
	AuthProviderX509CertURL string `json:"auth_provider_x509_cert_url"`
	ClientX509CertURL       string `json:"client_x509_cert_url"`
}

// Create GoogleCloudServiceAccount from BigqueryConfig
func NewGoogleCloudServiceAccount(googleCloudAuthConfig *protos.GoogleCloudAuthConfig) (*GoogleCloudServiceAccount, error) {
	var serviceAccount GoogleCloudServiceAccount
	serviceAccount.Type = googleCloudAuthConfig.AuthType
	serviceAccount.ProjectID = googleCloudAuthConfig.ProjectId
	serviceAccount.PrivateKeyID = googleCloudAuthConfig.PrivateKeyId
	serviceAccount.PrivateKey = googleCloudAuthConfig.PrivateKey
	serviceAccount.ClientEmail = googleCloudAuthConfig.ClientEmail
	serviceAccount.ClientID = googleCloudAuthConfig.ClientId
	serviceAccount.AuthURI = googleCloudAuthConfig.AuthUri
	serviceAccount.TokenURI = googleCloudAuthConfig.TokenUri
	serviceAccount.AuthProviderX509CertURL = googleCloudAuthConfig.AuthProviderX509CertUrl
	serviceAccount.ClientX509CertURL = googleCloudAuthConfig.ClientX509CertUrl

	if err := serviceAccount.Validate(); err != nil {
		return nil, fmt.Errorf("failed to validate BigQueryServiceAccount: %w", err)
	}

	return &serviceAccount, nil
}

// Validate validates a GoogleCloudServiceAccount, that none of the fields are empty.
func (gcsa *GoogleCloudServiceAccount) Validate() error {
	v := reflect.ValueOf(*gcsa)
	for i := 0; i < v.NumField(); i++ {
		if v.Field(i).String() == "" {
			return fmt.Errorf("field %s is empty", v.Type().Field(i).Name)
		}
	}
	return nil
}

// Return GoogleCloudServiceAccount as JSON byte array
func (gcsa *GoogleCloudServiceAccount) ToJSON() ([]byte, error) {
	return json.Marshal(gcsa)
}

// CreateBigQueryClient creates a new BigQuery client from a GoogleCloudServiceAccount.
func (gcsa *GoogleCloudServiceAccount) CreateBigQueryClient(ctx context.Context) (*bigquery.Client, error) {
	gcsaJSON, err := gcsa.ToJSON()
	if err != nil {
		return nil, fmt.Errorf("failed to get json: %v", err)
	}

	client, err := bigquery.NewClient(
		ctx,
		gcsa.ProjectID,
		option.WithCredentialsJSON(gcsaJSON),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery client: %v", err)
	}

	return client, nil
}

// CreateStorageClient creates a new Storage client from a GoogleCloudServiceAccount.
func (gcsa *GoogleCloudServiceAccount) CreateStorageClient(ctx context.Context) (*storage.Client, error) {
	bqsaJSON, err := gcsa.ToJSON()
	if err != nil {
		return nil, fmt.Errorf("failed to get json: %v", err)
	}

	client, err := storage.NewClient(
		ctx,
		option.WithCredentialsJSON(bqsaJSON),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create Storage client: %v", err)
	}

	return client, nil
}
