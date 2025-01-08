package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"google.golang.org/api/option"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

type GcpServiceAccount struct {
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

func GcpServiceAccountFromProto(sa *protos.GcpServiceAccount) *GcpServiceAccount {
	return &GcpServiceAccount{
		Type:                    sa.AuthType,
		ProjectID:               sa.ProjectId,
		PrivateKeyID:            sa.PrivateKeyId,
		PrivateKey:              sa.PrivateKey,
		ClientEmail:             sa.ClientEmail,
		ClientID:                sa.ClientId,
		AuthURI:                 sa.AuthUri,
		TokenURI:                sa.TokenUri,
		AuthProviderX509CertURL: sa.AuthProviderX509CertUrl,
		ClientX509CertURL:       sa.ClientX509CertUrl,
	}
}

// Validates a GcpServiceAccount, that none of the fields are empty.
func (sa *GcpServiceAccount) Validate() error {
	v := reflect.ValueOf(*sa)
	for i := range v.NumField() {
		if v.Field(i).String() == "" {
			return fmt.Errorf("field %s is empty", v.Type().Field(i).Name)
		}
	}
	return nil
}

// CreateBigQueryClient creates a new BigQuery client from a GcpServiceAccount.
func (sa *GcpServiceAccount) CreateBigQueryClient(ctx context.Context) (*bigquery.Client, error) {
	saJSON, err := json.Marshal(sa)
	if err != nil {
		return nil, fmt.Errorf("failed to get json: %v", err)
	}

	client, err := bigquery.NewClient(
		ctx,
		sa.ProjectID,
		option.WithCredentialsJSON(saJSON),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery client: %v", err)
	}

	return client, nil
}

// CreateStorageClient creates a new Storage client from a GcpServiceAccount.
func (sa *GcpServiceAccount) CreateStorageClient(ctx context.Context) (*storage.Client, error) {
	saJSON, err := json.Marshal(sa)
	if err != nil {
		return nil, fmt.Errorf("failed to get json: %v", err)
	}

	client, err := storage.NewClient(
		ctx,
		option.WithCredentialsJSON(saJSON),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create Storage client: %v", err)
	}

	return client, nil
}

// CreatePubSubClient creates a new PubSub client from a GcpServiceAccount.
func (sa *GcpServiceAccount) CreatePubSubClient(ctx context.Context) (*pubsub.Client, error) {
	saJSON, err := json.Marshal(sa)
	if err != nil {
		return nil, fmt.Errorf("failed to get json: %v", err)
	}

	client, err := pubsub.NewClient(
		ctx,
		sa.ProjectID,
		option.WithCredentialsJSON(saJSON),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery client: %v", err)
	}

	return client, nil
}
