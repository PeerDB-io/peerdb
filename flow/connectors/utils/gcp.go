package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"cloud.google.com/go/auth/credentials"
	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub/v2"
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

func (sa *GcpServiceAccount) authOption() (option.ClientOption, error) {
	saJSON, err := json.Marshal(sa) //nolint:gosec // G117: credential struct marshaled for inline use
	if err != nil {
		return nil, fmt.Errorf("failed to marshal service account json: %v", err)
	}

	creds, err := credentials.DetectDefault(&credentials.DetectOptions{
		CredentialsJSON: saJSON,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to detect credentials: %v", err)
	}

	return option.WithAuthCredentials(creds), nil
}

func (sa *GcpServiceAccount) CreateBigQueryClient(ctx context.Context) (*bigquery.Client, error) {
	authOpt, err := sa.authOption()
	if err != nil {
		return nil, err
	}

	client, err := bigquery.NewClient(ctx, sa.ProjectID, authOpt)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery client: %v", err)
	}

	return client, nil
}

func (sa *GcpServiceAccount) CreateStorageClient(ctx context.Context) (*storage.Client, error) {
	authOpt, err := sa.authOption()
	if err != nil {
		return nil, err
	}

	client, err := storage.NewClient(ctx, authOpt)
	if err != nil {
		return nil, fmt.Errorf("failed to create Storage client: %v", err)
	}

	return client, nil
}

func (sa *GcpServiceAccount) CreatePubSubClient(ctx context.Context) (*pubsub.Client, error) {
	authOpt, err := sa.authOption()
	if err != nil {
		return nil, err
	}

	client, err := pubsub.NewClient(ctx, sa.ProjectID, authOpt)
	if err != nil {
		return nil, fmt.Errorf("failed to create PubSub client: %v", err)
	}

	return client, nil
}
