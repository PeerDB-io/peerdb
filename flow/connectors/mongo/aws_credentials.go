package connmongo

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

// awsCredentialsProvider hands credentials from an AWS SDK provider to the Mongo driver, so
// MONGODB-AWS logins reuse PeerDB's role assumption.
type awsCredentialsProvider struct {
	provider aws.CredentialsProvider
}

func (p awsCredentialsProvider) Retrieve(ctx context.Context) (options.AWSCredentials, error) {
	creds, err := p.provider.Retrieve(ctx)
	if err != nil {
		return options.AWSCredentials{}, exceptions.NewMongoIAMAuthError(err)
	}
	// The driver caches credentials until Expires; without it, it would reuse them after expiry.
	return options.AWSCredentials{
		AccessKeyID:     creds.AccessKeyID,
		SecretAccessKey: creds.SecretAccessKey,
		SessionToken:    creds.SessionToken,
		Source:          creds.Source,
		CanExpire:       creds.CanExpire,
		Expires:         creds.Expires,
		AccountID:       creds.AccountID,
	}, nil
}
