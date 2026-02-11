package mongo

import (
	"crypto/tls"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/connstring"
)

const (
	ReadPreferencePrimary            = "primary"
	ReadPreferencePrimaryPreferred   = "primaryPreferred"
	ReadPreferenceSecondary          = "secondary"
	ReadPreferenceSecondaryPreferred = "secondaryPreferred"
	ReadPreferenceNearest            = "nearest"
)

type ClientConfig struct {
	Uri                 string
	Username            string
	Password            string
	ReadPreference      string
	DisableTls          bool
	RootCa              string
	TlsHost             string
	CreateTlsConfigFunc func(minVersion uint16, rootCAs string, host string, tlsHost string, skipCertVerification bool) (*tls.Config, error)
	Dialer              options.ContextDialer
}

func BuildClientOptions(config ClientConfig) (*options.ClientOptions, error) {
	connStr, err := connstring.Parse(config.Uri)
	if err != nil {
		return nil, fmt.Errorf("error parsing uri: %w", err)
	}

	if connStr.UsernameSet {
		return nil, errors.New("connection string should not contain username and password")
	}

	clientOptions := options.Client().
		ApplyURI(config.Uri).
		SetAppName("PeerDB Mongo Connector").
		SetAuth(options.Credential{
			Username: config.Username,
			Password: config.Password,
		}).
		SetCompressors([]string{"zstd", "snappy"}).
		SetReadConcern(readconcern.Majority()).
		SetDialer(config.Dialer)

	switch config.ReadPreference {
	case ReadPreferencePrimary:
		clientOptions.SetReadPreference(readpref.Primary())
	case ReadPreferencePrimaryPreferred:
		clientOptions.SetReadPreference(readpref.PrimaryPreferred())
	case ReadPreferenceSecondary:
		clientOptions.SetReadPreference(readpref.Secondary())
	case ReadPreferenceSecondaryPreferred, "":
		clientOptions.SetReadPreference(readpref.SecondaryPreferred())
	case ReadPreferenceNearest:
		clientOptions.SetReadPreference(readpref.Nearest())
	default:
		return nil, fmt.Errorf("invalid ReadPreference: %s", config.ReadPreference)
	}

	if config.DisableTls {
		// user disabled TLS via toggle — override URI params
		clientOptions.TLSConfig = nil
	} else if connStr.SSLSet && !connStr.SSL {
		// user set tls=false in URI param — honor it
	} else {
		// apply TLS config — honor tlsInsecure from URI params if set
		skipCertVerification := connStr.SSLInsecureSet && connStr.SSLInsecure
		tlsConfig, err := config.CreateTlsConfigFunc(tls.VersionTLS12, config.RootCa, "", config.TlsHost, skipCertVerification)
		if err != nil {
			return nil, err
		}
		clientOptions.SetTLSConfig(tlsConfig)
	}

	if err := clientOptions.Validate(); err != nil {
		return nil, fmt.Errorf("error validating client options: %w", err)
	}

	return clientOptions, nil
}
