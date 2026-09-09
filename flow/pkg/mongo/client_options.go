package mongo

import (
	"crypto/tls"
	"fmt"
	"net/url"
	"strings"

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
	CreateTlsConfigFunc  func(minVersion uint16, rootCAs string, host string, tlsHost string, skipCertVerification bool) (*tls.Config, error)
	Dialer               options.ContextDialer
	Uri                  string
	Username             string
	Password             string
	ReadPreference       string
	RootCa               string
	TlsHost              string
	DisableTls           bool
	SkipCertVerification bool
}

func BuildClientOptions(config ClientConfig) (*options.ClientOptions, error) {
	connStr, err := connstring.Parse(config.Uri)
	if err != nil {
		return nil, fmt.Errorf("error parsing uri: %w", err)
	}

	if connStr.UsernameSet {
		return nil, fmt.Errorf("connection string should not contain username and password")
	}

	// The username/password are supplied separately, so an authMechanism embedded in
	// the URI (e.g. SCRAM-SHA-256 for Firestore) makes ApplyURI record a
	// "username required" validation error. Strip it from the URI and apply it on the
	// credential instead so the mechanism is still honored.
	uri, authMechanism := extractAuthMechanism(config.Uri)

	credential := options.Credential{
		Username: config.Username,
		Password: config.Password,
	}
	if authMechanism != "" {
		credential.AuthMechanism = authMechanism
	}

	clientOptions := options.Client().
		ApplyURI(uri).
		SetAppName("PeerDB Mongo Connector").
		SetAuth(credential).
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
		// apply TLS config
		skipCertVerification := config.SkipCertVerification || // explicit deactivation from config
			connStr.SSLInsecureSet && connStr.SSLInsecure // honor tlsInsecure from URI params if set
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

// extractAuthMechanism removes the authMechanism query parameter from a MongoDB URI,
// returning the sanitized URI and the mechanism value (empty if not present). Only the
// query portion is manipulated so comma-separated host lists are left untouched.
func extractAuthMechanism(uri string) (string, string) {
	base, query, found := strings.Cut(uri, "?")
	if !found {
		return uri, ""
	}
	values, err := url.ParseQuery(query)
	if err != nil {
		return uri, ""
	}
	mechanism := values.Get("authMechanism")
	if mechanism == "" {
		return uri, ""
	}
	values.Del("authMechanism")
	if len(values) == 0 {
		return base, mechanism
	}
	return base + "?" + values.Encode(), mechanism
}
