package shared

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/lestrrat-go/jwx/v2/jws"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type AuthenticationConfig struct {
	DevKeySetJSON string `json:"key_set_json" yaml:"key_set_json" mapstructure:"key_set_json"`
	Auth0Domain   string `json:"auth0_domain" yaml:"auth0_domain" mapstructure:"auth0_domain"`
	Auth0ClientID string `json:"auth0_client_id" yaml:"auth0_client_id" mapstructure:"auth0_client_id"`
	Enabled       bool   `json:"enabled" yaml:"enabled" mapstructure:"enabled"`
}

type identityProvider struct {
	keySet      jwk.Set
	validateOpt jwt.ValidateOption
	issuer      string
}

func AuthGrpcMiddleware() ([]grpc.ServerOption, error) {
	devKeySetJSON := os.Getenv("PEERDB_OAUTH_KEY_SET_JSON")
	auth0Domain := os.Getenv("PEERDB_OAUTH_AUTH0DOMAIN")
	auth0ClientID := os.Getenv("PEERDB_OAUTH_AUTH0CLIENTID")
	cfg := AuthenticationConfig{
		Enabled:       devKeySetJSON != "" || auth0Domain != "",
		DevKeySetJSON: devKeySetJSON,
		Auth0Domain:   auth0Domain,
		Auth0ClientID: auth0ClientID,
	}
	// load identity providers before checking if authentication is enabled so configuration can be validated
	ip, err := identityProvidersFromConfig(cfg)

	if !cfg.Enabled {
		if err != nil { // if there was an error loading identity providers, warn only if authentication is disabled
			slog.Warn("failed to initialize JWK key set", slog.Any("error", err))
		}

		slog.Warn("authentication is disabled")

		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			var authHeader string
			authHeaders := metadata.ValueFromIncomingContext(ctx, "authorization")
			if len(authHeaders) == 1 {
				authHeader = authHeaders[0]
			} else if len(authHeaders) > 1 {
				return nil, errors.New("multiple Authorization headers supplied, request rejected")
			}
			_, err := validateRequestToken(authHeader, ip...)
			if err != nil {
				slog.Debug("failed to validate request token", slog.Any("error", err))
				return nil, err
			}
			return handler(ctx, req)
		}),
	}, nil
}

func validateRequestToken(authHeader string, ip ...identityProvider) ([]byte, error) {
	payload, err := jwtFromRequest(authHeader)
	if err != nil {
		return nil, fmt.Errorf("failed to parse authorization header: %w", err)
	}

	token, err := jwt.ParseInsecure(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to parse token: %w", err)
	}

	provider, err := identityProviderByToken(ip, token)
	if err != nil {
		return nil, err
	}

	validateOpts := identityProviderValidateOpts(provider)
	if err := jwt.Validate(token, validateOpts...); err != nil {
		return nil, fmt.Errorf("failed to validate token: %w", err)
	}

	if _, err := jws.Verify(payload, jws.WithKeySet(provider.keySet)); err != nil {
		return nil, fmt.Errorf("failed to verify token: %w", err)
	}

	return payload, nil
}

// jwtFromRequest extracts the JWT token from the Authorization header.
// it truncates the "Bearer" prefix from the header value if exists.
func jwtFromRequest(authHeader string) ([]byte, error) {
	if authHeader == "" {
		return nil, errors.New("missing Authorization header")
	}

	return []byte(strings.TrimPrefix(authHeader, "Bearer ")), nil
}

func identityProviderValidateOpts(provider identityProvider) []jwt.ValidateOption {
	validateOpts := []jwt.ValidateOption{
		jwt.WithIssuer(provider.issuer),
		jwt.WithValidator(jwt.IsExpirationValid()),
	}

	if provider.validateOpt != nil {
		validateOpts = append(validateOpts, provider.validateOpt)
	}
	return validateOpts
}

func identityProviderByToken(ip []identityProvider, token jwt.Token) (identityProvider, error) {
	var provider identityProvider
	for _, p := range ip {
		if p.issuer == token.Issuer() {
			provider = p
			break
		}
	}

	if provider.issuer == "" {
		return identityProvider{}, fmt.Errorf("identity provider for issuer %s not found", token.Issuer())
	}
	return provider, nil
}

type identityProviderResolver func(cfg AuthenticationConfig) (*identityProvider, error)

func identityProvidersFromConfig(cfg AuthenticationConfig) ([]identityProvider, error) {
	resolvers := []identityProviderResolver{
		devIdentityProvider,
		auth0IdentityProvider,
	}

	ip := make([]identityProvider, 0, len(resolvers))
	for _, resolver := range resolvers {
		provider, err := resolver(cfg)
		if err != nil {
			return nil, err
		}

		if provider == nil {
			continue
		}

		ip = append(ip, *provider)
	}

	if len(ip) == 0 {
		return nil, errors.New("no identity providers configured")
	}

	return ip, nil
}

func auth0IdentityProvider(cfg AuthenticationConfig) (*identityProvider, error) {
	if cfg.Auth0Domain == "" {
		slog.Debug("Auth0 domain not configured for identity provider")
		return nil, nil
	}

	// Auth0 claims in doc token has client ID as the issuer
	// but in reality it's domain/custom domain
	// https://auth0.com/docs/get-started/authentication-and-authorization-flow/authenticate-with-private-key-jwt#build-the-assertion
	issuer := fmt.Sprintf("https://%s/", cfg.Auth0Domain)
	url := fmt.Sprintf("https://%s/.well-known/jwks.json", cfg.Auth0Domain)

	cache := jwk.NewCache(context.Background())
	if err := cache.Register(url); err != nil {
		return nil, fmt.Errorf("failed to register Auth0 JWK key set: %w", err)
	}
	set := jwk.NewCachedSet(cache, url)

	slog.Info("JWK key set from Auth0 loaded", slog.String("jwks", url), slog.Int("size", set.Len()))

	return &identityProvider{
		issuer:      issuer,
		keySet:      set,
		validateOpt: jwt.WithIssuer(issuer),
	}, nil
}

func devIdentityProvider(cfg AuthenticationConfig) (*identityProvider, error) {
	if cfg.DevKeySetJSON == "" {
		slog.Debug("Dev JWK key set JSON not configured for identity provider")
		return nil, nil
	}

	set, err := jwk.ParseString(cfg.DevKeySetJSON)
	if err != nil {
		return nil, fmt.Errorf("failed to parse dev JWK key set from JSON: %w", err)
	}

	slog.Info("Dev JWK key set from JSON loaded", slog.Int("size", set.Len()))

	return &identityProvider{
		issuer: "clickpipes-dev",
		keySet: set,
	}, nil
}
