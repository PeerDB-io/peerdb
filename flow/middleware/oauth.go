package middleware

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"

	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/lestrrat-go/jwx/v2/jws"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/PeerDB-io/peer-flow/peerdbenv"
)

//nolint:lll
type AuthenticationConfig struct {
	OauthJwtCustomClaims  map[string]string `json:"oauth_custom_claims" yaml:"oauth_custom_claims" mapstructure:"oauth_custom_claims"`
	KeySetJSON            string            `json:"key_set_json" yaml:"key_set_json" mapstructure:"key_set_json"`
	OAuthIssuerUrl        string            `json:"oauth_domain" yaml:"oauth_domain" mapstructure:"oauth_domain"`
	Enabled               bool              `json:"enabled" yaml:"enabled" mapstructure:"enabled"`
	OAuthDiscoveryEnabled bool              `json:"oauth_discovery_enabled" yaml:"oauth_discovery_enabled" mapstructure:"oauth_discovery_enabled"`
}

type identityProvider struct {
	keySet      jwk.Set
	validateOpt jwt.ValidateOption
	issuer      string
}

func AuthGrpcMiddleware(unauthenticatedMethods []string) (grpc.UnaryServerInterceptor, error) {
	oauthConfig := peerdbenv.GetPeerDBOAuthConfig()
	oauthJwtClaims := map[string]string{}
	if oauthConfig.OAuthJwtClaimKey != "" {
		oauthJwtClaims[oauthConfig.OAuthJwtClaimKey] = oauthConfig.OAuthClaimValue
	}
	cfg := AuthenticationConfig{
		Enabled:               oauthConfig.OAuthIssuerUrl != "",
		KeySetJSON:            oauthConfig.KeySetJson,
		OAuthDiscoveryEnabled: oauthConfig.OAuthDiscoveryEnabled,
		OAuthIssuerUrl:        oauthConfig.OAuthIssuerUrl,
		OauthJwtCustomClaims:  oauthJwtClaims,
	}
	// load identity providers before checking if authentication is enabled so configuration can be validated
	ip, err := identityProvidersFromConfig(cfg)

	if !cfg.Enabled {
		if err != nil { // if there was an error loading identity providers, warn only if authentication is disabled
			slog.Warn("OAuth is disabled", slog.Any("error", err))
		}

		slog.Warn("authentication is disabled")

		return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			return handler(ctx, req)
		}, nil
	}

	if err != nil {
		return nil, err
	}

	unauthenticatedMethodsMap := make(map[string]struct{}, len(unauthenticatedMethods))
	for _, method := range unauthenticatedMethods {
		unauthenticatedMethodsMap[method] = struct{}{}
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if _, unauthorized := unauthenticatedMethodsMap[info.FullMethod]; !unauthorized {
			var authHeader string
			authHeaders := metadata.ValueFromIncomingContext(ctx, "Authorization")
			if len(authHeaders) == 1 {
				authHeader = authHeaders[0]
			} else if len(authHeaders) > 1 {
				slog.Warn("Multiple Authorization headers supplied, request rejected", slog.String("method", info.FullMethod))
				return nil, status.Errorf(codes.Unauthenticated, "multiple Authorization headers supplied, request rejected")
			}
			_, err := validateRequestToken(authHeader, cfg.OauthJwtCustomClaims, ip...)
			if err != nil {
				slog.Debug("Failed to validate request token", slog.String("method", info.FullMethod), slog.Any("error", err))
				return nil, status.Errorf(codes.Unauthenticated, "%s", err.Error())
			}
		}

		return handler(ctx, req)
	}, nil
}

func validateRequestToken(authHeader string, claims map[string]string, ip ...identityProvider) ([]byte, error) {
	payload, err := jwtFromRequest(authHeader)
	if err != nil {
		return nil, fmt.Errorf("failed to parse authorization header: %w", err)
	}

	// We could simplify to jwt.Parse(payload, opts...), but it is ok for now
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

	for key, value := range claims {
		if token.PrivateClaims()[key] != value {
			return nil, fmt.Errorf("token claim %s mismatch", key)
		}
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
		keysetIdentityProvider,
		openIdIdentityProvider,
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

func openIdIdentityProvider(cfg AuthenticationConfig) (*identityProvider, error) {
	if cfg.OAuthIssuerUrl == "" {
		slog.Debug("OAuth Issuer Url not configured for identity provider")
		return nil, nil
	}
	if !cfg.OAuthDiscoveryEnabled {
		slog.Debug("OAuth discovery not enabled for identity provider")
		return nil, nil
	}
	issuer := cfg.OAuthIssuerUrl
	// This is a well known URL for jwks defined in OpenID discovery spec
	jwksDiscoveryUrl, err := url.JoinPath(cfg.OAuthIssuerUrl, "/.well-known/jwks.json")
	if err != nil {
		return nil, err
	}

	cache := jwk.NewCache(context.Background())
	if err := cache.Register(jwksDiscoveryUrl); err != nil {
		return nil, fmt.Errorf("failed to register JWK key set from Discovery URL %s: %w", jwksDiscoveryUrl, err)
	}
	set := jwk.NewCachedSet(cache, jwksDiscoveryUrl)

	slog.Info("JWK key set from Discovery Endpoint loaded", slog.String("jwks", jwksDiscoveryUrl), slog.Int("size", set.Len()))

	return &identityProvider{
		issuer:      issuer,
		keySet:      set,
		validateOpt: jwt.WithIssuer(issuer),
	}, nil
}

func keysetIdentityProvider(cfg AuthenticationConfig) (*identityProvider, error) {
	if cfg.KeySetJSON == "" {
		slog.Debug("JWK key set JSON not configured for identity provider")
		return nil, nil
	}

	set, err := jwk.ParseString(cfg.KeySetJSON)
	if err != nil {
		return nil, fmt.Errorf("failed to parse JWK key set from JSON: %w", err)
	}

	slog.Info("JWK key set from JSON loaded", slog.Int("size", set.Len()))

	return &identityProvider{
		issuer: cfg.OAuthIssuerUrl,
		keySet: set,
	}, nil
}
