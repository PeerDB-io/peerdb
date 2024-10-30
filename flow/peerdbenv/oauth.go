package peerdbenv

import (
	"log/slog"
	"strconv"
)

type PeerDBOAuthConfig struct {
	// there can be more complex use cases where domain != issuer, but we handle them later if required
	OAuthIssuerUrl string `json:"oauth_issuer_url"`
	KeySetJson     string `json:"key_set_json"`
	// This is a custom claim we may wish to validate (if needed)
	OAuthJwtClaimKey string `json:"oauth_jwt_claim_key"`
	OAuthClaimValue  string `json:"oauth_jwt_claim_value"`
	// Enabling uses /.well-known/ OpenID discovery endpoints, thus key-set etc. don't need to be specified
	OAuthDiscoveryEnabled bool `json:"oauth_discovery_enabled"`
}

func GetPeerDBOAuthConfig() PeerDBOAuthConfig {
	oauthIssuerUrl := GetEnvString("PEERDB_OAUTH_ISSUER_URL", "")
	oauthDiscoveryEnabledString := GetEnvString("PEERDB_OAUTH_DISCOVERY_ENABLED", "false")
	oauthDiscoveryEnabled, err := strconv.ParseBool(oauthDiscoveryEnabledString)
	if err != nil {
		slog.Error("failed to parse PEERDB_OAUTH_DISCOVERY_ENABLED to bool", "error", err)
		oauthDiscoveryEnabled = false
	}
	oauthKeysetJson := GetEnvString("PEERDB_OAUTH_KEYSET_JSON", "")

	oauthJwtClaimKey := GetEnvString("PEERDB_OAUTH_JWT_CLAIM_KEY", "")
	oauthJwtClaimValue := GetEnvString("PEERDB_OAUTH_JWT_CLAIM_VALUE", "")

	return PeerDBOAuthConfig{
		OAuthIssuerUrl:        oauthIssuerUrl,
		OAuthDiscoveryEnabled: oauthDiscoveryEnabled,
		KeySetJson:            oauthKeysetJson,
		OAuthJwtClaimKey:      oauthJwtClaimKey,
		OAuthClaimValue:       oauthJwtClaimValue,
	}
}
