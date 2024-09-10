package peerdbenv

import "strconv"

type PeerDBOAuthConfig struct {
	OAuthIssuerUrl        string `json:"oauth_issuer_url"`        // there can be more complex use cases where domain != issuer, but we handle them later if required
	OAuthDiscoveryEnabled bool   `json:"oauth_discovery_enabled"` // Enabling this uses the /.well-known/ OpenID discovery endpoints, thus key-set etc., doesn't need to be specified
	KeySetJson            string `json:"key_set_json"`
	OAuthJwtClaimKey      string `json:"oauth_jwt_claim_key"` // This is a custom claim we may wish to validate (if needed)
	OAuthClaimValue       string `json:"oauth_jwt_claim_value"`
}

func GetPeerDBOAuthConfig() PeerDBOAuthConfig {
	oauthIssuerUrl := GetEnvString("PEERDB_OAUTH_ISSUER_URL", "")
	oauthDiscoveryEnabledString := GetEnvString("PEERDB_OAUTH_DISCOVERY_ENABLED", "false")
	oauthDiscoveryEnabled, err := strconv.ParseBool(oauthDiscoveryEnabledString)
	if err != nil {
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
