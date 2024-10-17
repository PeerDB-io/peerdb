use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use base64::prelude::{Engine as _, BASE64_STANDARD};
use jsonwebtoken::{encode as jwt_encode, Algorithm, EncodingKey, Header};
use rsa::pkcs1::EncodeRsaPrivateKey;
use rsa::pkcs8::{DecodePrivateKey, EncodePublicKey};
use rsa::RsaPrivateKey;
use secrecy::SecretString;
use serde::Serialize;
use sha2::{Digest, Sha256};
use tracing::info;

#[derive(Debug, Serialize)]
struct JwtClaims {
    iss: String,
    sub: String,
    iat: u64,
    exp: u64,
}

#[derive(Clone)]
pub struct SnowflakeAuth {
    account_id: String,
    normalized_account_id: String,
    username: String,
    private_key: RsaPrivateKey,
    public_key_fp: Option<String>,
    refresh_threshold: u64,
    expiry_threshold: u64,
    last_refreshed: u64,
    current_jwt: Option<SecretString>,
}

impl SnowflakeAuth {
    #[tracing::instrument(name = "peer_sflake::init_client_auth", skip_all)]
    pub fn new(
        account_id: String,
        username: String,
        private_key: &str,
        password: Option<&str>,
        refresh_threshold: u64,
        expiry_threshold: u64,
    ) -> anyhow::Result<Self> {
        let pkey = match password {
            Some(pw) => DecodePrivateKey::from_pkcs8_encrypted_pem(private_key, pw)
                .context("Invalid private key or decryption failed")?,
            None => DecodePrivateKey::from_pkcs8_pem(private_key).context("Invalid private key")?,
        };
        let mut snowflake_auth: SnowflakeAuth = SnowflakeAuth {
            // moved normalized_account_id above account_id to satisfy the borrow checker.
            normalized_account_id: SnowflakeAuth::normalize_account_identifier(&account_id),
            account_id,
            username,
            private_key: pkey,
            public_key_fp: None,
            refresh_threshold,
            expiry_threshold,
            last_refreshed: 0,
            current_jwt: None,
        };
        snowflake_auth.public_key_fp = Some(SnowflakeAuth::gen_public_key_fp(
            &snowflake_auth.private_key,
        )?);
        snowflake_auth.refresh_jwt()?;

        Ok(snowflake_auth)
    }

    // Normalize the account identifer to a form that is embedded into the JWT.
    // Logic adapted from Snowflake's example Python code for key-pair authentication "sql-api-generate-jwt.py".
    fn normalize_account_identifier(raw_account: &str) -> String {
        let split_index = if !raw_account.contains(".global") {
            *raw_account
                .find('.')
                .get_or_insert(raw_account.chars().count())
        } else {
            *raw_account
                .find('-')
                .get_or_insert(raw_account.chars().count())
        };
        raw_account
            .chars()
            .flat_map(char::to_uppercase)
            .take(split_index)
            .collect()
    }

    #[tracing::instrument(name = "peer_sflake::gen_public_key_fp", skip_all)]
    fn gen_public_key_fp(private_key: &RsaPrivateKey) -> anyhow::Result<String> {
        let public_key = private_key.to_public_key().to_public_key_der()?;
        let res = format!(
            "SHA256:{}",
            BASE64_STANDARD.encode(Sha256::new_with_prefix(public_key.as_bytes()).finalize())
        );
        Ok(res)
    }

    #[tracing::instrument(name = "peer_sflake::auth_refresh_jwt", skip_all)]
    fn refresh_jwt(&mut self) -> anyhow::Result<()> {
        let private_key_jwt: EncodingKey =
            EncodingKey::from_rsa_der(self.private_key.to_pkcs1_der()?.as_bytes());
        self.last_refreshed = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        info!(
            "Refreshing SnowFlake JWT for account: {} and user: {} at time {}",
            self.account_id, self.username, self.last_refreshed
        );
        let jwt_claims: JwtClaims = JwtClaims {
            iss: format!(
                "{}.{}.{}",
                self.normalized_account_id,
                self.username.to_uppercase(),
                self.public_key_fp
                    .as_deref()
                    .context("No public key fingerprint")?
            ),
            sub: format!(
                "{}.{}",
                self.normalized_account_id,
                self.username.to_uppercase()
            ),
            iat: self.last_refreshed,
            exp: self.last_refreshed + self.expiry_threshold,
        };
        let header: Header = Header::new(Algorithm::RS256);

        let encoded_jwt = jwt_encode(&header, &jwt_claims, &private_key_jwt)?;
        let secret = SecretString::from(encoded_jwt);

        self.current_jwt = Some(secret);

        Ok(())
    }

    pub fn get_jwt(&mut self) -> anyhow::Result<&SecretString> {
        if SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs()
            >= (self.last_refreshed + self.refresh_threshold)
        {
            self.refresh_jwt()?;
        }

        self.current_jwt
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("JWT not initialized. Please call refresh_jwt() first."))
    }
}
