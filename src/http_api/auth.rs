use std::{collections::HashSet, sync::Arc};

use axum::{
    extract::State,
    http::{HeaderMap, header::AUTHORIZATION},
    middleware::Next,
    response::{IntoResponse, Response},
};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::Deserialize;

use super::{ApiError, api_error};

pub(super) const AUDIENCE: &str = "lqepoch-market-data";
const TERMINAL_ISSUER: &str = "eqoboard-openterminal";
const RESEARCH_ISSUER: &str = "openterminal-research";
const TERMINAL_KID: &str = "mdp-terminal";
const RESEARCH_KID: &str = "mdp-research";
const MAX_TOKEN_BYTES: usize = 8 * 1024;
const MAX_TOKEN_LIFETIME_SECS: u64 = 60;

#[derive(Clone)]
pub struct AuthConfig {
    terminal_key: Arc<[u8]>,
    research_key: Arc<[u8]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AuthFailure {
    Unauthorized,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Claims {
    iss: String,
    aud: serde_json::Value,
    sub: String,
    idp_iss: String,
    jti: String,
    iat: u64,
    exp: u64,
    scope: Vec<String>,
}

impl AuthConfig {
    pub fn from_environment() -> crate::Result<Option<Self>> {
        let terminal = read_secret("MDP_TERMINAL_JWT_SECRET")?;
        let research = read_secret("MDP_RESEARCH_JWT_SECRET")?;
        match (terminal, research) {
            (None, None) => Ok(None),
            (Some(terminal), Some(research))
                if terminal.len() >= 32 && research.len() >= 32 && terminal != research =>
            {
                Ok(Some(Self {
                    terminal_key: Arc::from(terminal),
                    research_key: Arc::from(research),
                }))
            }
            _ => Err(crate::MarketDataError::InvalidInput),
        }
    }

    fn authenticate(&self, headers: &HeaderMap) -> std::result::Result<(), AuthFailure> {
        let authorization = headers
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .ok_or(AuthFailure::Unauthorized)?;
        let token = authorization
            .strip_prefix("Bearer ")
            .filter(|token| {
                !token.is_empty()
                    && token.len() <= MAX_TOKEN_BYTES
                    && !token.bytes().any(|byte| byte.is_ascii_whitespace())
            })
            .ok_or(AuthFailure::Unauthorized)?;
        let header = decode_header(token).map_err(|_| AuthFailure::Unauthorized)?;
        if header.alg != Algorithm::HS256 || header.typ.as_deref() != Some("JWT") {
            return Err(AuthFailure::Unauthorized);
        }
        let (expected_issuer, key) = match header.kid.as_deref() {
            Some(TERMINAL_KID) => (TERMINAL_ISSUER, self.terminal_key.as_ref()),
            Some(RESEARCH_KID) => (RESEARCH_ISSUER, self.research_key.as_ref()),
            _ => return Err(AuthFailure::Unauthorized),
        };
        let mut validation = Validation::new(Algorithm::HS256);
        validation.leeway = 0;
        validation.validate_exp = true;
        validation.validate_nbf = true;
        validation.set_audience(&[AUDIENCE]);
        validation.set_issuer(&[expected_issuer]);
        validation.required_spec_claims = HashSet::from([
            "exp".to_owned(),
            "iss".to_owned(),
            "aud".to_owned(),
            "sub".to_owned(),
            "iat".to_owned(),
            "jti".to_owned(),
        ]);
        let token_data = decode::<Claims>(token, &DecodingKey::from_secret(key), &validation)
            .map_err(|_| AuthFailure::Unauthorized)?;
        let claims = token_data.claims;
        let now = jsonwebtoken::get_current_timestamp();
        if claims.iss != expected_issuer
            || claims.aud != serde_json::Value::String(AUDIENCE.to_owned())
            || claims.scope.len() != 1
            || claims
                .scope
                .first()
                .is_none_or(|scope| scope != "market:read")
            || claims.exp <= claims.iat
            || claims.exp.saturating_sub(claims.iat) > MAX_TOKEN_LIFETIME_SECS
            || claims.iat > now
            || claims.sub.is_empty()
            || claims.sub.len() > 256
            || claims.idp_iss.is_empty()
            || claims.idp_iss.len() > 512
            || claims.jti.is_empty()
            || claims.jti.len() > 128
            || [
                claims.sub.as_str(),
                claims.idp_iss.as_str(),
                claims.jti.as_str(),
            ]
            .iter()
            .any(|value| value.chars().any(char::is_control))
        {
            return Err(AuthFailure::Unauthorized);
        }
        Ok(())
    }
}

fn read_secret(name: &str) -> crate::Result<Option<Vec<u8>>> {
    match std::env::var(name) {
        Ok(value) if !value.is_empty() => Ok(Some(value.into_bytes())),
        Ok(_) => Err(crate::MarketDataError::InvalidInput),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(crate::MarketDataError::InvalidInput),
    }
}

pub(super) async fn require_market_read(
    State(auth): State<Option<AuthConfig>>,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    let Some(auth) = auth else {
        return api_error(ApiError::Unavailable).into_response();
    };
    if auth.authenticate(request.headers()).is_err() {
        return api_error(ApiError::Unauthorized).into_response();
    }
    next.run(request).await
}

#[cfg(test)]
pub(super) mod test_support {
    use std::time::{SystemTime, UNIX_EPOCH};

    use jsonwebtoken::{EncodingKey, Header, encode};
    use serde::Serialize;

    use super::*;

    const TERMINAL_SECRET: &[u8] = b"terminal-test-key-is-independent-and-at-least-32-bytes";
    const RESEARCH_SECRET: &[u8] = b"research-test-key-is-independent-and-at-least-32";

    #[derive(Serialize)]
    struct TestClaims<'a> {
        iss: &'a str,
        aud: &'a str,
        sub: &'a str,
        idp_iss: &'a str,
        jti: &'a str,
        iat: u64,
        exp: u64,
        scope: Vec<&'a str>,
    }

    pub(in crate::http_api) fn auth_config() -> AuthConfig {
        AuthConfig {
            terminal_key: Arc::from(TERMINAL_SECRET),
            research_key: Arc::from(RESEARCH_SECRET),
        }
    }

    pub(in crate::http_api) fn token(
        issuer: &str,
        kid: &str,
        secret: &[u8],
        scope: &str,
    ) -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        token_with_claims(issuer, AUDIENCE, kid, secret, now, now + 30, &[scope])
    }

    pub(in crate::http_api) fn token_with_claims(
        issuer: &str,
        audience: &str,
        kid: &str,
        secret: &[u8],
        issued_at: u64,
        expires_at: u64,
        scopes: &[&str],
    ) -> String {
        let mut header = Header::new(Algorithm::HS256);
        header.kid = Some(kid.to_owned());
        header.typ = Some("JWT".to_owned());
        encode(
            &header,
            &TestClaims {
                iss: issuer,
                aud: audience,
                sub: "test-user",
                idp_iss: "https://identity.example.test",
                jti: "request-id-1",
                iat: issued_at,
                exp: expires_at,
                scope: scopes.to_vec(),
            },
            &EncodingKey::from_secret(secret),
        )
        .unwrap()
    }

    pub(in crate::http_api) fn terminal_token(scope: &str) -> String {
        token(TERMINAL_ISSUER, TERMINAL_KID, TERMINAL_SECRET, scope)
    }

    pub(in crate::http_api) fn research_token(scope: &str) -> String {
        token(RESEARCH_ISSUER, RESEARCH_KID, RESEARCH_SECRET, scope)
    }
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, HeaderValue, header::AUTHORIZATION};

    use super::{AuthFailure, test_support};

    const TERMINAL_ISSUER: &str = "eqoboard-openterminal";
    const RESEARCH_ISSUER: &str = "openterminal-research";
    const TERMINAL_KID: &str = "mdp-terminal";
    const RESEARCH_KID: &str = "mdp-research";
    const TERMINAL_SECRET: &[u8] = b"terminal-test-key-is-independent-and-at-least-32-bytes";
    const RESEARCH_SECRET: &[u8] = b"research-test-key-is-independent-and-at-least-32";

    fn authenticate(token: &str) -> std::result::Result<(), AuthFailure> {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        test_support::auth_config().authenticate(&headers)
    }

    #[test]
    fn accepts_only_short_lived_exact_market_read_delegations_bound_to_issuer_kid_and_key() {
        let now = jsonwebtoken::get_current_timestamp();
        for (issuer, kid, key) in [
            (TERMINAL_ISSUER, TERMINAL_KID, TERMINAL_SECRET),
            (RESEARCH_ISSUER, RESEARCH_KID, RESEARCH_SECRET),
        ] {
            let token = test_support::token_with_claims(
                issuer,
                super::AUDIENCE,
                kid,
                key,
                now,
                now + 60,
                &["market:read"],
            );
            assert!(authenticate(&token).is_ok());
        }

        let invalid = [
            test_support::token_with_claims(
                "wrong-issuer",
                super::AUDIENCE,
                TERMINAL_KID,
                TERMINAL_SECRET,
                now,
                now + 30,
                &["market:read"],
            ),
            test_support::token_with_claims(
                TERMINAL_ISSUER,
                "eqoboard-gateway",
                TERMINAL_KID,
                TERMINAL_SECRET,
                now,
                now + 30,
                &["market:read"],
            ),
            test_support::token_with_claims(
                TERMINAL_ISSUER,
                super::AUDIENCE,
                "gateway-bff",
                TERMINAL_SECRET,
                now,
                now + 30,
                &["market:read"],
            ),
            test_support::token_with_claims(
                TERMINAL_ISSUER,
                super::AUDIENCE,
                TERMINAL_KID,
                RESEARCH_SECRET,
                now,
                now + 30,
                &["market:read"],
            ),
            test_support::token_with_claims(
                TERMINAL_ISSUER,
                super::AUDIENCE,
                TERMINAL_KID,
                TERMINAL_SECRET,
                now,
                now + 61,
                &["market:read"],
            ),
            test_support::token_with_claims(
                TERMINAL_ISSUER,
                super::AUDIENCE,
                TERMINAL_KID,
                TERMINAL_SECRET,
                now + 1,
                now + 30,
                &["market:read"],
            ),
            test_support::token_with_claims(
                TERMINAL_ISSUER,
                super::AUDIENCE,
                TERMINAL_KID,
                TERMINAL_SECRET,
                now,
                now + 30,
                &["market:read", "orders:read"],
            ),
        ];
        for token in invalid {
            assert_eq!(authenticate(&token), Err(AuthFailure::Unauthorized));
        }
    }
}
