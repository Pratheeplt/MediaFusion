/// Streaming provider debrid cache status and submission.
///
/// Routes:
///   POST /streaming_provider/cache/status
///   POST /streaming_provider/cache/submit
///
/// These mirror Python's cache_helpers: Redis hash `debrid_cache:{service}`
/// stores info_hash → unix expiry timestamp.
use std::sync::Arc;

use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Redirect, Response},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::Utc;
use fred::prelude::HashesInterface;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use uuid::Uuid;

use crate::{
    providers::torrents::realdebrid, state::AppState, util::http as http_util, util::retry,
};

const CACHE_KEY_PREFIX: &str = "debrid_cache:";
const EXPIRY_DAYS_SECS: i64 = 7 * 86400;

const DEBRIDLINK_CLIENT_ID: &str = "RyrV22FOg30DsxjYPziRKA";

#[derive(Deserialize)]
pub struct CacheStatusRequest {
    pub service: String,
    pub info_hashes: Vec<String>,
}

#[derive(Serialize)]
pub struct CacheStatusResponse {
    pub cached_status: std::collections::HashMap<String, bool>,
}

#[derive(Deserialize)]
pub struct CacheSubmitRequest {
    pub service: String,
    pub info_hashes: Vec<String>,
}

#[derive(Serialize)]
pub struct CacheSubmitResponse {
    pub success: bool,
    pub message: String,
}

pub async fn check_cache_status(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CacheStatusRequest>,
) -> impl IntoResponse {
    if req.info_hashes.is_empty() {
        return Json(CacheStatusResponse {
            cached_status: std::collections::HashMap::new(),
        });
    }

    let service = normalize_service(&req.service);
    let cache_key = format!("{CACHE_KEY_PREFIX}{service}");
    let now = Utc::now().timestamp();

    let fields: Vec<String> = req.info_hashes.clone();
    let timestamps: Vec<Option<String>> = state
        .redis
        .hmget(&cache_key, fields)
        .await
        .unwrap_or_else(|_| vec![None; req.info_hashes.len()]);

    let mut cached_status = std::collections::HashMap::new();
    let mut expired: Vec<String> = Vec::new();

    for (hash, ts_opt) in req.info_hashes.iter().zip(timestamps.iter()) {
        match ts_opt {
            Some(ts_str) => {
                let expiry: i64 = ts_str.parse().unwrap_or(0);
                if expiry > now {
                    cached_status.insert(hash.clone(), true);
                } else {
                    expired.push(hash.clone());
                    cached_status.insert(hash.clone(), false);
                }
            }
            None => {
                cached_status.insert(hash.clone(), false);
            }
        }
    }

    // Clean up expired entries (best-effort)
    if !expired.is_empty() {
        let _ = state.redis.hdel::<(), _, _>(&cache_key, expired).await;
    }

    Json(CacheStatusResponse { cached_status })
}

pub async fn submit_cached_hashes(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CacheSubmitRequest>,
) -> impl IntoResponse {
    if req.info_hashes.is_empty() {
        return (
            StatusCode::OK,
            Json(CacheSubmitResponse {
                success: true,
                message: "No info hashes provided".into(),
            }),
        );
    }

    let service = normalize_service(&req.service);
    let cache_key = format!("{CACHE_KEY_PREFIX}{service}");
    let expiry_ts = (Utc::now().timestamp() + EXPIRY_DAYS_SECS).to_string();

    // Build mapping: info_hash → expiry timestamp
    let mapping: Vec<(String, String)> = req
        .info_hashes
        .iter()
        .map(|h| (h.clone(), expiry_ts.clone()))
        .collect();

    let result = state.redis.hset::<(), _, _>(&cache_key, mapping).await;

    match result {
        Ok(_) => (
            StatusCode::OK,
            Json(CacheSubmitResponse {
                success: true,
                message: format!(
                    "Stored {} cached info hashes for {}",
                    req.info_hashes.len(),
                    service
                ),
            }),
        ),
        Err(e) => {
            tracing::error!("submit_cached_hashes Redis error: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(CacheSubmitResponse {
                    success: false,
                    message: "Error storing cached info hashes".into(),
                }),
            )
        }
    }
}

/// StremThru uses the store name as service name when set.
fn normalize_service(service: &str) -> &str {
    service
}

// ── Provider OAuth / device-code auth ────────────────────────────────────────

/// GET /streaming_provider/realdebrid/get-device-code
pub async fn realdebrid_get_device_code(State(state): State<Arc<AppState>>) -> Response {
    let url = format!(
        "https://api.real-debrid.com/oauth/v2/device/code?client_id={}&new_credentials=yes",
        realdebrid::OPENSOURCE_CLIENT_ID
    );
    match retry::with_transport_retry("realdebrid_get_device_code", || {
        state.http_for_provider("realdebrid").get(&url).send()
    })
    .await
    {
        Ok(resp) => {
            let status = resp.status();
            match resp.json::<Value>().await {
                Ok(body) => (
                    StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::OK),
                    Json(body),
                )
                    .into_response(),
                Err(e) => {
                    tracing::error!("realdebrid_get_device_code: parse error: {e}");
                    (
                        StatusCode::BAD_GATEWAY,
                        Json(serde_json::json!({"detail": "Invalid response from Real-Debrid"})),
                    )
                        .into_response()
                }
            }
        }
        Err(e) => {
            tracing::error!(
                error_kind = http_util::transport_error_kind(&e),
                root_cause = http_util::root_cause(&e),
                "realdebrid_get_device_code: request error: {e}"
            );
            (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({"detail": "Failed to contact Real-Debrid"})),
            )
                .into_response()
        }
    }
}

/// POST /streaming_provider/realdebrid/authorize
pub async fn realdebrid_authorize(
    State(state): State<Arc<AppState>>,
    Json(body): Json<Value>,
) -> Response {
    let device_code = match body.get("device_code").and_then(|v| v.as_str()) {
        Some(c) => c.to_string(),
        None => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(serde_json::json!({"detail": "Missing device_code"})),
            )
                .into_response();
        }
    };

    match realdebrid::authorize_device_code(state.http_for_provider("realdebrid"), &device_code)
        .await
    {
        Ok(json_body) => (StatusCode::OK, Json(json_body)).into_response(),
        Err(e) => {
            tracing::error!("realdebrid_authorize: {e}");
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "error": e.to_string(),
                    "message": e.to_string(),
                })),
            )
                .into_response()
        }
    }
}

/// GET /streaming_provider/debridlink/get-device-code
pub async fn debridlink_get_device_code(State(state): State<Arc<AppState>>) -> Response {
    let url = "https://debrid-link.com/api/oauth/device/code";
    let payload = serde_json::json!({
        "client_id": DEBRIDLINK_CLIENT_ID,
        "scope": "get.post.downloader get.post.seedbox get.account get.files get.post.stream",
    });
    match state
        .http_for_provider("debridlink")
        .post(url)
        .json(&payload)
        .send()
        .await
    {
        Ok(resp) => {
            let status = resp.status();
            match resp.json::<Value>().await {
                Ok(body) => (
                    StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::OK),
                    Json(body),
                )
                    .into_response(),
                Err(e) => {
                    tracing::error!("debridlink_get_device_code: parse error: {e}");
                    (
                        StatusCode::BAD_GATEWAY,
                        Json(serde_json::json!({"detail": "Invalid response from DebridLink"})),
                    )
                        .into_response()
                }
            }
        }
        Err(e) => {
            tracing::error!("debridlink_get_device_code: request error: {e}");
            (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({"detail": "Failed to contact DebridLink"})),
            )
                .into_response()
        }
    }
}

/// POST /streaming_provider/debridlink/authorize
pub async fn debridlink_authorize(
    State(state): State<Arc<AppState>>,
    Json(body): Json<Value>,
) -> Response {
    let device_code = match body.get("device_code").and_then(|v| v.as_str()) {
        Some(c) => c.to_string(),
        None => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(serde_json::json!({"detail": "Missing device_code"})),
            )
                .into_response();
        }
    };

    let url = "https://debrid-link.com/api/oauth/token";
    let payload = serde_json::json!({
        "client_id": DEBRIDLINK_CLIENT_ID,
        "code": device_code,
        "grant_type": "http://oauth.net/grant_type/device/1.0",
    });
    match state
        .http_for_provider("debridlink")
        .post(url)
        .json(&payload)
        .send()
        .await
    {
        Ok(resp) => {
            let status = resp.status();
            match resp.json::<Value>().await {
                Ok(json_body) => {
                    if status.is_success() {
                        // Extract refresh_token and base64-url-no-pad encode it
                        if let Some(refresh_token) =
                            json_body.get("refresh_token").and_then(|v| v.as_str())
                        {
                            let encoded = URL_SAFE_NO_PAD.encode(refresh_token.as_bytes());
                            return (StatusCode::OK, Json(serde_json::json!({"token": encoded})))
                                .into_response();
                        }
                        tracing::warn!("debridlink_authorize: no refresh_token in response");
                    }

                    // Return 200 with the provider body for pending/expired OAuth states so the
                    // frontend keeps polling (matches Python DebridLink.authorize behavior).
                    (StatusCode::OK, Json(json_body)).into_response()
                }
                Err(e) => {
                    tracing::error!("debridlink_authorize: parse error: {e}");
                    (
                        StatusCode::BAD_GATEWAY,
                        Json(serde_json::json!({"detail": "Invalid response from DebridLink"})),
                    )
                        .into_response()
                }
            }
        }
        Err(e) => {
            tracing::error!("debridlink_authorize: request error: {e}");
            (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({"detail": "Failed to contact DebridLink"})),
            )
                .into_response()
        }
    }
}

#[derive(Deserialize)]
pub struct PremiumizeAuthorizeQuery {
    pub state: Option<String>,
}

#[derive(Deserialize)]
pub struct PremiumizeOAuthRedirectQuery {
    pub code: Option<String>,
    pub state: Option<String>,
    pub error: Option<String>,
    pub error_description: Option<String>,
}

fn premiumize_oauth_redirect_uri(host_url: &str) -> String {
    format!("{host_url}/streaming_provider/premiumize/oauth2_redirect")
}

fn premiumize_oauth_result_page(
    state: Option<&str>,
    token: Option<&str>,
    error: Option<&str>,
) -> Response {
    let success = token.is_some();
    let title = if success {
        "Premiumize Authorized"
    } else {
        "Premiumize Authorization Failed"
    };
    let heading = if success {
        "Premiumize authorized"
    } else {
        "Premiumize authorization failed"
    };
    let display_message = if success {
        "Your Premiumize credentials were sent to the configuration page. This tab will close automatically."
    } else {
        error.unwrap_or("Premiumize authorization failed.")
    };

    let message = serde_json::json!({
        "type": "mediafusion:premiumize-oauth",
        "status": if success { "success" } else { "error" },
        "state": state.unwrap_or(""),
        "token": token,
        "error": error,
    });
    let message_json = serde_json::to_string(&message)
        .unwrap_or_else(|_| "{}".to_string())
        .replace('&', "\\u0026")
        .replace('<', "\\u003c")
        .replace('>', "\\u003e");

    let escaped = display_message
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;");

    (
        StatusCode::OK,
        [
            (axum::http::header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (axum::http::header::CACHE_CONTROL, "no-store"),
        ],
        format!(
            "<!DOCTYPE html><html><head><title>{title}</title></head>\
             <body><h1>{heading}</h1><p>{escaped}</p>\
             <p>If this tab does not close, return to Configure and try again.</p>\
             <script>\
             const result = {message_json};\
             const deliver = () => {{\
               if (window.opener && !window.opener.closed) {{\
                 window.opener.postMessage(result, '*');\
               }}\
             }};\
             deliver();\
             window.setInterval(deliver, 500);\
             </script></body></html>"
        ),
    )
        .into_response()
}

fn premiumize_oauth_error_page(state: Option<&str>, message: &str) -> Response {
    premiumize_oauth_result_page(state, None, Some(message))
}

fn premiumize_oauth_success_page(state: Option<&str>, token: &str) -> Response {
    premiumize_oauth_result_page(state, Some(token), None)
}

/// GET /streaming_provider/premiumize/authorize
pub async fn premiumize_authorize(
    State(state): State<Arc<AppState>>,
    Query(params): Query<PremiumizeAuthorizeQuery>,
) -> Response {
    let client_id = state
        .config
        .premiumize_oauth_client_id
        .as_deref()
        .unwrap_or("");

    if client_id.is_empty() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"detail": "Premiumize OAuth client ID not configured. Set PREMIUMIZE_OAUTH_CLIENT_ID environment variable."})),
        )
            .into_response();
    }

    let redirect_uri = premiumize_oauth_redirect_uri(&state.config.host_url);
    let oauth_state = params
        .state
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| Uuid::new_v4().simple().to_string());

    let url = format!(
        "https://www.premiumize.me/authorize?client_id={}&response_type=code&redirect_uri={}&state={}",
        client_id,
        urlencoding::encode(&redirect_uri),
        urlencoding::encode(&oauth_state),
    );

    Redirect::temporary(&url).into_response()
}

/// GET /streaming_provider/premiumize/oauth2_redirect
pub async fn premiumize_oauth2_redirect(
    State(state): State<Arc<AppState>>,
    Query(params): Query<PremiumizeOAuthRedirectQuery>,
) -> Response {
    let oauth_state = params.state.clone();

    if let Some(error) = params.error.filter(|s| !s.is_empty()) {
        let description = params
            .error_description
            .filter(|s| !s.is_empty())
            .unwrap_or(error);
        return premiumize_oauth_error_page(
            oauth_state.as_deref(),
            &format!("Premiumize authorization failed: {description}"),
        );
    }

    let code = match params.code.filter(|s| !s.is_empty()) {
        Some(code) => code,
        None => {
            return premiumize_oauth_error_page(
                oauth_state.as_deref(),
                "Missing authorization code from Premiumize.",
            );
        }
    };

    let client_id = state
        .config
        .premiumize_oauth_client_id
        .as_deref()
        .unwrap_or("");
    let client_secret = state
        .config
        .premiumize_oauth_client_secret
        .as_deref()
        .unwrap_or("");

    if client_id.is_empty() || client_secret.is_empty() {
        return premiumize_oauth_error_page(
            oauth_state.as_deref(),
            "Premiumize OAuth is not configured on this server. Set PREMIUMIZE_OAUTH_CLIENT_ID and PREMIUMIZE_OAUTH_CLIENT_SECRET.",
        );
    }

    let redirect_uri = premiumize_oauth_redirect_uri(&state.config.host_url);
    let token_resp = match state
        .http_for_provider("premiumize")
        .post("https://www.premiumize.me/token")
        .form(&[
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("code", code.as_str()),
            ("grant_type", "authorization_code"),
            ("redirect_uri", redirect_uri.as_str()),
        ])
        .send()
        .await
    {
        Ok(resp) => resp,
        Err(e) => {
            tracing::error!("premiumize_oauth2_redirect: token request error: {e}");
            return premiumize_oauth_error_page(
                oauth_state.as_deref(),
                "Failed to contact Premiumize. Please try again.",
            );
        }
    };

    let token_body: Value = match token_resp.json().await {
        Ok(body) => body,
        Err(e) => {
            tracing::error!("premiumize_oauth2_redirect: token parse error: {e}");
            return premiumize_oauth_error_page(
                oauth_state.as_deref(),
                "Invalid response from Premiumize.",
            );
        }
    };

    if let Some(oauth_error) = token_body.get("error").and_then(|v| v.as_str()) {
        let message = match oauth_error {
            "invalid_grant" => {
                "That Premiumize login link expired or was already used. Open Configure and connect Premiumize again."
            }
            "invalid_client" => {
                "Premiumize OAuth client credentials are invalid. Check Premiumize OAuth settings on the server."
            }
            other => other,
        };
        tracing::warn!("premiumize_oauth2_redirect: token exchange failed: {message}");
        return premiumize_oauth_error_page(oauth_state.as_deref(), message);
    }

    let access_token = match token_body
        .get("access_token")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
    {
        Some(token) => token,
        None => {
            tracing::warn!("premiumize_oauth2_redirect: missing access_token in response");
            return premiumize_oauth_error_page(
                oauth_state.as_deref(),
                "Premiumize authorization completed, but no access token was returned.",
            );
        }
    };

    let encoded_token = crate::providers::torrents::premiumize::encode_oauth_token(access_token);
    premiumize_oauth_success_page(oauth_state.as_deref(), &encoded_token)
}
