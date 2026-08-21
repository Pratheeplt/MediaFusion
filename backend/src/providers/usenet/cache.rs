/// Redis caching for resolved usenet playback URLs.
///
/// Key: `usenet_provider_` + hex(SHA256(`{secret}_{provider}_{nzb_guid}_{season}_{episode}`))
/// TTL: 1 hour — long enough to survive a Stremio retry loop, short enough
///      to pick up renewed TorBox/Debrider links after expiry.
///
/// The provider name MUST be part of the key: the same `nzb_guid` can be
/// resolved through different providers, and each provider's resolved URL
/// must be cached independently (otherwise a request for provider A can be
/// served a stale URL that was resolved for provider B).
use fred::prelude::{Expiration, KeysInterface};
use sha2::{Digest, Sha256};

const KEY_PREFIX: &str = "usenet_provider_";
pub const TTL: i64 = 3600;

pub fn cache_key(secret: &str, provider_name: &str, nzb_guid: &str, season: i32, episode: i32) -> String {
    let provider = provider_name.to_lowercase();
    let raw = format!("{secret}_{provider}_{nzb_guid}_{season}_{episode}");
    let hex: String = Sha256::digest(raw.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    format!("{KEY_PREFIX}{hex}")
}

pub async fn get(redis: &fred::clients::Client, key: &str) -> Option<String> {
    redis.get::<Option<String>, _>(key).await.ok().flatten()
}

pub async fn set(redis: &fred::clients::Client, key: &str, url: &str) {
    let _ = redis
        .set::<(), _, _>(key, url, Some(Expiration::EX(TTL)), None, false)
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_key_varies_by_provider() {
        let easynews = cache_key("secret", "easynews", "guid", 0, 0);
        let torbox = cache_key("secret", "torbox", "guid", 0, 0);
        assert_ne!(easynews, torbox);
    }

    #[test]
    fn cache_key_is_case_insensitive_on_provider() {
        let lower = cache_key("secret", "easynews", "guid", 0, 0);
        let upper = cache_key("secret", "EasyNews", "guid", 0, 0);
        assert_eq!(lower, upper);
    }
}
