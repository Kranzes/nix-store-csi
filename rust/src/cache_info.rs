use anyhow::Context;
use url::Url;

/// Nix's default.
pub const DEFAULT_PRIORITY: u32 = 50;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct CacheInfo {
    pub store_dir: Option<String>,
    pub priority: Option<u32>,
}

pub fn parse(text: &str) -> anyhow::Result<CacheInfo> {
    let mut info = CacheInfo::default();
    for line in text.lines() {
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        let value = value.trim();
        match key.trim() {
            "StoreDir" => info.store_dir = Some(value.to_owned()),
            "Priority" => {
                info.priority = Some(
                    value
                        .parse()
                        .with_context(|| format!("Priority {value:?}"))?,
                );
            }
            _ => {}
        }
    }
    Ok(info)
}

/// Lower goes first, and the URL's `?priority=` beats `nix-cache-info`, as in Nix.
pub fn priority(url: &Url, info: &CacheInfo) -> anyhow::Result<u32> {
    let from_url = url.query_pairs().find(|(k, _)| k == "priority");
    Ok(match from_url {
        Some((_, value)) => value
            .parse()
            .with_context(|| format!("priority {value:?}"))?,
        None => info.priority.unwrap_or(DEFAULT_PRIORITY),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses() {
        let info = parse("StoreDir: /nix/store\nWantMassQuery: 1\nPriority: 40\n").unwrap();
        assert_eq!(
            info,
            CacheInfo {
                store_dir: Some("/nix/store".into()),
                priority: Some(40),
            }
        );
        assert_eq!(parse("").unwrap(), CacheInfo::default());
        assert!(parse("Priority: high").is_err());
    }

    #[test]
    fn picks_priority() {
        let info = CacheInfo {
            store_dir: None,
            priority: Some(40),
        };
        let priority = |url: &str, info: &CacheInfo| priority(&url.parse().unwrap(), info);
        assert_eq!(priority("https://c", &info).unwrap(), 40);
        assert_eq!(priority("https://c?priority=10", &info).unwrap(), 10);
        assert_eq!(priority("https://c", &CacheInfo::default()).unwrap(), 50);
        assert!(priority("https://c?priority=x", &info).is_err());
    }
}
