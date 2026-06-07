// src/core/warden/config.rs

use anyhow::Result;
use serde::Deserialize;
use std::time::Duration;
use tokio::fs;

#[derive(Debug, Clone, Deserialize)]
pub struct WardenConfig {
    #[serde(default = "default_host")]
    pub host: String,

    #[serde(default = "default_port")]
    pub port: u16,

    pub announce_ip: Option<String>,

    pub masters: Vec<MonitoredMaster>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MonitoredMaster {
    pub name: String,
    pub ip: String,
    pub port: u16,
    pub quorum: usize,

    #[serde(with = "humantime_serde")]
    pub down_after: Duration,

    #[serde(with = "humantime_serde")]
    pub failover_timeout: Duration,

    #[serde(with = "humantime_serde", default = "default_hello_interval")]
    pub hello_interval: Duration,
}

fn default_host() -> String {
    "0.0.0.0".to_string()
}

fn default_port() -> u16 {
    26379
}

fn default_hello_interval() -> Duration {
    Duration::from_secs(2)
}

impl WardenConfig {
    pub async fn from_file(path: &str) -> Result<Self> {
        let content = fs::read_to_string(path).await?;
        let config: WardenConfig = toml::from_str(&content)?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tempfile::NamedTempFile;

    #[test]
    fn test_default_host() {
        assert_eq!(default_host(), "0.0.0.0");
    }

    #[test]
    fn test_default_port_is_sentinel_default() {
        assert_eq!(default_port(), 26379);
    }

    #[test]
    fn test_default_hello_interval_is_2_seconds() {
        assert_eq!(default_hello_interval(), Duration::from_secs(2));
    }

    #[test]
    fn test_from_str_minimal_config_uses_defaults() {
        let toml_src = r#"
            [[masters]]
            name = "mymaster"
            ip = "127.0.0.1"
            port = 6379
            quorum = 2
            down_after = "30s"
            failover_timeout = "180s"
        "#;
        let cfg: WardenConfig = toml::from_str(toml_src).unwrap();
        assert_eq!(cfg.host, "0.0.0.0");
        assert_eq!(cfg.port, 26379);
        assert!(cfg.announce_ip.is_none());
        assert_eq!(cfg.masters.len(), 1);
        let m = &cfg.masters[0];
        assert_eq!(m.name, "mymaster");
        assert_eq!(m.ip, "127.0.0.1");
        assert_eq!(m.port, 6379);
        assert_eq!(m.quorum, 2);
        assert_eq!(m.down_after, Duration::from_secs(30));
        assert_eq!(m.failover_timeout, Duration::from_secs(180));
        // hello_interval has a default applied at deserialize time.
        assert_eq!(m.hello_interval, Duration::from_secs(2));
    }

    #[test]
    fn test_from_str_overrides_all_defaults() {
        let toml_src = r#"
            host = "10.0.0.5"
            port = 4000
            announce_ip = "10.0.0.6"
            [[masters]]
            name = "primary"
            ip = "10.0.0.1"
            port = 6379
            quorum = 3
            down_after = "5s"
            failover_timeout = "60s"
            hello_interval = "500ms"
        "#;
        let cfg: WardenConfig = toml::from_str(toml_src).unwrap();
        assert_eq!(cfg.host, "10.0.0.5");
        assert_eq!(cfg.port, 4000);
        assert_eq!(cfg.announce_ip.as_deref(), Some("10.0.0.6"));
        let m = &cfg.masters[0];
        assert_eq!(m.quorum, 3);
        assert_eq!(m.down_after, Duration::from_secs(5));
        assert_eq!(m.failover_timeout, Duration::from_secs(60));
        assert_eq!(m.hello_interval, Duration::from_millis(500));
    }

    #[test]
    fn test_from_str_multiple_masters() {
        let toml_src = r#"
            [[masters]]
            name = "a"
            ip = "10.0.0.1"
            port = 6379
            quorum = 2
            down_after = "30s"
            failover_timeout = "180s"
            [[masters]]
            name = "b"
            ip = "10.0.0.2"
            port = 6380
            quorum = 1
            down_after = "10s"
            failover_timeout = "60s"
        "#;
        let cfg: WardenConfig = toml::from_str(toml_src).unwrap();
        assert_eq!(cfg.masters.len(), 2);
        assert_eq!(cfg.masters[0].name, "a");
        assert_eq!(cfg.masters[1].name, "b");
        assert_eq!(cfg.masters[1].quorum, 1);
    }

    #[test]
    fn test_from_str_humantime_units() {
        // The `humantime_serde` adapter should accept a variety of unit suffixes.
        let toml_src = r#"
            [[masters]]
            name = "m"
            ip = "127.0.0.1"
            port = 6379
            quorum = 1
            down_after = "2m"
            failover_timeout = "1h"
            hello_interval = "750ms"
        "#;
        let cfg: WardenConfig = toml::from_str(toml_src).unwrap();
        let m = &cfg.masters[0];
        assert_eq!(m.down_after, Duration::from_secs(120));
        assert_eq!(m.failover_timeout, Duration::from_secs(3600));
        assert_eq!(m.hello_interval, Duration::from_millis(750));
    }

    #[test]
    fn test_from_str_invalid_duration_is_error() {
        let toml_src = r#"
            [[masters]]
            name = "m"
            ip = "127.0.0.1"
            port = 6379
            quorum = 1
            down_after = "not-a-duration"
            failover_timeout = "1s"
        "#;
        let r: Result<WardenConfig, _> = toml::from_str(toml_src);
        assert!(r.is_err());
    }

    #[test]
    fn test_from_str_missing_masters_is_error() {
        let toml_src = r#"
            host = "0.0.0.0"
            port = 26379
        "#;
        let r: Result<WardenConfig, _> = toml::from_str(toml_src);
        assert!(r.is_err());
    }

    #[test]
    fn test_from_str_missing_required_master_field_is_error() {
        // `name` is required on MonitoredMaster; omitting it must fail.
        let toml_src = r#"
            [[masters]]
            ip = "127.0.0.1"
            port = 6379
            quorum = 1
            down_after = "1s"
            failover_timeout = "1s"
        "#;
        let r: Result<WardenConfig, _> = toml::from_str(toml_src);
        assert!(r.is_err());
    }

    #[test]
    fn test_from_str_wrong_type_for_quorum_is_error() {
        // quorum must be a number, not a string.
        let toml_src = r#"
            [[masters]]
            name = "m"
            ip = "127.0.0.1"
            port = 6379
            quorum = "two"
            down_after = "1s"
            failover_timeout = "1s"
        "#;
        let r: Result<WardenConfig, _> = toml::from_str(toml_src);
        assert!(r.is_err());
    }

    #[tokio::test]
    async fn test_from_file_reads_and_parses() {
        let toml_src = r#"
            host = "127.0.0.1"
            port = 26379
            [[masters]]
            name = "primary"
            ip = "127.0.0.1"
            port = 6379
            quorum = 1
            down_after = "5s"
            failover_timeout = "10s"
        "#;
        let tmp = NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), toml_src).unwrap();
        let cfg = WardenConfig::from_file(tmp.path().to_str().unwrap())
            .await
            .unwrap();
        assert_eq!(cfg.host, "127.0.0.1");
        assert_eq!(cfg.masters.len(), 1);
        assert_eq!(cfg.masters[0].name, "primary");
    }

    #[tokio::test]
    async fn test_from_file_missing_file_is_error() {
        let r = WardenConfig::from_file("/nonexistent/path/warden.toml").await;
        assert!(r.is_err());
    }

    #[tokio::test]
    async fn test_from_file_invalid_toml_is_error() {
        let tmp = NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), "this is not valid toml = = =").unwrap();
        let r = WardenConfig::from_file(tmp.path().to_str().unwrap()).await;
        assert!(r.is_err());
    }
}
