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
