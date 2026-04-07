use crate::error::Result;
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub defaults: Defaults,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Defaults {
    #[serde(default = "default_poll_ms")]
    pub poll_ms: u64, // Follow poll interval in milliseconds.

    #[serde(default = "default_tail")]
    pub tail: usize, // Initial history lines shown before follow mode starts.

    #[serde(default = "default_batch_size")]
    pub batch_size: usize, // Max log rows per batch write.

    #[serde(default = "default_batch_timeout_secs")]
    pub batch_timeout_secs: u64, // Max seconds to wait before flushing a partial batch.

    #[serde(default = "default_auto_cleanup_days")]
    pub auto_cleanup_days: Option<i64>, // Auto-clean tasks older than N days; None disables it.
}

fn default_poll_ms() -> u64 {
    500
}

fn default_tail() -> usize {
    10
}

fn default_batch_size() -> usize {
    100
}

fn default_batch_timeout_secs() -> u64 {
    2
}

fn default_auto_cleanup_days() -> Option<i64> {
    None
}

impl Default for Defaults {
    fn default() -> Self {
        Self {
            poll_ms: default_poll_ms(),
            tail: default_tail(),
            batch_size: default_batch_size(),
            batch_timeout_secs: default_batch_timeout_secs(),
            auto_cleanup_days: default_auto_cleanup_days(),
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            defaults: Defaults::default(),
        }
    }
}

pub fn load_config() -> Result<Config> {
    let config_path = get_config_path()?;

    if !config_path.exists() {
        return Ok(Config::default());
    }

    let content = std::fs::read_to_string(&config_path)?;
    let config: Config = toml::from_str(&content).map_err(|e| {
        crate::error::LogexError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("failed to parse config: {}", e),
        ))
    })?;

    Ok(config)
}

pub fn get_config_path() -> Result<PathBuf> {
    let mut logex_dir = dirs::home_dir().ok_or_else(|| {
        crate::error::LogexError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "cannot locate user home directory",
        ))
    })?;
    logex_dir.push(".logex");
    Ok(logex_dir.join("config.toml"))
}

pub fn create_default_config() -> Result<()> {
    let config_path = get_config_path()?;

    if config_path.exists() {
        return Ok(());
    }

    let default_content = r#"# logex config file
# All settings are optional. Missing values fall back to built-in defaults.
[defaults]
# Follow poll interval in milliseconds. Default: 500
poll_ms = 500

# Initial history lines shown before follow mode starts. Default: 10
tail = 10

# Max log rows per batch write. Default: 100
# The batch flushes immediately when this threshold is reached.
batch_size = 100

# Max seconds to wait before flushing a partial batch. Default: 2
# This still flushes even if batch_size has not been reached.
batch_timeout_secs = 2

# Auto-clean tasks older than N days. Disabled by default.
# Uncomment and set a number to enable automatic cleanup.
# auto_cleanup_days = 30
"#;

    std::fs::write(&config_path, default_content)?;
    Ok(())
}
