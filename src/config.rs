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
    pub poll_ms: u64, // follow 模式轮询间隔（毫秒）

    #[serde(default = "default_tail")]
    pub tail: usize, // follow 模式启动时显示的历史日志行数

    #[serde(default = "default_batch_size")]
    pub batch_size: usize, // 日志批量插入大小（条数）

    #[serde(default = "default_batch_timeout_secs")]
    pub batch_timeout_secs: u64, // 日志批量插入超时（秒）

    #[serde(default = "default_auto_cleanup_days")]
    pub auto_cleanup_days: Option<i64>, // 自动清理多少天前的日志（None 表示不自动清理）
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

    let default_content = r#"# logex 配置文件
# 所有配置项都是可选的，未配置时使用默认值

[defaults]
# follow 模式轮询间隔（毫秒），默认 500
poll_ms = 500

# follow 模式启动时显示的历史日志行数，默认 10
tail = 10

# 日志批量插入大小（条数），默认 100
# 达到此数量时立即提交到数据库
batch_size = 100

# 日志批量插入超时（秒），默认 2
# 超过此时间未达到 batch_size 也会提交
batch_timeout_secs = 2

# 自动清理多少天前的日志，默认不清理
# 取消注释并设置天数以启用自动清理
# auto_cleanup_days = 30
"#;

    std::fs::write(&config_path, default_content)?;
    Ok(())
}
