use std::fmt;

#[derive(Debug)]
pub enum LogexError {
    Database(rusqlite::Error),
    Io(std::io::Error),
    TimeFormat(String),
    InvalidWorkDir(String),
    ClearValidation(String),
    TaskNotFound(i64),
    TaskExecution(String),
    ConfigError(String),
}

impl fmt::Display for LogexError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Database(e) => write!(f, "database error: {}", e),
            Self::Io(e) => write!(f, "io error: {}", e),
            Self::TimeFormat(s) => write!(f, "invalid time format: {}", s),
            Self::InvalidWorkDir(s) => write!(f, "invalid work dir: {}", s),
            Self::ClearValidation(s) => write!(f, "{}", s),
            Self::TaskNotFound(id) => write!(f, "task {} not found", id),
            Self::TaskExecution(s) => write!(f, "{}", s),
            Self::ConfigError(s) => write!(f, "config error: {}", s),
        }
    }
}

impl std::error::Error for LogexError {}

impl From<rusqlite::Error> for LogexError {
    fn from(e: rusqlite::Error) -> Self {
        Self::Database(e)
    }
}

impl From<std::io::Error> for LogexError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

pub type Result<T> = std::result::Result<T, LogexError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    Running,
    Success,
    Failed,
}

impl TaskStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Success => "success",
            Self::Failed => "failed",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "success" => Self::Success,
            "failed" => Self::Failed,
            _ => Self::Running,
        }
    }
}

impl fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Info,
    Warn,
    Error,
    Unknown,
}

impl LogLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
            Self::Unknown => "unknown",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "info" => Self::Info,
            "warn" => Self::Warn,
            "error" => Self::Error,
            _ => Self::Unknown,
        }
    }

    pub fn from_stream(stream: &str) -> Self {
        match stream {
            "stdout" => Self::Info,
            "stderr" => Self::Error,
            _ => Self::Unknown,
        }
    }
}

impl fmt::Display for LogLevel {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}
