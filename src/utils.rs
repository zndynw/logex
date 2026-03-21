use crate::error::{LogLevel, LogexError, Result};
use chrono::{Local, LocalResult, NaiveDate, NaiveDateTime, TimeZone};

pub fn now_rfc3339() -> String {
    Local::now().to_rfc3339()
}

pub fn normalize_time_input(input: &str, is_end: bool) -> Result<String> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(input) {
        return Ok(dt.with_timezone(&Local).to_rfc3339());
    }

    if let Ok(naive) = NaiveDateTime::parse_from_str(input, "%Y-%m-%d %H:%M:%S") {
        return local_naive_to_rfc3339(naive);
    }

    if let Ok(naive) = NaiveDateTime::parse_from_str(input, "%Y-%m-%d %H:%M") {
        return local_naive_to_rfc3339(naive);
    }

    if let Ok(date) = NaiveDate::parse_from_str(input, "%Y-%m-%d") {
        let naive = if is_end {
            date.and_hms_opt(23, 59, 59)
                .ok_or_else(|| LogexError::TimeFormat("invalid date".into()))?
        } else {
            date.and_hms_opt(0, 0, 0)
                .ok_or_else(|| LogexError::TimeFormat("invalid date".into()))?
        };
        return local_naive_to_rfc3339(naive);
    }

    Err(LogexError::TimeFormat(format!(
        "{}, supported: RFC3339 | YYYY-MM-DD | YYYY-MM-DD HH:MM[:SS]",
        input
    )))
}

fn local_naive_to_rfc3339(naive: NaiveDateTime) -> Result<String> {
    let local_dt = match Local.from_local_datetime(&naive) {
        LocalResult::Single(dt) => dt,
        LocalResult::Ambiguous(dt, _) => dt,
        LocalResult::None => {
            return Err(LogexError::TimeFormat(format!(
                "local time does not exist: {}",
                naive
            )));
        }
    };
    Ok(local_dt.to_rfc3339())
}

pub fn normalize_time_arg(input: Option<&str>, is_end: bool) -> Result<Option<String>> {
    input.map(|v| normalize_time_input(v, is_end)).transpose()
}

pub fn detect_level(stream: &str) -> LogLevel {
    LogLevel::from_stream(stream)
}

pub fn format_rfc3339_millis(rfc: &str) -> String {
    chrono::DateTime::parse_from_rfc3339(rfc)
        .ok()
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string())
        .unwrap_or_else(|| rfc.to_string())
}

pub fn format_duration(ms: Option<i64>) -> String {
    match ms {
        Some(d) if d < 1000 => format!("{}ms", d),
        Some(d) if d < 60_000 => format!("{:.2}s", d as f64 / 1000.0),
        Some(d) => format!("{:.2}m", d as f64 / 60_000.0),
        None => "-".to_string(),
    }
}

pub fn json_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

pub fn json_opt_string(value: Option<&str>) -> String {
    match value {
        Some(v) => format!("\"{}\"", json_escape(v)),
        None => "null".to_string(),
    }
}
