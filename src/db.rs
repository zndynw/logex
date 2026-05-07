use crate::config;
use crate::error::Result;
use crate::migrations;
use crate::utils::now_rfc3339;
use rusqlite::{Connection, params};
use std::path::{Path, PathBuf};
use std::time::Duration;

const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_secs(10);
pub const STALE_RUNNING_TASK_MINUTES: i64 = 24 * 60;

pub fn init_storage() -> Result<(PathBuf, Connection)> {
    let mut logex_dir = dirs::home_dir().ok_or_else(|| {
        crate::error::LogexError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "cannot locate user home directory",
        ))
    })?;
    logex_dir.push(".logex");
    std::fs::create_dir_all(&logex_dir)?;

    config::create_default_config()?;

    let db_path = logex_dir.join("logex.db");
    let conn = open_configured_connection(&db_path)?;
    migrations::migrate(&conn)?;
    mark_stale_running_tasks(&conn, &now_rfc3339(), STALE_RUNNING_TASK_MINUTES)?;

    Ok((db_path, conn))
}

pub fn open_configured_connection(path: impl AsRef<Path>) -> Result<Connection> {
    let conn = Connection::open(path)?;
    configure_connection(&conn, true)?;
    Ok(conn)
}

pub fn configure_connection(conn: &Connection, enable_wal: bool) -> Result<()> {
    conn.busy_timeout(SQLITE_BUSY_TIMEOUT)?;
    conn.execute_batch("PRAGMA foreign_keys = ON;")?;
    if enable_wal {
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
    }
    Ok(())
}

pub fn auto_cleanup(conn: &Connection, days: i64) -> Result<usize> {
    let cutoff = (chrono::Local::now() - chrono::Duration::days(days)).to_rfc3339();
    let count = conn.execute("DELETE FROM tasks WHERE started_at < ?1", params![cutoff])?;
    Ok(count)
}

pub fn mark_stale_running_tasks(
    conn: &Connection,
    now: &str,
    stale_after_minutes: i64,
) -> Result<usize> {
    let cutoff = (chrono::DateTime::parse_from_rfc3339(now)
        .map_err(|err| crate::error::LogexError::TimeFormat(err.to_string()))?
        - chrono::Duration::minutes(stale_after_minutes))
    .to_rfc3339();

    let mut stmt = conn.prepare(
        "SELECT id, started_at FROM tasks WHERE status = 'running' AND started_at < ?1",
    )?;
    let rows = stmt.query_map(params![cutoff], |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
    })?;

    let mut stale_tasks = Vec::new();
    for row in rows {
        stale_tasks.push(row?);
    }
    drop(stmt);

    for (task_id, started_at) in &stale_tasks {
        let ended_at = now.to_string();
        let duration_ms = chrono::DateTime::parse_from_rfc3339(now)
            .map_err(|err| crate::error::LogexError::TimeFormat(err.to_string()))?
            .timestamp_millis()
            - chrono::DateTime::parse_from_rfc3339(started_at)
                .map_err(|err| crate::error::LogexError::TimeFormat(err.to_string()))?
                .timestamp_millis();
        let message = format!(
            "task marked failed after being running for more than {} minutes",
            stale_after_minutes
        );

        let tx = conn.unchecked_transaction()?;
        tx.execute(
            "INSERT INTO task_logs(task_id, ts, stream, level, message) VALUES(?1, ?2, ?3, ?4, ?5)",
            params![task_id, now, "stderr", "error", message],
        )?;
        tx.execute(
            "UPDATE tasks SET ended_at = ?1, duration_ms = ?2, exit_code = ?3, status = ?4 WHERE id = ?5 AND status = 'running'",
            params![ended_at, duration_ms, -1, "failed", task_id],
        )?;
        tx.commit()?;
    }

    Ok(stale_tasks.len())
}
