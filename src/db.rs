use crate::config;
use crate::error::Result;
use crate::migrations;
use rusqlite::{Connection, params};
use std::path::PathBuf;

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
    let conn = Connection::open(&db_path)?;
    migrations::migrate(&conn)?;

    Ok((db_path, conn))
}

pub fn auto_cleanup(conn: &Connection, days: i64) -> Result<usize> {
    let cutoff = (chrono::Local::now() - chrono::Duration::days(days)).to_rfc3339();
    let count = conn.execute("DELETE FROM tasks WHERE started_at < ?1", params![cutoff])?;
    Ok(count)
}
