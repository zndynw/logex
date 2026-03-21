use crate::config;
use crate::error::Result;
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
    conn.execute_batch(
        r#"
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tag TEXT,
            command TEXT NOT NULL,
            work_dir TEXT NOT NULL,
            started_at TEXT NOT NULL,
            ended_at TEXT,
            duration_ms INTEGER,
            exit_code INTEGER,
            status TEXT NOT NULL,
            env_vars TEXT
        );

        CREATE TABLE IF NOT EXISTS task_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id INTEGER NOT NULL,
            ts TEXT NOT NULL,
            stream TEXT NOT NULL,
            level TEXT NOT NULL,
            message TEXT NOT NULL,
            FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_tasks_tag_started_at ON tasks(tag, started_at);
        CREATE INDEX IF NOT EXISTS idx_tasks_status_started_at ON tasks(status, started_at);
        CREATE INDEX IF NOT EXISTS idx_task_logs_task_id_ts ON task_logs(task_id, ts);
        CREATE INDEX IF NOT EXISTS idx_task_logs_level_ts ON task_logs(level, ts);
        CREATE INDEX IF NOT EXISTS idx_task_logs_task_level_ts ON task_logs(task_id, level, ts);
        CREATE INDEX IF NOT EXISTS idx_tasks_tag_status_started ON tasks(tag, status, started_at);

        CREATE VIRTUAL TABLE IF NOT EXISTS task_logs_fts USING fts5(
            message,
            content=task_logs,
            content_rowid=id
        );

        CREATE TRIGGER IF NOT EXISTS task_logs_ai AFTER INSERT ON task_logs BEGIN
            INSERT INTO task_logs_fts(rowid, message) VALUES (new.id, new.message);
        END;

        CREATE TRIGGER IF NOT EXISTS task_logs_ad AFTER DELETE ON task_logs BEGIN
            DELETE FROM task_logs_fts WHERE rowid = old.id;
        END;

        CREATE TRIGGER IF NOT EXISTS task_logs_au AFTER UPDATE ON task_logs BEGIN
            DELETE FROM task_logs_fts WHERE rowid = old.id;
            INSERT INTO task_logs_fts(rowid, message) VALUES (new.id, new.message);
        END;
        "#,
    )?;

    conn.execute_batch("ALTER TABLE tasks ADD COLUMN env_vars TEXT")
        .ok();

    Ok((db_path, conn))
}

pub fn auto_cleanup(conn: &Connection, days: i64) -> Result<usize> {
    let cutoff = (chrono::Local::now() - chrono::Duration::days(days)).to_rfc3339();
    let count = conn.execute("DELETE FROM tasks WHERE started_at < ?1", params![cutoff])?;
    Ok(count)
}
