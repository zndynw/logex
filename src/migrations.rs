use crate::Result;
use rusqlite::Connection;

const CURRENT_SCHEMA_VERSION: i64 = 1;

pub fn migrate(conn: &Connection) -> Result<()> {
    conn.execute_batch("PRAGMA foreign_keys = ON;")?;

    let version = schema_version(conn)?;
    if version >= CURRENT_SCHEMA_VERSION {
        ensure_schema_objects(conn)?;
        return Ok(());
    }

    ensure_schema_objects(conn)?;
    upgrade_legacy_tasks_table(conn)?;
    set_schema_version(conn, CURRENT_SCHEMA_VERSION)?;
    Ok(())
}

fn schema_version(conn: &Connection) -> Result<i64> {
    Ok(conn.query_row("PRAGMA user_version", [], |row| row.get(0))?)
}

fn set_schema_version(conn: &Connection, version: i64) -> Result<()> {
    conn.execute_batch(&format!("PRAGMA user_version = {version};"))?;
    Ok(())
}

fn ensure_schema_objects(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tag TEXT,
            command TEXT NOT NULL,
            command_json TEXT,
            shell TEXT,
            work_dir TEXT NOT NULL,
            started_at TEXT NOT NULL,
            ended_at TEXT,
            duration_ms INTEGER,
            pid INTEGER,
            parent_task_id INTEGER,
            retry_of_task_id INTEGER,
            trigger_type TEXT,
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

    // Rebuild the FTS index so pre-existing legacy rows are searchable after migration.
    conn.execute("INSERT INTO task_logs_fts(task_logs_fts) VALUES('rebuild')", [])?;

    Ok(())
}

fn upgrade_legacy_tasks_table(conn: &Connection) -> Result<()> {
    ensure_task_column(conn, "env_vars", "TEXT")?;
    ensure_task_column(conn, "command_json", "TEXT")?;
    ensure_task_column(conn, "shell", "TEXT")?;
    ensure_task_column(conn, "pid", "INTEGER")?;
    ensure_task_column(conn, "parent_task_id", "INTEGER")?;
    ensure_task_column(conn, "retry_of_task_id", "INTEGER")?;
    ensure_task_column(conn, "trigger_type", "TEXT")?;
    Ok(())
}

fn ensure_task_column(conn: &Connection, column: &str, definition: &str) -> Result<()> {
    if !task_column_exists(conn, column)? {
        conn.execute_batch(&format!("ALTER TABLE tasks ADD COLUMN {column} {definition}"))?;
    }
    Ok(())
}

fn task_column_exists(conn: &Connection, column: &str) -> Result<bool> {
    let mut stmt = conn.prepare("PRAGMA table_info(tasks)")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;

    for row in rows {
        if row? == column {
            return Ok(true);
        }
    }

    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::ExportFormat;
    use crate::exporter::render_export;
    use crate::filters::{LogRowQuery, NormalizedTimeRange};
    use crate::store::{
        LineageFilter, TaskListFilter, fetch_log_rows, fetch_log_rows_fts, fetch_task_detail,
        fetch_task_list, fetch_tail_start_id,
    };
    use rusqlite::params;

    #[test]
    fn migrates_legacy_unversioned_tasks_table() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            r#"
            CREATE TABLE tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tag TEXT,
                command TEXT NOT NULL,
                work_dir TEXT NOT NULL,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                duration_ms INTEGER,
                exit_code INTEGER,
                status TEXT NOT NULL
            );
            CREATE TABLE task_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER NOT NULL,
                ts TEXT NOT NULL,
                stream TEXT NOT NULL,
                level TEXT NOT NULL,
                message TEXT NOT NULL
            );
            "#
        )
        .unwrap();

        migrate(&conn).unwrap();

        assert!(task_column_exists(&conn, "env_vars").unwrap());
        assert!(task_column_exists(&conn, "command_json").unwrap());
        assert!(task_column_exists(&conn, "shell").unwrap());
        assert!(task_column_exists(&conn, "pid").unwrap());
        assert!(task_column_exists(&conn, "parent_task_id").unwrap());
        assert!(task_column_exists(&conn, "retry_of_task_id").unwrap());
        assert!(task_column_exists(&conn, "trigger_type").unwrap());
        assert_eq!(schema_version(&conn).unwrap(), CURRENT_SCHEMA_VERSION);
    }

    #[test]
    fn initializes_fresh_database_and_sets_version() {
        let conn = Connection::open_in_memory().unwrap();

        migrate(&conn).unwrap();

        assert!(task_column_exists(&conn, "command").unwrap());
        assert!(task_column_exists(&conn, "command_json").unwrap());
        assert!(task_column_exists(&conn, "trigger_type").unwrap());
        assert_eq!(schema_version(&conn).unwrap(), CURRENT_SCHEMA_VERSION);
    }

    #[test]
    fn migrated_legacy_database_supports_list_query_and_export_flows() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            r#"
            CREATE TABLE tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tag TEXT,
                command TEXT NOT NULL,
                work_dir TEXT NOT NULL,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                duration_ms INTEGER,
                exit_code INTEGER,
                status TEXT NOT NULL
            );
            CREATE TABLE task_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER NOT NULL,
                ts TEXT NOT NULL,
                stream TEXT NOT NULL,
                level TEXT NOT NULL,
                message TEXT NOT NULL
            );
            "#,
        )
        .unwrap();
        conn.execute(
            "INSERT INTO tasks(tag, command, work_dir, started_at, ended_at, duration_ms, exit_code, status) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                "legacy-demo",
                "cargo test",
                ".",
                "2026-03-21T12:00:00+08:00",
                "2026-03-21T12:01:00+08:00",
                60_000,
                1,
                "failed"
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO task_logs(task_id, ts, stream, level, message) VALUES(?1, ?2, ?3, ?4, ?5)",
            params![1, "2026-03-21T12:00:10+08:00", "stdout", "info", "start build"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO task_logs(task_id, ts, stream, level, message) VALUES(?1, ?2, ?3, ?4, ?5)",
            params![1, "2026-03-21T12:00:20+08:00", "stderr", "error", "deploy timeout"],
        )
        .unwrap();

        migrate(&conn).unwrap();

        let tasks = fetch_task_list(
            &conn,
            &TaskListFilter {
                tag: Some("legacy-demo".into()),
                status: Some("failed".into()),
                lineage_filter: LineageFilter::All,
                limit: 10,
                offset: 0,
            },
        )
        .unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].command, "cargo test");
        assert_eq!(tasks[0].trigger_type, None);

        let detail = fetch_task_detail(&conn, 1).unwrap().unwrap();
        assert_eq!(detail.id, 1);
        assert_eq!(detail.command_json, None);
        assert_eq!(detail.shell, None);
        assert_eq!(detail.pid, None);

        let query = LogRowQuery {
            task_id: Some(1),
            tag: Some("legacy-demo".into()),
            level: Some("error".into()),
            status: Some("failed".into()),
            time_range: NormalizedTimeRange {
                from: Some("2026-03-21T12:00:00+08:00".into()),
                to: Some("2026-03-21T12:01:00+08:00".into()),
            },
        };

        let queried_logs = fetch_log_rows(&conn, &query, 0).unwrap();
        assert_eq!(queried_logs.len(), 1);
        assert_eq!(queried_logs[0].message, "deploy timeout");

        let fts_logs = fetch_log_rows_fts(&conn, &query, 0, "timeout").unwrap();
        assert_eq!(fts_logs.len(), 1);
        assert_eq!(fts_logs[0].message, "deploy timeout");

        let tail_start = fetch_tail_start_id(&conn, &query, 0).unwrap();
        assert_eq!(tail_start, Some(2));

        let html = render_export(ExportFormat::Html, &queried_logs, Some(&detail));
        assert!(html.contains("<h2>Log Summary</h2>"));
        assert!(html.contains("deploy timeout"));
        assert!(html.contains("<th>Trigger Type</th><td>-</td>"));
    }
}
