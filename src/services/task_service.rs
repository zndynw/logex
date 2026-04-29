use crate::Result;
use crate::cli::{ClearArgs, ListArgs, ListOutput};
use crate::filters::{ClearTaskFilter, TaskListFilter};
use crate::formatter::{print_list_rows_table, task_lineage_label};
use crate::store::{LineageFilter, fetch_task_list_with_range};
use rusqlite::Connection;

pub fn handle_list(conn: &Connection, args: ListArgs) -> Result<()> {
    let filter = TaskListFilter::from_list_args(&args)?;
    let task_rows = fetch_task_list_with_range(
        conn,
        filter.tag.as_deref(),
        None,
        &filter.time_range,
        LineageFilter::All,
        filter.limit,
        filter.offset,
    )?;

    if task_rows.is_empty() {
        println!("no tasks found");
        return Ok(());
    }

    match args.output {
        ListOutput::Table => print_list_rows_table(&task_rows),
        ListOutput::Plain => {
            for row in &task_rows {
                let env_info = row.env_vars.as_deref().unwrap_or("-");
                let lineage = task_lineage_label(row).unwrap_or_else(|| "-".to_string());
                println!(
                    "id={} tag={} status={} lineage={} shell={} pid={} started_at={} command={} env={}",
                    row.id,
                    row.tag.as_deref().unwrap_or("-"),
                    row.status,
                    lineage,
                    row.shell.as_deref().unwrap_or("-"),
                    row.pid
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                    row.started_at,
                    row.command,
                    env_info
                );
            }
        }
    }

    Ok(())
}

pub fn handle_clear(conn: &Connection, args: ClearArgs) -> Result<()> {
    crate::executor::validate_clear_args(&args)?;
    let filter = ClearTaskFilter::from_clear_args(&args)?;

    let mut sql = String::from("DELETE FROM tasks WHERE 1=1");
    let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

    if let Some(ref id) = filter.task_id {
        sql.push_str(" AND id = ?");
        params_vec.push(Box::new(*id));
    }
    if let Some(ref tag) = filter.tag {
        sql.push_str(" AND tag = ?");
        params_vec.push(Box::new(tag.clone()));
    }
    if let Some(ref from_ts) = filter.time_range.from {
        sql.push_str(" AND started_at >= ?");
        params_vec.push(Box::new(from_ts.clone()));
    }
    if let Some(ref to_ts) = filter.time_range.to {
        sql.push_str(" AND started_at <= ?");
        params_vec.push(Box::new(to_ts.clone()));
    }

    let params_refs: Vec<&dyn rusqlite::ToSql> = params_vec.iter().map(|b| b.as_ref()).collect();

    let count = conn.execute(&sql, params_refs.as_slice())?;
    println!("cleared {} task(s)", count);

    if args.vacuum {
        conn.execute_batch("VACUUM")?;
        println!("vacuum completed");
    } else if count > 0 {
        println!("note: SQLite file size may not shrink until VACUUM is run");
    }

    Ok(())
}

pub fn handle_vacuum(conn: &Connection) -> Result<()> {
    conn.execute_batch("VACUUM")?;
    println!("vacuum completed");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::params;

    fn setup_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            r#"
            PRAGMA foreign_keys = ON;
            CREATE TABLE tasks (
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
                env_files_json TEXT,
                env_vars_json TEXT
            );
            CREATE TABLE task_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER NOT NULL,
                ts TEXT NOT NULL,
                stream TEXT NOT NULL,
                level TEXT NOT NULL,
                message TEXT NOT NULL,
                FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE
            );
            "#,
        )
        .unwrap();
        conn
    }

    #[test]
    fn clear_with_vacuum_reclaims_sqlite_freelist_pages() {
        let conn = setup_conn();
        conn.execute(
            "INSERT INTO tasks(tag, command, work_dir, started_at, status) VALUES(?1, ?2, ?3, ?4, ?5)",
            params!["demo", "cargo test", ".", "2026-04-29T12:00:00+08:00", "failed"],
        )
        .unwrap();
        let large_message = "x".repeat(4096);
        for _ in 0..128 {
            conn.execute(
                "INSERT INTO task_logs(task_id, ts, stream, level, message) VALUES(?1, ?2, ?3, ?4, ?5)",
                params![1, "2026-04-29T12:00:01+08:00", "stderr", "error", large_message],
            )
            .unwrap();
        }

        handle_clear(
            &conn,
            ClearArgs {
                task_id: None,
                tag: Some("demo".into()),
                from: None,
                to: None,
                all: false,
                yes: false,
                vacuum: true,
            },
        )
        .unwrap();

        let freelist_count: i64 = conn
            .query_row("PRAGMA freelist_count", [], |row| row.get(0))
            .unwrap();
        assert_eq!(freelist_count, 0);
    }

    #[test]
    fn standalone_vacuum_reclaims_sqlite_freelist_pages() {
        let conn = setup_conn();
        conn.execute(
            "INSERT INTO tasks(tag, command, work_dir, started_at, status) VALUES(?1, ?2, ?3, ?4, ?5)",
            params!["demo", "cargo test", ".", "2026-04-29T12:00:00+08:00", "failed"],
        )
        .unwrap();
        let large_message = "x".repeat(4096);
        for _ in 0..128 {
            conn.execute(
                "INSERT INTO task_logs(task_id, ts, stream, level, message) VALUES(?1, ?2, ?3, ?4, ?5)",
                params![1, "2026-04-29T12:00:01+08:00", "stderr", "error", large_message],
            )
            .unwrap();
        }

        handle_clear(
            &conn,
            ClearArgs {
                task_id: None,
                tag: Some("demo".into()),
                from: None,
                to: None,
                all: false,
                yes: false,
                vacuum: false,
            },
        )
        .unwrap();

        let freelist_before: i64 = conn
            .query_row("PRAGMA freelist_count", [], |row| row.get(0))
            .unwrap();
        assert!(freelist_before > 0);

        handle_vacuum(&conn).unwrap();

        let freelist_after: i64 = conn
            .query_row("PRAGMA freelist_count", [], |row| row.get(0))
            .unwrap();
        assert_eq!(freelist_after, 0);
    }
}
