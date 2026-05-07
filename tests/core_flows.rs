use logex::cli::{ExportArgs, QueryMatchMode, QuerySearchField, RunArgs};
use logex::config::Config;
use logex::executor::{TaskOrigin, TriggerType, get_task_info, run_task, run_task_with_origin};
use logex::exporter::render_export;
use logex::filters::{LogRowQuery, NormalizedTimeRange};
use logex::db::{open_configured_connection, mark_stale_running_tasks};
use logex::migrations::migrate;
use logex::services::export_service::handle_export;
use logex::store::{
    LineageFilter, TaskListFilter, fetch_log_rows, fetch_task_detail, fetch_task_list,
    fetch_task_logs,
};
use logex::{Result, cli::ExportFormat};
use rusqlite::{Connection, params};
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

fn powershell_command(script: &str) -> Vec<String> {
    let shell = if cfg!(windows) { "powershell" } else { "pwsh" };
    vec![
        shell.into(),
        "-NoProfile".into(),
        "-Command".into(),
        script.into(),
    ]
}

fn run_args(tag: &str, command: Vec<String>) -> RunArgs {
    RunArgs {
        tag: Some(tag.into()),
        cwd: Some(PathBuf::from(env!("CARGO_MANIFEST_DIR"))),
        live: false,
        background: false,
        wait_for: None,
        env_files: Vec::new(),
        env_vars: Vec::new(),
        command,
    }
}

fn unique_export_path(filename: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_nanos();
    std::env::temp_dir()
        .join("logex-core-flow-tests")
        .join(unique.to_string())
        .join(filename)
}

fn unique_temp_path(filename: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_nanos();
    std::env::temp_dir()
        .join("logex-core-flow-tests")
        .join(unique.to_string())
        .join(filename)
}

#[test]
fn migrated_database_records_query_retries_and_exports_a_task_flow() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    migrate(&conn)?;

    let config = Config::default();
    let command = powershell_command(
        "[Console]::Out.WriteLine('flow stdout'); \
         [Console]::Error.WriteLine('flow stderr'); \
         exit 1",
    );
    let (task_id, status) = run_task(&conn, run_args("core-flow", command), &config)?;
    assert_eq!(status, "failed");

    let error_rows = fetch_log_rows(
        &conn,
        &LogRowQuery {
            task_id: Some(task_id),
            tag: Some("core-flow".into()),
            level: Some("error".into()),
            status: Some("failed".into()),
            time_range: NormalizedTimeRange::default(),
        },
        0,
    )?;
    assert_eq!(error_rows.len(), 1);
    assert_eq!(error_rows[0].stream, "stderr");
    assert!(error_rows[0].message.contains("flow stderr"));

    let retry_source = get_task_info(&conn, task_id)?;
    let (retry_task_id, retry_status) = run_task_with_origin(
        &conn,
        run_args("core-flow", retry_source.command_args),
        &config,
        TaskOrigin {
            parent_task_id: None,
            retry_of_task_id: Some(task_id),
            trigger_type: Some(TriggerType::Retry),
        },
    )?;
    assert_eq!(retry_status, "failed");

    let retry_rows = fetch_task_list(
        &conn,
        &TaskListFilter {
            tag: Some("core-flow".into()),
            status: Some("failed".into()),
            lineage_filter: LineageFilter::RetryOnly,
            limit: 10,
            offset: 0,
        },
    )?;
    assert_eq!(retry_rows.len(), 1);
    assert_eq!(retry_rows[0].id, retry_task_id);
    assert_eq!(retry_rows[0].retry_of_task_id, Some(task_id));
    assert_eq!(retry_rows[0].trigger_type.as_deref(), Some("retry"));

    let retry_detail = fetch_task_detail(&conn, retry_task_id)?.expect("retry task detail");
    let retry_logs = fetch_task_logs(&conn, retry_task_id, 0)?;
    assert_eq!(retry_logs.len(), 2);

    let rendered = render_export(ExportFormat::Json, &retry_logs, Some(&retry_detail));
    assert!(rendered.contains("\"tag\":\"core-flow\""));
    assert!(rendered.contains(&format!("\"retry_of_task_id\":{task_id}")));
    assert!(rendered.contains("\"trigger_type\":\"retry\""));
    assert!(rendered.contains("flow stdout"));
    assert!(rendered.contains("flow stderr"));

    let export_path = unique_export_path("retry-flow.json");
    handle_export(
        &conn,
        ExportArgs {
            task_id: Some(retry_task_id),
            tag: Some("core-flow".into()),
            from: None,
            to: None,
            level: None,
            status: Some("failed".into()),
            grep: vec!["stderr".into()],
            grep_mode: QueryMatchMode::Any,
            grep_fields: vec![QuerySearchField::Message],
            case_sensitive: false,
            invert_match: false,
            format: ExportFormat::Json,
            output: export_path.clone(),
        },
    )?;

    let exported = fs::read_to_string(export_path)?;
    assert!(exported.contains("\"task\":{"));
    assert!(exported.contains(&format!("\"id\":{retry_task_id}")));
    assert!(exported.contains("\"message\":\"flow stderr\""));
    assert!(!exported.contains("\"message\":\"flow stdout\""));

    Ok(())
}

#[test]
fn configured_disk_connections_use_wal_and_busy_timeout() -> Result<()> {
    let db_path = unique_temp_path("configured.sqlite");
    fs::create_dir_all(db_path.parent().expect("temp db parent"))?;

    let conn = open_configured_connection(&db_path)?;
    migrate(&conn)?;

    let busy_timeout_ms: i64 = conn.query_row("PRAGMA busy_timeout", [], |row| row.get(0))?;
    let journal_mode: String = conn.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
    let foreign_keys: i64 = conn.query_row("PRAGMA foreign_keys", [], |row| row.get(0))?;

    assert!(busy_timeout_ms >= 5000);
    assert_eq!(journal_mode.to_lowercase(), "wal");
    assert_eq!(foreign_keys, 1);

    Ok(())
}

#[test]
fn concurrent_disk_writers_wait_instead_of_failing_with_database_locked() -> Result<()> {
    let db_path = unique_temp_path("concurrent.sqlite");
    fs::create_dir_all(db_path.parent().expect("temp db parent"))?;
    let conn = open_configured_connection(&db_path)?;
    migrate(&conn)?;
    drop(conn);

    let workers = 4;
    let barrier = Arc::new(Barrier::new(workers));
    let mut handles = Vec::new();

    for worker in 0..workers {
        let db_path = db_path.clone();
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || -> Result<()> {
            let conn = open_configured_connection(&db_path)?;
            let started_at = format!("2026-05-07T12:00:0{worker}+08:00");
            conn.execute(
                "INSERT INTO tasks(tag, command, work_dir, started_at, status) VALUES(?1, ?2, ?3, ?4, ?5)",
                params!["concurrent", "echo ok", ".", started_at, "running"],
            )?;
            let task_id = conn.last_insert_rowid();
            barrier.wait();

            for line in 0..40 {
                conn.execute(
                    "INSERT INTO task_logs(task_id, ts, stream, level, message) VALUES(?1, ?2, ?3, ?4, ?5)",
                    params![
                        task_id,
                        "2026-05-07T12:00:10+08:00",
                        "stdout",
                        "info",
                        format!("worker={worker} line={line}")
                    ],
                )?;
            }

            Ok(())
        }));
    }

    for handle in handles {
        handle.join().expect("writer thread panicked")?;
    }

    let conn = open_configured_connection(&db_path)?;
    let log_count: i64 = conn.query_row("SELECT COUNT(*) FROM task_logs", [], |row| row.get(0))?;
    assert_eq!(log_count, 160);

    Ok(())
}

#[test]
fn stale_running_tasks_are_marked_failed_with_a_log_message() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    migrate(&conn)?;
    conn.execute(
        "INSERT INTO tasks(tag, command, work_dir, started_at, status, pid) VALUES(?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            "orphan",
            "cargo test",
            ".",
            "2026-05-07T10:00:00+08:00",
            "running",
            424242_i64
        ],
    )?;

    let marked = mark_stale_running_tasks(&conn, "2026-05-07T11:00:00+08:00", 30)?;
    assert_eq!(marked, 1);

    let status: String = conn.query_row("SELECT status FROM tasks WHERE id = 1", [], |row| row.get(0))?;
    let exit_code: i32 = conn.query_row("SELECT exit_code FROM tasks WHERE id = 1", [], |row| row.get(0))?;
    let log_message: String = conn.query_row(
        "SELECT message FROM task_logs WHERE task_id = 1 ORDER BY id DESC LIMIT 1",
        [],
        |row| row.get(0),
    )?;

    assert_eq!(status, "failed");
    assert_eq!(exit_code, -1);
    assert!(log_message.contains("marked failed after being running for more than 30 minutes"));

    Ok(())
}
