use logex::cli::{ExportArgs, QueryMatchMode, QuerySearchField, RunArgs};
use logex::config::Config;
use logex::executor::{TaskOrigin, TriggerType, get_task_info, run_task, run_task_with_origin};
use logex::exporter::render_export;
use logex::filters::{LogRowQuery, NormalizedTimeRange};
use logex::migrations::migrate;
use logex::services::export_service::handle_export;
use logex::store::{
    LineageFilter, TaskListFilter, fetch_log_rows, fetch_task_detail, fetch_task_list,
    fetch_task_logs,
};
use logex::{Result, cli::ExportFormat};
use rusqlite::Connection;
use std::fs;
use std::path::PathBuf;
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
