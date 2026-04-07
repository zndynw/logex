use crate::Result;
use crate::cli::ExportArgs;
use crate::exporter::render_export;
use crate::filters::{LogRowQuery, LogSearchFilter, matches_query_row};
use crate::store::{fetch_log_rows, fetch_task_detail};
use rusqlite::Connection;

pub fn handle_export(conn: &Connection, args: ExportArgs) -> Result<()> {
    let outputs = build_export_outputs(conn, &args)?;
    if outputs.is_empty() {
        eprintln!("no logs matched");
        return Ok(());
    }

    for output in outputs {
        println!("{output}");
    }

    Ok(())
}

fn build_export_outputs(conn: &Connection, args: &ExportArgs) -> Result<Vec<String>> {
    let row_query = LogRowQuery::from_export_args(&args)?;
    let filter = LogSearchFilter::from_export_args(&args);

    let rows = fetch_log_rows(conn, &row_query, 0)?;
    let matched_rows: Vec<_> = rows
        .into_iter()
        .filter(|row| matches_query_row(row, &filter))
        .collect();

    if matched_rows.is_empty() {
        return Ok(Vec::new());
    }

    let task_ids: std::collections::HashSet<i64> = matched_rows.iter().map(|r| r.task_id).collect();

    let mut tasks = Vec::new();
    for tid in task_ids {
        if let Some(task) = fetch_task_detail(conn, tid)? {
            tasks.push(task);
        }
    }

    let mut outputs = Vec::new();
    for task in &tasks {
        let task_logs: Vec<_> = matched_rows
            .iter()
            .filter(|row| row.task_id == task.id)
            .cloned()
            .collect();
        outputs.push(render_export(args.format, &task_logs, Some(task)));
    }

    Ok(outputs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{ExportFormat, QueryMatchMode, QuerySearchField, RunArgs};
    use crate::config::Config;
    use crate::executor::run_task_with_origin;
    use crate::migrations;
    use std::path::PathBuf;

    fn setup_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        migrations::migrate(&conn).unwrap();
        conn
    }

    #[test]
    fn build_export_outputs_groups_filtered_logs_per_task() {
        let conn = setup_conn();
        let args = RunArgs {
            tag: Some("demo".into()),
            cwd: None,
            live: false,
            wait_for: None,
            env_files: vec![],
            env_vars: vec![],
            command: vec![
                "powershell".into(),
                "-Command".into(),
                "Write-Output start; Write-Error 'deploy timeout'; exit 1".into(),
            ],
        };

        let (task_id, status) = run_task_with_origin(&conn, args, &Config::default(), Default::default()).unwrap();
        assert_eq!(status, "failed");

        let outputs = build_export_outputs(
            &conn,
            &ExportArgs {
                task_id: Some(task_id),
                tag: None,
                from: None,
                to: None,
                level: Some("error".into()),
                status: Some("failed".into()),
                grep: vec!["timeout".into()],
                grep_mode: QueryMatchMode::Any,
                grep_fields: vec![QuerySearchField::Message],
                case_sensitive: false,
                invert_match: false,
                format: ExportFormat::Json,
                output: PathBuf::from("ignored.json"),
            },
        )
        .unwrap();

        assert_eq!(outputs.len(), 1);
        assert!(outputs[0].contains("\"task\":{"));
        assert!(outputs[0].contains("\"id\":"));
        assert!(outputs[0].contains("deploy timeout"));
        assert!(!outputs[0].contains("\"start\""));
    }
}
