use crate::Result;
use crate::cli::ExportArgs;
use crate::exporter::render_export;
use crate::filters::{LogRowQuery, LogSearchFilter};
use crate::services::query_service::fetch_filtered_log_rows;
use crate::store::fetch_task_detail;
use rusqlite::Connection;
use std::fs;

#[cfg(test)]
fn test_shell_command(script: &str) -> Vec<String> {
    let shell = if cfg!(windows) { "powershell" } else { "pwsh" };
    vec![shell.into(), "-Command".into(), script.into()]
}

pub fn handle_export(conn: &Connection, args: ExportArgs) -> Result<()> {
    let outputs = build_export_outputs(conn, &args)?;
    if outputs.is_empty() {
        eprintln!("no logs matched");
        return Ok(());
    }

    if let Some(parent) = args.output.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }
    fs::write(&args.output, outputs.join("\n\n"))?;
    println!("exported {}", args.output.display());

    Ok(())
}

fn build_export_outputs(conn: &Connection, args: &ExportArgs) -> Result<Vec<String>> {
    let row_query = LogRowQuery::from_export_args(&args)?;
    let filter = LogSearchFilter::from_export_args(&args);

    let matched_rows = fetch_filtered_log_rows(conn, &row_query, &filter, 0)?;

    if matched_rows.is_empty() {
        return Ok(Vec::new());
    }

    let mut task_ids: Vec<i64> = matched_rows.iter().map(|r| r.task_id).collect();
    task_ids.sort_unstable();
    task_ids.dedup();

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
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn setup_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        migrations::migrate(&conn).unwrap();
        conn
    }

    fn unique_export_path(filename: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir()
            .join("logex-export-service-tests")
            .join(unique.to_string())
            .join(filename)
    }

    #[test]
    fn handle_export_writes_rendered_content_to_output_file() {
        let conn = setup_conn();
        let args = RunArgs {
            tag: Some("demo".into()),
            cwd: None,
            live: false,
            background: false,
            wait_for: None,
            env_files: vec![],
            env_vars: vec![],
            command: test_shell_command("Write-Output 'export me'"),
        };

        let (task_id, status) =
            run_task_with_origin(&conn, args, &Config::default(), Default::default()).unwrap();
        assert_eq!(status, "success");

        let output = unique_export_path("export.json");
        handle_export(
            &conn,
            ExportArgs {
                task_id: Some(task_id),
                tag: None,
                from: None,
                to: None,
                level: None,
                status: None,
                grep: vec![],
                grep_mode: QueryMatchMode::Any,
                grep_fields: vec![QuerySearchField::Message],
                case_sensitive: false,
                invert_match: false,
                format: ExportFormat::Json,
                output: output.clone(),
            },
        )
        .unwrap();

        let rendered = fs::read_to_string(output).unwrap();
        assert!(rendered.contains("\"task\":{"));
        assert!(rendered.contains("export me"));
    }

    #[test]
    fn handle_export_concatenates_multiple_task_outputs_with_blank_line() {
        let conn = setup_conn();
        let first_args = RunArgs {
            tag: Some("demo".into()),
            cwd: None,
            live: false,
            background: false,
            wait_for: None,
            env_files: vec![],
            env_vars: vec![],
            command: test_shell_command("Write-Output first-task"),
        };
        let second_args = RunArgs {
            tag: Some("demo".into()),
            cwd: None,
            live: false,
            background: false,
            wait_for: None,
            env_files: vec![],
            env_vars: vec![],
            command: test_shell_command("Write-Output second-task"),
        };

        let (first_task_id, first_status) =
            run_task_with_origin(&conn, first_args, &Config::default(), Default::default())
                .unwrap();
        let (second_task_id, second_status) =
            run_task_with_origin(&conn, second_args, &Config::default(), Default::default())
                .unwrap();
        assert_eq!(first_status, "success");
        assert_eq!(second_status, "success");

        let output = unique_export_path("export.txt");
        handle_export(
            &conn,
            ExportArgs {
                task_id: None,
                tag: Some("demo".into()),
                from: None,
                to: None,
                level: None,
                status: None,
                grep: vec![],
                grep_mode: QueryMatchMode::Any,
                grep_fields: vec![QuerySearchField::Message],
                case_sensitive: false,
                invert_match: false,
                format: ExportFormat::Txt,
                output: output.clone(),
            },
        )
        .unwrap();

        let rendered = fs::read_to_string(output).unwrap();
        assert!(rendered.contains(&format!("task_id={first_task_id}")));
        assert!(rendered.contains(&format!("task_id={second_task_id}")));
        assert_eq!(rendered.matches("logex export").count(), 2);
        assert!(rendered.contains("\n\n\nlogex export"));
        assert!(rendered.contains("first-task"));
        assert!(rendered.contains("second-task"));
    }

    #[test]
    fn build_export_outputs_groups_filtered_logs_per_task() {
        let conn = setup_conn();
        let args = RunArgs {
            tag: Some("demo".into()),
            cwd: None,
            live: false,
            background: false,
            wait_for: None,
            env_files: vec![],
            env_vars: vec![],
            command: test_shell_command("Write-Output start; Write-Error 'deploy timeout'; exit 1"),
        };

        let (task_id, status) =
            run_task_with_origin(&conn, args, &Config::default(), Default::default()).unwrap();
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

    #[test]
    fn build_export_outputs_uses_shared_filtered_fetch_for_message_grep() {
        let conn = setup_conn();
        let args = RunArgs {
            tag: Some("demo".into()),
            cwd: None,
            live: false,
            background: false,
            wait_for: None,
            env_files: vec![],
            env_vars: vec![],
            command: test_shell_command(
                "Write-Output 'service panic'; Write-Output 'service idle'",
            ),
        };

        let (task_id, status) =
            run_task_with_origin(&conn, args, &Config::default(), Default::default()).unwrap();
        assert_eq!(status, "success");

        let export_args = ExportArgs {
            task_id: Some(task_id),
            tag: Some("demo".into()),
            from: None,
            to: None,
            level: Some("info".into()),
            status: Some("success".into()),
            grep: vec!["panic".into()],
            grep_mode: QueryMatchMode::Any,
            grep_fields: vec![QuerySearchField::Message],
            case_sensitive: false,
            invert_match: false,
            format: ExportFormat::Txt,
            output: PathBuf::from("ignored.txt"),
        };
        let row_query = LogRowQuery::from_export_args(&export_args).unwrap();
        let filter = LogSearchFilter::from_export_args(&export_args);
        let shared_rows =
            crate::services::query_service::fetch_filtered_log_rows(&conn, &row_query, &filter, 0)
                .unwrap();
        let outputs = build_export_outputs(&conn, &export_args).unwrap();

        assert_eq!(shared_rows.len(), 1);
        assert_eq!(shared_rows[0].message, "service panic");
        assert_eq!(outputs.len(), 1);
        assert!(outputs[0].contains("log_count=1"));
        assert!(outputs[0].contains("service panic"));
        assert!(!outputs[0].contains("message=service idle"));
    }

    #[test]
    fn build_export_outputs_preserves_substring_message_grep_semantics() {
        let conn = setup_conn();
        let args = RunArgs {
            tag: Some("demo".into()),
            cwd: None,
            live: false,
            background: false,
            wait_for: None,
            env_files: vec![],
            env_vars: vec![],
            command: test_shell_command("Write-Output 'request timeout'"),
        };

        let (task_id, status) =
            run_task_with_origin(&conn, args, &Config::default(), Default::default()).unwrap();
        assert_eq!(status, "success");

        let outputs = build_export_outputs(
            &conn,
            &ExportArgs {
                task_id: Some(task_id),
                tag: Some("demo".into()),
                from: None,
                to: None,
                level: Some("info".into()),
                status: Some("success".into()),
                grep: vec!["time".into()],
                grep_mode: QueryMatchMode::Any,
                grep_fields: vec![QuerySearchField::Message],
                case_sensitive: false,
                invert_match: false,
                format: ExportFormat::Txt,
                output: PathBuf::from("ignored.txt"),
            },
        )
        .unwrap();

        assert_eq!(outputs.len(), 1);
        assert!(outputs[0].contains("request timeout"));
    }
}
