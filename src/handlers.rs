use crate::Result;
use crate::cli::*;
use crate::config::Config;
use crate::executor::*;
use crate::seeder::seed_sample_data;
use crate::services::{analyze_service, export_service, query_service, tag_service, task_service};
use rusqlite::Connection;

pub fn handle_seed(mut conn: Connection, args: SeedArgs) -> Result<()> {
    let summary = seed_sample_data(&mut conn, &args)?;
    println!(
        "seeded_tasks={} seeded_logs={} success={} failed={} running={} tag_prefix={}",
        summary.tasks_inserted,
        summary.logs_inserted,
        summary.success_tasks,
        summary.failed_tasks,
        summary.running_tasks,
        args.tag_prefix
    );
    Ok(())
}

pub fn handle_query(conn: &Connection, args: QueryArgs, config: &Config) -> Result<()> {
    query_service::handle_query(conn, args, config)
}

pub fn handle_export(conn: &Connection, args: ExportArgs) -> Result<()> {
    export_service::handle_export(conn, args)
}

pub fn handle_list(conn: &Connection, args: ListArgs) -> Result<()> {
    task_service::handle_list(conn, args)
}

pub fn handle_tags(conn: &Connection, args: TagsArgs) -> Result<()> {
    tag_service::handle_tags(conn, args)
}

pub fn handle_analyze(conn: &Connection, args: AnalyzeArgs) -> Result<()> {
    analyze_service::handle_analyze(conn, args)
}

pub fn handle_clear(conn: &Connection, args: ClearArgs) -> Result<()> {
    task_service::handle_clear(conn, args)
}

pub fn handle_retry(conn: &Connection, args: RetryArgs, config: &Config) -> Result<()> {
    let task = get_task_info(conn, args.task_id)?;

    let run_args = RunArgs {
        tag: task.tag,
        cwd: Some(std::path::PathBuf::from(task.work_dir)),
        command: task.command_args,
        live: args.live,
        wait_for: None,
        env_files: vec![],
        env_vars: vec![],
    };

    let (task_id, status) = run_task_with_origin(
        conn,
        run_args,
        config,
        TaskOrigin {
            parent_task_id: Some(args.task_id),
            retry_of_task_id: Some(args.task_id),
            trigger_type: Some(TriggerType::Retry),
        },
    )?;
    println!("task_id={task_id} status={status}");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analyzer::AnalysisReport;
    use crate::filters::{LogSearchFilter, TagListFilter, matches_query_row};
    use crate::formatter::QueryLogRow;
    use crate::services::analyze_service::render_analyze_output;
    use crate::services::tag_service::query_tag_rows;
    use rusqlite::params;

    fn setup_conn() -> Connection {
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
                status TEXT NOT NULL,
                env_vars TEXT
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
        conn
    }

    #[test]
    fn query_tag_rows_returns_last_task_metadata() {
        let conn = setup_conn();

        conn.execute(
            "INSERT INTO tasks(tag, command, work_dir, started_at, ended_at, duration_ms, exit_code, status, env_vars) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params!["jt_jsc", "cargo test", ".", "2026-03-21T10:00:00+08:00", Option::<String>::None, Option::<i64>::None, Option::<i64>::None, "success", Option::<String>::None],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO tasks(tag, command, work_dir, started_at, ended_at, duration_ms, exit_code, status, env_vars) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params!["jt_jsc", "cargo build", ".", "2026-03-21T11:00:00+08:00", Option::<String>::None, Option::<i64>::None, Option::<i64>::None, "failed", Option::<String>::None],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO tasks(tag, command, work_dir, started_at, ended_at, duration_ms, exit_code, status, env_vars) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params!["d_risk", "cargo run", ".", "2026-03-21T12:00:00+08:00", Option::<String>::None, Option::<i64>::None, Option::<i64>::None, "running", Option::<String>::None],
        )
        .unwrap();

        let filter = TagListFilter::from_tags_args(&TagsArgs {
                from: None,
                to: None,
                grep: None,
                output: TagsOutput::Table,
                limit: 50,
                offset: 0,
            })
            .unwrap();

        let rows = query_tag_rows(&conn, &filter).unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].tag, "d_risk");
        assert_eq!(rows[0].task_count, 1);
        assert_eq!(rows[0].last_task_id, 3);
        assert_eq!(rows[0].last_started_at, "2026-03-21T12:00:00+08:00");

        assert_eq!(rows[1].tag, "jt_jsc");
        assert_eq!(rows[1].task_count, 2);
        assert_eq!(rows[1].last_task_id, 2);
        assert_eq!(rows[1].last_started_at, "2026-03-21T11:00:00+08:00");
    }

    #[test]
    fn query_tag_rows_breaks_started_at_ties_by_latest_id() {
        let conn = setup_conn();

        conn.execute(
            "INSERT INTO tasks(tag, command, work_dir, started_at, ended_at, duration_ms, exit_code, status, env_vars) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params!["demo", "cargo test", ".", "2026-03-21T11:00:00+08:00", Option::<String>::None, Option::<i64>::None, Option::<i64>::None, "success", Option::<String>::None],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO tasks(tag, command, work_dir, started_at, ended_at, duration_ms, exit_code, status, env_vars) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params!["demo", "cargo run", ".", "2026-03-21T11:00:00+08:00", Option::<String>::None, Option::<i64>::None, Option::<i64>::None, "failed", Option::<String>::None],
        )
        .unwrap();

        let filter = TagListFilter::from_tags_args(&TagsArgs {
                from: None,
                to: None,
                grep: None,
                output: TagsOutput::Table,
                limit: 50,
                offset: 0,
            })
            .unwrap();

        let rows = query_tag_rows(&conn, &filter).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].tag, "demo");
        assert_eq!(rows[0].task_count, 2);
        assert_eq!(rows[0].last_task_id, 2);
        assert_eq!(rows[0].last_started_at, "2026-03-21T11:00:00+08:00");
    }

    #[test]
    fn render_analyze_output_returns_plain_and_json_content() {
        let analysis = AnalysisReport {
            logs: crate::analyzer::LogAnalysis {
                total: 4,
                error: 1,
                warn: 1,
                info: 2,
                unknown: 0,
                stdout: 3,
                stderr: 1,
                other_streams: 0,
                first_ts: Some("2026-03-21T10:00:00+08:00".into()),
                last_ts: Some("2026-03-21T11:00:00+08:00".into()),
            },
            tasks: crate::analyzer::TaskAnalysis {
                total: 2,
                running: 0,
                success: 1,
                failed: 1,
            },
            durations: crate::analyzer::DurationAnalysis {
                finished_count: 2,
                min_ms: Some(1000),
                avg_ms: Some(1500.0),
                max_ms: Some(2000),
            },
            top_tags: vec![crate::analyzer::TagAnalysis {
                tag: "demo".into(),
                task_count: 2,
                log_count: 4,
                error_count: 1,
                warn_count: 1,
                info_count: 2,
                unknown_count: 0,
                last_started_at: Some("2026-03-21T11:00:00+08:00".into()),
            }],
        };

        let plain = render_analyze_output(&analysis, false);
        assert!(plain.contains("analyze_result"));
        assert!(plain.contains("top_tag tag=demo"));

        let json = render_analyze_output(&analysis, true);
        assert!(json.contains("\"logs\""));
        assert!(json.contains("\"top_tags\""));
    }

    #[test]
    fn shared_query_filter_respects_selected_fields() {
        let args = QueryArgs {
            task_id: None,
            tag: None,
            from: None,
            to: None,
            level: None,
            status: None,
            view: QueryView::Detail,
            output: QueryOutput::Table,
            grep: vec!["deploy".into()],
            grep_mode: QueryMatchMode::Any,
            grep_fields: vec![QuerySearchField::Tag],
            case_sensitive: false,
            invert_match: false,
            no_highlight: false,
            after_context: 0,
            before_context: 0,
            context: None,
            follow: false,
            tail: 10,
            poll_ms: 500,
        };

        let row = QueryLogRow {
            id: 1,
            task_id: 42,
            tag: Some("deploy-prod".into()),
            ts: "2026-03-21T12:00:00+08:00".into(),
            stream: "stderr".into(),
            level: "error".into(),
            message: "startup failed".into(),
            status: "failed".into(),
        };

        let filter = LogSearchFilter::from_query_args(&args);
        assert!(matches_query_row(&row, &filter));

        let wrong_field_row = QueryLogRow {
            tag: Some("prod".into()),
            message: "deploy failed".into(),
            ..row
        };
        assert!(!matches_query_row(&wrong_field_row, &filter));
    }

    #[test]
    fn shared_query_filter_supports_all_match_mode() {
        let args = QueryArgs {
            task_id: None,
            tag: None,
            from: None,
            to: None,
            level: None,
            status: None,
            view: QueryView::Detail,
            output: QueryOutput::Table,
            grep: vec!["timeout".into(), "retry".into()],
            grep_mode: QueryMatchMode::All,
            grep_fields: vec![QuerySearchField::Message],
            case_sensitive: false,
            invert_match: false,
            no_highlight: false,
            after_context: 0,
            before_context: 0,
            context: None,
            follow: false,
            tail: 10,
            poll_ms: 500,
        };

        let filter = LogSearchFilter::from_query_args(&args);
        let matching_row = QueryLogRow {
            id: 1,
            task_id: 42,
            tag: Some("deploy-prod".into()),
            ts: "2026-03-21T12:00:00+08:00".into(),
            stream: "stderr".into(),
            level: "error".into(),
            message: "connection timeout before retry".into(),
            status: "failed".into(),
        };
        let partial_row = QueryLogRow {
            message: "connection timeout".into(),
            ..matching_row.clone()
        };

        assert!(matches_query_row(&matching_row, &filter));
        assert!(!matches_query_row(&partial_row, &filter));
    }
}
