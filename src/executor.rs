use crate::cli::*;
use crate::config::Config;
use crate::error::{LogLevel, LogexError, Result, TaskStatus};
use crate::store::{fetch_task_run_record, fetch_task_status};
use crate::utils::*;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Command as ProcessCommand, Stdio};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::thread;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredCommand {
    pub argv: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskRunInfo {
    pub command_text: String,
    pub command_args: Vec<String>,
    pub work_dir: String,
    pub tag: Option<String>,
    pub shell: Option<String>,
    pub pid: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TriggerType {
    Manual,
    Dependency,
    Retry,
}

impl TriggerType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Manual => "manual",
            Self::Dependency => "dependency",
            Self::Retry => "retry",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TaskOrigin {
    pub parent_task_id: Option<i64>,
    pub retry_of_task_id: Option<i64>,
    pub trigger_type: Option<TriggerType>,
}

pub fn resolve_work_dir(cwd: Option<&PathBuf>) -> Result<PathBuf> {
    let work_dir = match cwd {
        Some(dir) => dir.clone(),
        None => std::env::current_dir()?,
    };

    if !work_dir.exists() {
        return Err(LogexError::InvalidWorkDir(format!(
            "does not exist: {}",
            work_dir.display()
        )));
    }
    if !work_dir.is_dir() {
        return Err(LogexError::InvalidWorkDir(format!(
            "not a directory: {}",
            work_dir.display()
        )));
    }

    Ok(work_dir)
}

pub fn run_task(conn: &Connection, args: RunArgs, config: &Config) -> Result<(i64, String)> {
    run_task_with_origin(conn, args, config, TaskOrigin::default())
}

pub fn submit_task_with_origin(
    conn: &Connection,
    args: &RunArgs,
    origin: TaskOrigin,
) -> Result<i64> {
    let work_dir = resolve_work_dir(args.cwd.as_ref())?;
    let command_text = args.command.join(" ");
    let command_json = encode_command_json(&args.command)?;
    let started_at = now_rfc3339();
    let shell = if args.env_files.is_empty() && args.env_vars.is_empty() {
        None
    } else {
        Some("bash".to_string())
    };

    let env_vars_text = if args.env_files.is_empty() && args.env_vars.is_empty() {
        None
    } else {
        let mut parts = Vec::new();
        for f in &args.env_files {
            parts.push(format!("-e {}", f.display()));
        }
        for v in &args.env_vars {
            parts.push(format!("-E {}", v));
        }
        Some(parts.join(" "))
    };

    conn.execute(
        "INSERT INTO tasks(tag, command, command_json, shell, work_dir, started_at, parent_task_id, retry_of_task_id, trigger_type, status, env_vars) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        params![
            args.tag.clone(),
            command_text,
            command_json,
            shell,
            work_dir.display().to_string(),
            started_at,
            origin.parent_task_id,
            origin.retry_of_task_id,
            origin.trigger_type.as_ref().map(TriggerType::as_str),
            TaskStatus::Running.as_str(),
            env_vars_text
        ],
    )?;

    Ok(conn.last_insert_rowid())
}

pub fn execute_submitted_task(
    conn: &Connection,
    task_id: i64,
    args: RunArgs,
    config: &Config,
) -> Result<String> {
    let work_dir = resolve_work_dir(args.cwd.as_ref())?;
    let started_at: String = conn.query_row(
        "SELECT started_at FROM tasks WHERE id = ?1",
        params![task_id],
        |row| row.get(0),
    )?;

    let mut cmd = if args.env_files.is_empty() && args.env_vars.is_empty() {
        let mut c = ProcessCommand::new(&args.command[0]);
        c.args(&args.command[1..]);
        c
    } else {
        let mut source_cmd = String::new();
        for env_file in &args.env_files {
            source_cmd.push_str(&format!("source {} && ", env_file.display()));
        }
        for env_var in &args.env_vars {
            source_cmd.push_str(&format!("export {} && ", env_var));
        }
        source_cmd.push_str("exec \"$@\"");

        let mut c = ProcessCommand::new("bash");
        c.arg("-c").arg(source_cmd).arg("--").args(&args.command);
        c
    };

    let mut child = match cmd
        .current_dir(&work_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(err) => {
            let message = format!("failed to start task process: {err}");
            fail_submitted_task(conn, task_id, &message)?;
            return Err(err.into());
        }
    };
    let pid = child.id();

    conn.execute(
        "UPDATE tasks SET pid = ?1 WHERE id = ?2",
        params![pid, task_id],
    )?;

    let interrupted = Arc::new(AtomicBool::new(false));
    let interrupted_clone = interrupted.clone();

    ctrlc::set_handler(move || {
        interrupted_clone.store(true, Ordering::SeqCst);
    })
    .ok();

    let stdout = child.stdout.take().ok_or_else(|| {
        let err = std::io::Error::new(
            std::io::ErrorKind::Other,
            "failed to capture stdout",
        );
        let _ = fail_submitted_task(conn, task_id, &format!("{err}"));
        LogexError::Io(err)
    })?;
    let stderr = child.stderr.take().ok_or_else(|| {
        let err = std::io::Error::new(
            std::io::ErrorKind::Other,
            "failed to capture stderr",
        );
        let _ = fail_submitted_task(conn, task_id, &format!("{err}"));
        LogexError::Io(err)
    })?;

    let (tx, rx) = mpsc::channel::<(String, String)>();

    let tx_out = tx.clone();
    let handle_out = thread::spawn(move || {
        let reader = BufReader::new(stdout);
        for line in reader.lines().map_while(std::result::Result::ok) {
            let _ = tx_out.send(("stdout".to_string(), line));
        }
    });

    let tx_err = tx.clone();
    let handle_err = thread::spawn(move || {
        let reader = BufReader::new(stderr);
        for line in reader.lines().map_while(std::result::Result::ok) {
            let _ = tx_err.send(("stderr".to_string(), line));
        }
    });

    drop(tx);

    let mut batch = Vec::new();
    let batch_size = config.defaults.batch_size;
    let batch_timeout = Duration::from_secs(config.defaults.batch_timeout_secs);
    let live_output = args.live;

    loop {
        if interrupted.load(Ordering::SeqCst) {
            let _ = child.kill();
            break;
        }

        match rx.recv_timeout(batch_timeout) {
            Ok((stream, message)) => {
                if live_output {
                    if stream == "stderr" {
                        eprintln!("{}", message);
                    } else {
                        println!("{}", message);
                    }
                }

                let ts = now_rfc3339();
                let level = detect_level(&stream);
                batch.push((ts, stream, level, message));

                if batch.len() >= batch_size {
                    flush_batch(conn, task_id, &mut batch)?;
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if !batch.is_empty() {
                    flush_batch(conn, task_id, &mut batch)?;
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }

    if !batch.is_empty() {
        flush_batch(conn, task_id, &mut batch)?;
    }

    let status = child.wait()?;
    let _ = handle_out.join();
    let _ = handle_err.join();

    let final_status = if interrupted.load(Ordering::SeqCst) {
        TaskStatus::Failed
    } else if status.success() {
        TaskStatus::Success
    } else {
        TaskStatus::Failed
    };
    finalize_task(
        conn,
        task_id,
        &started_at,
        status.code().unwrap_or(-1),
        final_status,
    )?;

    Ok(final_status.to_string())
}

pub fn run_task_with_origin(
    conn: &Connection,
    args: RunArgs,
    config: &Config,
    origin: TaskOrigin,
) -> Result<(i64, String)> {
    let task_id = submit_task_with_origin(conn, &args, origin)?;
    let final_status = execute_submitted_task(conn, task_id, args, config)?;
    Ok((task_id, final_status.to_string()))
}

pub fn validate_clear_args(args: &ClearArgs) -> Result<()> {
    let has_scoped_filter =
        args.task_id.is_some() || args.tag.is_some() || args.from.is_some() || args.to.is_some();

    if args.all {
        if !args.yes {
            return Err(LogexError::ClearValidation(
                "clear --all requires --yes confirmation".into(),
            ));
        }
        return Ok(());
    }

    if !has_scoped_filter {
        return Err(LogexError::ClearValidation(
            "refuse to clear without filter; use --all --yes for full cleanup".into(),
        ));
    }

    Ok(())
}

pub fn get_task_info(conn: &Connection, task_id: i64) -> Result<TaskRunInfo> {
    let row = fetch_task_run_record(conn, task_id)?.ok_or(LogexError::TaskNotFound(task_id))?;
    let command_args = decode_command_source(row.command_json.as_deref(), &row.command)?;

    Ok(TaskRunInfo {
        command_text: row.command,
        command_args,
        work_dir: row.work_dir,
        tag: row.tag,
        shell: row.shell,
        pid: row.pid,
    })
}

pub fn wait_for_task(conn: &Connection, task_id: i64) -> Result<String> {
    use std::thread;
    use std::time::Duration;

    loop {
        let status = fetch_task_status(conn, task_id)?.ok_or(LogexError::TaskNotFound(task_id))?;

        if status != "running" {
            return Ok(status);
        }

        thread::sleep(Duration::from_millis(500));
    }
}

fn flush_batch(
    conn: &Connection,
    task_id: i64,
    batch: &mut Vec<(String, String, LogLevel, String)>,
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }

    let tx = conn.unchecked_transaction()?;
    let mut stmt = tx.prepare(
        "INSERT INTO task_logs(task_id, ts, stream, level, message) VALUES(?1, ?2, ?3, ?4, ?5)",
    )?;

    for (ts, stream, level, message) in batch.drain(..) {
        stmt.execute(params![task_id, &ts, &stream, level.as_str(), &message])?;
    }

    drop(stmt);
    tx.commit()?;
    Ok(())
}

pub fn fail_submitted_task(conn: &Connection, task_id: i64, message: &str) -> Result<()> {
    let timestamp = now_rfc3339();
    conn.execute(
        "INSERT INTO task_logs(task_id, ts, stream, level, message) VALUES(?1, ?2, ?3, ?4, ?5)",
        params![
            task_id,
            timestamp,
            "stderr",
            LogLevel::Error.as_str(),
            message
        ],
    )?;

    let started_at: String = conn.query_row(
        "SELECT started_at FROM tasks WHERE id = ?1",
        params![task_id],
        |row| row.get(0),
    )?;
    finalize_task(conn, task_id, &started_at, -1, TaskStatus::Failed)?;
    Ok(())
}

fn finalize_task(
    conn: &Connection,
    task_id: i64,
    started_at: &str,
    exit_code: i32,
    final_status: TaskStatus,
) -> Result<()> {
    let ended_at = now_rfc3339();
    let duration_ms = compute_duration_ms(started_at, &ended_at)?;

    conn.execute(
        "UPDATE tasks SET ended_at=?1, duration_ms=?2, exit_code=?3, status=?4 WHERE id=?5",
        params![
            ended_at,
            duration_ms,
            exit_code,
            final_status.as_str(),
            task_id
        ],
    )?;

    Ok(())
}

fn compute_duration_ms(started_at: &str, ended_at: &str) -> Result<i64> {
    Ok(chrono::DateTime::parse_from_rfc3339(ended_at)
        .map_err(|e| LogexError::TimeFormat(e.to_string()))?
        .timestamp_millis()
        - chrono::DateTime::parse_from_rfc3339(started_at)
            .map_err(|e| LogexError::TimeFormat(e.to_string()))?
            .timestamp_millis())
}

fn encode_command_json(argv: &[String]) -> Result<String> {
    serde_json::to_string(&StoredCommand {
        argv: argv.to_vec(),
    })
    .map_err(|err| {
        LogexError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("failed to encode command metadata: {err}"),
        ))
    })
}

fn decode_command_source(command_json: Option<&str>, command_text: &str) -> Result<Vec<String>> {
    if let Some(command_json) = command_json {
        let stored: StoredCommand = serde_json::from_str(command_json).map_err(|err| {
            LogexError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("failed to parse command metadata: {err}"),
            ))
        })?;

        if !stored.argv.is_empty() {
            return Ok(stored.argv);
        }
    }

    let command_parts = shell_words::split(command_text).map_err(|e| {
        LogexError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("failed to parse command: {e}"),
        ))
    })?;

    if command_parts.is_empty() {
        return Err(LogexError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "empty command",
        )));
    }

    Ok(command_parts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::ExportFormat;
    use crate::exporter::render_export;
    use crate::store::{TaskListFilter, fetch_task_detail, fetch_task_list, fetch_task_logs};

    fn setup_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            r#"
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
    fn command_json_round_trips() {
        let argv = vec!["cargo".to_string(), "test".to_string(), "--lib".to_string()];
        let encoded = encode_command_json(&argv).unwrap();
        let decoded = decode_command_source(Some(&encoded), "cargo test --lib").unwrap();
        assert_eq!(decoded, argv);
    }

    #[test]
    fn get_task_info_prefers_structured_command_metadata() {
        let conn = setup_conn();
        let command_json = encode_command_json(&[
            "python".to_string(),
            "script.py".to_string(),
            "arg with spaces".to_string(),
        ])
        .unwrap();

        conn.execute(
            "INSERT INTO tasks(tag, command, command_json, shell, work_dir, started_at, pid, status) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                "demo",
                "python script.py arg\\ with\\ spaces",
                command_json,
                Option::<String>::None,
                ".",
                "2026-03-21T12:00:00+08:00",
                4321,
                "success"
            ],
        )
        .unwrap();

        let task = get_task_info(&conn, 1).unwrap();
        assert_eq!(
            task.command_args,
            vec![
                "python".to_string(),
                "script.py".to_string(),
                "arg with spaces".to_string()
            ]
        );
        assert_eq!(task.pid, Some(4321));
    }

    #[test]
    fn run_task_with_origin_persists_lineage_metadata() {
        let conn = setup_conn();
        let args = RunArgs {
            tag: Some("demo".into()),
            cwd: None,
            live: false,
            background: false,
            wait_for: None,
            env_files: vec![],
            env_vars: vec![],
            command: vec![
                "powershell".into(),
                "-Command".into(),
                "Write-Output ok".into(),
            ],
        };

        let _ = run_task_with_origin(
            &conn,
            args,
            &Config::default(),
            TaskOrigin {
                parent_task_id: Some(10),
                retry_of_task_id: Some(8),
                trigger_type: Some(TriggerType::Retry),
            },
        )
        .unwrap();

        let lineage: (Option<i64>, Option<i64>, Option<String>) = conn
            .query_row(
                "SELECT parent_task_id, retry_of_task_id, trigger_type FROM tasks WHERE id = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();

        assert_eq!(lineage.0, Some(10));
        assert_eq!(lineage.1, Some(8));
        assert_eq!(lineage.2.as_deref(), Some("retry"));
    }

    #[test]
    fn submit_task_with_origin_creates_running_task_before_execution() {
        let conn = setup_conn();
        let args = RunArgs {
            tag: Some("queue".into()),
            cwd: None,
            live: false,
            background: false,
            wait_for: None,
            env_files: vec![],
            env_vars: vec![],
            command: vec![
                "powershell".into(),
                "-Command".into(),
                "Write-Output ok".into(),
            ],
        };

        let task_id = submit_task_with_origin(
            &conn,
            &args,
            TaskOrigin {
                parent_task_id: Some(7),
                retry_of_task_id: None,
                trigger_type: Some(TriggerType::Dependency),
            },
        )
        .unwrap();

        let row: (String, Option<i64>, Option<String>, Option<i64>, Option<String>) = conn
            .query_row(
                "SELECT status, pid, ended_at, parent_task_id, trigger_type FROM tasks WHERE id = ?1",
                params![task_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .unwrap();

        assert_eq!(row.0, "running");
        assert_eq!(row.1, None);
        assert_eq!(row.2, None);
        assert_eq!(row.3, Some(7));
        assert_eq!(row.4.as_deref(), Some("dependency"));
    }

    #[test]
    fn retry_flow_surfaces_lineage_in_list_detail_and_export() {
        let conn = setup_conn();
        let config = Config::default();
        let first_args = RunArgs {
            tag: Some("demo".into()),
            cwd: None,
            live: false,
            background: false,
            wait_for: None,
            env_files: vec![],
            env_vars: vec![],
            command: vec![
                "powershell".into(),
                "-Command".into(),
                "Write-Error boom; exit 1".into(),
            ],
        };

        let (original_task_id, original_status) =
            run_task_with_origin(&conn, first_args, &config, TaskOrigin::default()).unwrap();
        assert_eq!(original_status, "failed");

        let retry_source = get_task_info(&conn, original_task_id).unwrap();
        let retry_args = RunArgs {
            tag: retry_source.tag,
            cwd: Some(PathBuf::from(retry_source.work_dir)),
            live: false,
            background: false,
            wait_for: None,
            env_files: vec![],
            env_vars: vec![],
            command: retry_source.command_args,
        };
        let (retry_task_id, retry_status) = run_task_with_origin(
            &conn,
            retry_args,
            &config,
            TaskOrigin {
                parent_task_id: Some(original_task_id),
                retry_of_task_id: Some(original_task_id),
                trigger_type: Some(TriggerType::Retry),
            },
        )
        .unwrap();
        assert_eq!(retry_status, "failed");

        let rows = fetch_task_list(
            &conn,
            &TaskListFilter {
                tag: None,
                status: None,
                lineage_filter: crate::store::LineageFilter::RetryOnly,
                limit: 10,
                offset: 0,
            },
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, retry_task_id);
        assert_eq!(rows[0].retry_of_task_id, Some(original_task_id));
        assert_eq!(rows[0].trigger_type.as_deref(), Some("retry"));

        let detail = fetch_task_detail(&conn, retry_task_id).unwrap().unwrap();
        assert_eq!(detail.parent_task_id, Some(original_task_id));
        assert_eq!(detail.retry_of_task_id, Some(original_task_id));
        assert_eq!(detail.trigger_type.as_deref(), Some("retry"));

        let logs = fetch_task_logs(&conn, retry_task_id, 0).unwrap();
        assert!(!logs.is_empty());

        let html = render_export(ExportFormat::Html, &logs, Some(&detail));
        assert!(html.contains("<th>Parent Task</th>"));
        assert!(html.contains(&format!("<td>{}</td>", original_task_id)));
        assert!(html.contains("<th>Retry Of</th>"));
        assert!(html.contains("<th>Trigger Type</th><td>retry</td>"));
        assert!(html.contains("<h2>Log Summary</h2>"));
    }

    #[test]
    fn execute_submitted_task_records_spawn_failure_log() {
        let conn = setup_conn();
        let args = RunArgs {
            tag: Some("broken".into()),
            cwd: None,
            live: false,
            background: false,
            wait_for: None,
            env_files: vec![],
            env_vars: vec![],
            command: vec!["__logex_missing_command__".into()],
        };

        let task_id = submit_task_with_origin(&conn, &args, TaskOrigin::default()).unwrap();
        let _err = execute_submitted_task(&conn, task_id, args, &Config::default())
            .expect_err("missing command should fail");

        let status: String = conn
            .query_row(
                "SELECT status FROM tasks WHERE id = ?1",
                params![task_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, "failed");

        let logs = fetch_task_logs(&conn, task_id, 0).unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].level, "error");
        assert!(logs[0].message.contains("failed to start task process"));
    }
}
