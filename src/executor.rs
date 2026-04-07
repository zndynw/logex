use crate::cli::*;
use crate::config::Config;
use crate::error::{LogLevel, LogexError, Result, TaskStatus};
use crate::utils::*;
use rusqlite::{Connection, params};
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Command as ProcessCommand, Stdio};
use std::sync::{mpsc, Arc, atomic::{AtomicBool, Ordering}};
use std::thread;
use std::time::Duration;

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
    let work_dir = resolve_work_dir(args.cwd.as_ref())?;
    let command_text = args.command.join(" ");
    let started_at = now_rfc3339();

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
        "INSERT INTO tasks(tag, command, work_dir, started_at, status, env_vars) VALUES(?1, ?2, ?3, ?4, ?5, ?6)",
        params![args.tag, command_text, work_dir.display().to_string(), started_at, TaskStatus::Running.as_str(), env_vars_text],
    )?;
    let task_id = conn.last_insert_rowid();

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

    let mut child = cmd
        .current_dir(&work_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    let interrupted = Arc::new(AtomicBool::new(false));
    let interrupted_clone = interrupted.clone();

    ctrlc::set_handler(move || {
        interrupted_clone.store(true, Ordering::SeqCst);
    }).ok();

    let stdout = child.stdout.take().ok_or_else(|| {
        LogexError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            "failed to capture stdout",
        ))
    })?;
    let stderr = child.stderr.take().ok_or_else(|| {
        LogexError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            "failed to capture stderr",
        ))
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

    let ended_at = now_rfc3339();
    let duration_ms = chrono::DateTime::parse_from_rfc3339(&ended_at)
        .map_err(|e| LogexError::TimeFormat(e.to_string()))?
        .timestamp_millis()
        - chrono::DateTime::parse_from_rfc3339(&started_at)
            .map_err(|e| LogexError::TimeFormat(e.to_string()))?
            .timestamp_millis();
    let exit_code = status.code().unwrap_or(-1);
    let final_status = if interrupted.load(Ordering::SeqCst) {
        TaskStatus::Failed
    } else if status.success() {
        TaskStatus::Success
    } else {
        TaskStatus::Failed
    };

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

pub fn get_task_info(conn: &Connection, task_id: i64) -> Result<(String, String, Option<String>)> {
    conn.query_row(
        "SELECT command, work_dir, tag FROM tasks WHERE id = ?1",
        params![task_id],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        },
    )
    .map_err(|_| LogexError::TaskNotFound(task_id))
}

pub fn wait_for_task(conn: &Connection, task_id: i64) -> Result<String> {
    use std::thread;
    use std::time::Duration;

    loop {
        let status: String = conn
            .query_row(
                "SELECT status FROM tasks WHERE id = ?1",
                params![task_id],
                |row| row.get(0),
            )
            .map_err(|_| LogexError::TaskNotFound(task_id))?;

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
