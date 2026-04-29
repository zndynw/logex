use clap::Parser;
use logex::LogexError;
use logex::Result;
use logex::cli::*;
use logex::config;
use logex::db::init_storage;
use logex::executor::*;
use logex::handlers::*;
use logex::store::fetch_task_status;
use logex::tui::run_tui;
use std::process::{Command as ProcessCommand, Stdio};

fn main() {
    let cli = Cli::parse();
    if let Err(err) = run(cli) {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}

fn run(cli: Cli) -> Result<()> {
    let (db_path, conn) = init_storage()?;
    let config = config::load_config()?;

    if let Some(days) = config.defaults.auto_cleanup_days {
        if let Ok(count) = logex::db::auto_cleanup(&conn, days) {
            if count > 0 {
                eprintln!(
                    "auto_cleanup: removed {} tasks older than {} days",
                    count, days
                );
            }
        }
    }

    match cli.command {
        Command::Run(args) => {
            let origin = TaskOrigin {
                trigger_type: Some(TriggerType::Manual),
                ..TaskOrigin::default()
            };
            let task_id = submit_task_with_origin(&conn, &args, origin)?;
            println!("task_id={task_id} status=running");

            if args.background {
                spawn_run_worker(&conn, task_id, &args)?;
            } else {
                let status = execute_run_lifecycle(&conn, &config, task_id, args)?;
                println!("task_id={task_id} status={status}");
            }
        }
        Command::RunWorker(args) => {
            let run_args = RunArgs {
                tag: None,
                cwd: args.cwd,
                live: args.live,
                background: false,
                wait_for: args.wait_for,
                env_files: args.env_files,
                env_vars: args.env_vars,
                command: args.command,
            };
            let status = execute_run_lifecycle(&conn, &config, args.task_id, run_args)?;
            eprintln!("task_id={} status={}", args.task_id, status);
        }
        Command::Seed(args) => handle_seed(conn, args)?,
        Command::Tui(args) => run_tui(&conn, &db_path, &config, args)?,
        Command::Query(args) => handle_query(&conn, args, &config)?,
        Command::Export(args) => handle_export(&conn, args)?,
        Command::List(args) => handle_list(&conn, args)?,
        Command::Tags(args) => handle_tags(&conn, args)?,
        Command::Analyze(args) => handle_analyze(&conn, args)?,
        Command::Clear(args) => handle_clear(&conn, args)?,
        Command::Vacuum => handle_vacuum(&conn)?,
        Command::Retry(args) => handle_retry(&conn, args, &config)?,
    }

    Ok(())
}

fn execute_run_lifecycle(
    conn: &rusqlite::Connection,
    config: &logex::config::Config,
    task_id: i64,
    mut args: RunArgs,
) -> Result<String> {
    if let Some(wait_id) = args.wait_for {
        eprint!("waiting for task {} to complete...", wait_id);
        let status = wait_for_task(conn, wait_id)?;
        eprintln!(" {}", status);

        if status != "success" {
            let message = format!("dependency task {} failed with status: {}", wait_id, status);
            fail_submitted_task(conn, task_id, &message)?;
            return Err(LogexError::TaskExecution(message));
        }

        conn.execute(
            "UPDATE tasks SET parent_task_id = ?1, trigger_type = ?2 WHERE id = ?3",
            rusqlite::params![wait_id, TriggerType::Dependency.as_str(), task_id],
        )?;
    }

    args.wait_for = None;

    match execute_submitted_task(conn, task_id, args, config) {
        Ok(status) => Ok(status),
        Err(err) => {
            if matches!(fetch_task_status(conn, task_id), Ok(Some(status)) if status == "running") {
                let message = format!("task execution failed before completion: {err}");
                let _ = fail_submitted_task(conn, task_id, &message);
            }
            Err(err)
        }
    }
}

fn spawn_run_worker(conn: &rusqlite::Connection, task_id: i64, args: &RunArgs) -> Result<()> {
    let current_exe = std::env::current_exe()?;
    let mut command = ProcessCommand::new(current_exe);
    command
        .arg("run-worker")
        .arg("--task-id")
        .arg(task_id.to_string());

    if let Some(wait_id) = args.wait_for {
        command.arg("--wait").arg(wait_id.to_string());
    }
    if let Some(cwd) = &args.cwd {
        command.arg("--cwd").arg(cwd);
    }
    if args.live {
        command.arg("--live");
    }
    for env_file in &args.env_files {
        command.arg("--env-file").arg(env_file);
    }
    for env_var in &args.env_vars {
        command.arg("--env").arg(env_var);
    }
    command.arg("--");
    for part in &args.command {
        command.arg(part);
    }

    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    if let Err(err) = command.spawn() {
        let message = format!("failed to start background worker: {err}");
        let _ = fail_submitted_task(conn, task_id, &message);
        return Err(err.into());
    }

    Ok(())
}
