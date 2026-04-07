use clap::Parser;
use logex::Result;
use logex::cli::*;
use logex::config;
use logex::db::init_storage;
use logex::executor::*;
use logex::handlers::*;
use logex::tui::run_tui;

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
            let mut origin = TaskOrigin {
                trigger_type: Some(TriggerType::Manual),
                ..TaskOrigin::default()
            };
            if let Some(wait_id) = args.wait_for {
                eprint!("waiting for task {} to complete...", wait_id);
                let status = wait_for_task(&conn, wait_id)?;
                eprintln!(" {}", status);

                if status != "success" {
                    eprintln!("dependency task {} failed with status: {}", wait_id, status);
                    std::process::exit(1);
                }
                origin.parent_task_id = Some(wait_id);
                origin.trigger_type = Some(TriggerType::Dependency);
            }

            let (task_id, status) = run_task_with_origin(&conn, args, &config, origin)?;
            println!("task_id={task_id} status={status}");
        }
        Command::Seed(args) => handle_seed(conn, args)?,
        Command::Tui(args) => run_tui(&conn, &db_path, &config, args)?,
        Command::Query(args) => handle_query(&conn, args, &config)?,
        Command::Export(args) => handle_export(&conn, args)?,
        Command::List(args) => handle_list(&conn, args)?,
        Command::Tags(args) => handle_tags(&conn, args)?,
        Command::Analyze(args) => handle_analyze(&conn, args)?,
        Command::Clear(args) => handle_clear(&conn, args)?,
        Command::Retry(args) => handle_retry(&conn, args, &config)?,
    }

    Ok(())
}
