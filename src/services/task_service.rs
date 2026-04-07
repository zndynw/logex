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

    Ok(())
}
