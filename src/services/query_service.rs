use crate::Result;
use crate::cli::{QueryArgs, QueryOutput, QueryView};
use crate::config::Config;
use crate::filters::{LogRowQuery, LogSearchFilter, matches_query_row};
use crate::formatter::{
    QueryHighlighter, QueryLogRow, print_detail_row, print_detail_row_json,
    print_detail_rows_follow_table, print_detail_rows_table,
};
use crate::store::{
    fetch_log_rows as fetch_store_log_rows, fetch_log_rows_fts as fetch_store_log_rows_fts,
    fetch_tail_start_id as fetch_store_tail_start_id,
};
use rusqlite::Connection;
use std::collections::VecDeque;
use std::thread;
use std::time::Duration;

pub fn handle_query(conn: &Connection, args: QueryArgs, config: &Config) -> Result<()> {
    let row_query = LogRowQuery::from_query_args(&args)?;
    let (before_ctx, after_ctx) =
        resolve_context_window(args.context, args.before_context, args.after_context);
    let filter = LogSearchFilter::from_query_args(&args);
    let highlighter = if args.no_highlight {
        None
    } else {
        QueryHighlighter::from_query_args(&args)
    };

    let tail = if args.tail > 0 {
        args.tail
    } else {
        config.defaults.tail
    };
    let mut last_log_id = resolve_initial_last_log_id(conn, &row_query, args.follow, tail)?;
    let mut pending_after = 0_usize;
    let mut before_buffer: VecDeque<QueryLogRow> = VecDeque::new();
    let mut follow_table_header_printed = false;
    let base_poll_ms = if args.poll_ms != 500 {
        args.poll_ms
    } else {
        config.defaults.poll_ms
    };
    let mut current_poll_ms;
    let mut idle_cycles = 0_u32;

    loop {
        let all_rows = fetch_log_rows(conn, &row_query, &filter, last_log_id)?;
        let saw_new_rows = !all_rows.is_empty();
        if let Some(last_row) = all_rows.last() {
            last_log_id = last_log_id.max(last_row.id);
        }

        if args.view == QueryView::Summary {
            let matched_rows: Vec<_> = all_rows
                .into_iter()
                .filter(|row| matches_query_row(row, &filter))
                .collect();
            if !matched_rows.is_empty() {
                print_summary_rows(&matched_rows, args.output);
            }
        } else {
            let mut table_rows = Vec::new();
            for row in all_rows {
                if matches_query_row(&row, &filter) {
                    for buf_row in before_buffer.drain(..) {
                        match args.output {
                            QueryOutput::Plain => {
                                print_detail_row(&buf_row, true, false, highlighter.as_ref())
                            }
                            QueryOutput::Table => table_rows.push((buf_row, true)),
                            QueryOutput::Json => print_detail_row_json(&buf_row, true),
                        }
                    }

                    match args.output {
                        QueryOutput::Plain => {
                            print_detail_row(&row, false, false, highlighter.as_ref())
                        }
                        QueryOutput::Table => table_rows.push((row.clone(), false)),
                        QueryOutput::Json => print_detail_row_json(&row, false),
                    }
                    pending_after = after_ctx;
                    continue;
                }

                if pending_after > 0 {
                    match args.output {
                        QueryOutput::Plain => {
                            print_detail_row(&row, false, true, highlighter.as_ref())
                        }
                        QueryOutput::Table => table_rows.push((row.clone(), true)),
                        QueryOutput::Json => print_detail_row_json(&row, true),
                    }
                    pending_after -= 1;
                }

                if before_ctx > 0 {
                    before_buffer.push_back(row);
                    while before_buffer.len() > before_ctx {
                        before_buffer.pop_front();
                    }
                }
            }

            if args.output == QueryOutput::Table {
                if args.follow {
                    print_detail_rows_follow_table(
                        table_rows
                            .iter()
                            .map(|(row, is_context)| (row, *is_context)),
                        &mut follow_table_header_printed,
                        highlighter.as_ref(),
                    );
                } else {
                    print_detail_rows_table(
                        table_rows
                            .iter()
                            .map(|(row, is_context)| (row, *is_context)),
                        highlighter.as_ref(),
                    );
                }
            }
        }

        if !args.follow {
            break;
        }

        if saw_new_rows {
            idle_cycles = 0;
            current_poll_ms = base_poll_ms;
        } else {
            idle_cycles += 1;
            current_poll_ms = (base_poll_ms * (1 << idle_cycles.min(4))).min(5000);
        }

        thread::sleep(Duration::from_millis(current_poll_ms));
    }

    Ok(())
}

fn resolve_context_window(context: Option<usize>, before: usize, after: usize) -> (usize, usize) {
    if let Some(c) = context {
        return (c, c);
    }
    (before, after)
}

fn resolve_initial_last_log_id(
    conn: &Connection,
    query: &LogRowQuery,
    follow: bool,
    tail: usize,
) -> Result<i64> {
    if !follow || tail == 0 {
        return Ok(0);
    }

    let tail_start_id = fetch_tail_start_id(conn, query, tail.saturating_sub(1) as i64)?;
    Ok(tail_start_id
        .map(|start_id| start_id.saturating_sub(1))
        .unwrap_or(0))
}

fn print_summary_rows(rows: &[QueryLogRow], output: QueryOutput) {
    match output {
        QueryOutput::Table | QueryOutput::Plain => {
            let total = rows.len();
            let by_level: std::collections::HashMap<_, _> =
                rows.iter()
                    .fold(std::collections::HashMap::new(), |mut acc, r| {
                        *acc.entry(&r.level).or_insert(0) += 1;
                        acc
                    });
            println!("total_logs={total}");
            for (level, count) in by_level {
                println!("level={level} count={count}");
            }
        }
        QueryOutput::Json => {
            println!("{{\"total\":{}}}", rows.len());
        }
    }
}

fn fetch_tail_start_id(conn: &Connection, query: &LogRowQuery, offset: i64) -> Result<Option<i64>> {
    fetch_store_tail_start_id(conn, query, offset)
}

fn fetch_log_rows(
    conn: &Connection,
    query: &LogRowQuery,
    filter: &LogSearchFilter,
    last_log_id: i64,
) -> Result<Vec<QueryLogRow>> {
    if filter.can_use_message_fts() {
        fetch_log_rows_fts(
            conn,
            query,
            last_log_id,
            filter.first_pattern().unwrap_or_default(),
        )
    } else {
        fetch_log_rows_standard(conn, query, last_log_id)
    }
}

fn fetch_log_rows_fts(
    conn: &Connection,
    query: &LogRowQuery,
    last_log_id: i64,
    pattern: &str,
) -> Result<Vec<QueryLogRow>> {
    fetch_store_log_rows_fts(conn, query, last_log_id, pattern)
}

fn fetch_log_rows_standard(
    conn: &Connection,
    query: &LogRowQuery,
    last_log_id: i64,
) -> Result<Vec<QueryLogRow>> {
    fetch_store_log_rows(conn, query, last_log_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{
        QueryArgs, QueryMatchMode, QueryOutput, QuerySearchField, QueryView, RunArgs,
    };
    use crate::config::Config;
    use crate::executor::run_task_with_origin;
    use crate::migrations;

    fn setup_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        migrations::migrate(&conn).unwrap();
        conn
    }

    #[test]
    fn fetch_log_rows_uses_fts_path_with_shared_filters() {
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
                "Write-Output boot; Write-Error 'deploy timeout'; Write-Error 'fatal deploy timeout'; exit 1".into(),
            ],
        };

        let (task_id, status) =
            run_task_with_origin(&conn, args, &Config::default(), Default::default()).unwrap();
        assert_eq!(status, "failed");

        let query_args = QueryArgs {
            task_id: Some(task_id),
            tag: Some("demo".into()),
            from: None,
            to: None,
            level: Some("error".into()),
            status: Some("failed".into()),
            view: QueryView::Detail,
            output: QueryOutput::Json,
            grep: vec!["timeout".into()],
            grep_mode: QueryMatchMode::Any,
            grep_fields: vec![QuerySearchField::Message],
            case_sensitive: false,
            invert_match: false,
            no_highlight: true,
            after_context: 0,
            before_context: 0,
            context: None,
            follow: false,
            tail: 10,
            poll_ms: 500,
        };
        let row_query = LogRowQuery::from_query_args(&query_args).unwrap();
        let filter = LogSearchFilter::from_query_args(&query_args);

        assert!(filter.can_use_message_fts());

        let rows = fetch_log_rows(&conn, &row_query, &filter, 0).unwrap();

        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|row| row.task_id == task_id));
        assert!(rows.iter().all(|row| row.level == "error"));
        assert!(rows.iter().all(|row| row.message.contains("timeout")));
    }

    #[test]
    fn resolve_initial_last_log_id_uses_tail_window_for_follow_mode() {
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
                "Write-Output one; Write-Output two; Write-Output three".into(),
            ],
        };

        let (task_id, status) =
            run_task_with_origin(&conn, args, &Config::default(), Default::default()).unwrap();
        assert_eq!(status, "success");

        let query = LogRowQuery {
            task_id: Some(task_id),
            tag: Some("demo".into()),
            level: None,
            status: Some("success".into()),
            time_range: Default::default(),
        };

        let last_log_id = resolve_initial_last_log_id(&conn, &query, true, 2).unwrap();
        let rows = fetch_store_log_rows(&conn, &query, last_log_id).unwrap();

        assert_eq!(rows.len(), 2);
        assert!(rows[0].message.contains("two"));
        assert!(rows[1].message.contains("three"));
    }

    #[test]
    fn resolve_initial_last_log_id_is_zero_without_follow_tail() {
        let conn = setup_conn();
        let query = LogRowQuery {
            task_id: None,
            tag: None,
            level: None,
            status: None,
            time_range: Default::default(),
        };

        assert_eq!(
            resolve_initial_last_log_id(&conn, &query, false, 10).unwrap(),
            0
        );
        assert_eq!(
            resolve_initial_last_log_id(&conn, &query, true, 0).unwrap(),
            0
        );
    }
}
