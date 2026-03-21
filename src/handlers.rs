use crate::Result;
use crate::analyzer::{
    AnalysisFilter, AnalysisReport, collect_analysis, render_analysis_json, render_analysis_plain,
};
use crate::cli::*;
use crate::config::Config;
use crate::executor::*;
use crate::formatter::*;
use crate::seeder::seed_sample_data;
use crate::utils::*;
use rusqlite::{Connection, OptionalExtension, params};
use std::collections::VecDeque;
use std::thread;
use std::time::Duration;

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
    let from = normalize_time_arg(args.from.as_deref(), false)?;
    let to = normalize_time_arg(args.to.as_deref(), true)?;
    let (before_ctx, after_ctx) =
        resolve_context_window(args.context, args.before_context, args.after_context);
    let grep_pattern = args
        .grep
        .iter()
        .map(|s| {
            if args.case_sensitive {
                s.clone()
            } else {
                s.to_lowercase()
            }
        })
        .collect::<Vec<_>>();
    let highlighter = if args.no_highlight {
        None
    } else {
        QueryHighlighter::from_query_args(&args)
    };

    let mut last_log_id = 0_i64;
    let mut pending_after = 0_usize;
    let mut before_buffer: VecDeque<QueryLogRow> = VecDeque::new();
    let mut follow_table_header_printed = false;

    let tail = if args.tail > 0 {
        args.tail
    } else {
        config.defaults.tail
    };
    let base_poll_ms = if args.poll_ms != 500 {
        args.poll_ms
    } else {
        config.defaults.poll_ms
    };
    let mut current_poll_ms;
    let mut idle_cycles = 0_u32;

    if args.follow && tail > 0 {
        let tail_start_id = fetch_tail_start_id(
            conn,
            &args,
            from.as_deref(),
            to.as_deref(),
            tail.saturating_sub(1) as i64,
        )?;
        if let Some(start_id) = tail_start_id {
            last_log_id = start_id.saturating_sub(1);
        }
    }

    loop {
        let all_rows = fetch_log_rows(conn, &args, from.as_deref(), to.as_deref(), last_log_id)?;
        let saw_new_rows = !all_rows.is_empty();
        if let Some(last_row) = all_rows.last() {
            last_log_id = last_log_id.max(last_row.id);
        }

        if args.view == QueryView::Summary {
            let matched_rows: Vec<_> = all_rows
                .into_iter()
                .filter(|row| {
                    row_matches(row, &grep_pattern, args.case_sensitive, args.invert_match)
                })
                .collect();
            if !matched_rows.is_empty() {
                print_summary_rows(&matched_rows, args.output);
            }
        } else {
            let mut table_rows = Vec::new();
            for row in all_rows {
                if row_matches(&row, &grep_pattern, args.case_sensitive, args.invert_match) {
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

pub fn handle_export(conn: &Connection, args: ExportArgs) -> Result<()> {
    let from = normalize_time_arg(args.from.as_deref(), false)?;
    let to = normalize_time_arg(args.to.as_deref(), true)?;
    let grep_pattern = args
        .grep
        .iter()
        .map(|s| {
            if args.case_sensitive {
                s.clone()
            } else {
                s.to_lowercase()
            }
        })
        .collect::<Vec<_>>();

    let rows = fetch_export_log_rows(conn, &args, from.as_deref(), to.as_deref())?;
    let matched_rows: Vec<_> = rows
        .into_iter()
        .filter(|row| row_matches(row, &grep_pattern, args.case_sensitive, args.invert_match))
        .collect();

    if matched_rows.is_empty() {
        eprintln!("no logs matched");
        return Ok(());
    }

    let task_ids: Vec<i64> = matched_rows.iter().map(|r| r.task_id).collect();
    let unique_task_ids: std::collections::HashSet<i64> = task_ids.into_iter().collect();

    let mut tasks = Vec::new();
    for tid in unique_task_ids {
        if let Some(task) = crate::store::fetch_task_detail(conn, tid)? {
            tasks.push(task);
        }
    }

    for task in &tasks {
        let task_logs: Vec<_> = matched_rows
            .iter()
            .filter(|r| r.task_id == task.id)
            .cloned()
            .collect();
        let output = crate::exporter::render_export(args.format, &task_logs, Some(task));
        println!("{}", output);
    }

    Ok(())
}

pub fn handle_list(conn: &Connection, args: ListArgs) -> Result<()> {
    let from = normalize_time_arg(args.from.as_deref(), false)?;
    let to = normalize_time_arg(args.to.as_deref(), true)?;

    let mut sql = String::from(
        "SELECT id, tag, command, work_dir, started_at, ended_at, duration_ms, status, env_vars FROM tasks WHERE 1=1",
    );
    let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

    if let Some(ref tag) = args.tag {
        sql.push_str(" AND tag = ?");
        params_vec.push(Box::new(tag.clone()));
    }
    if let Some(ref from_ts) = from {
        sql.push_str(" AND started_at >= ?");
        params_vec.push(Box::new(from_ts.clone()));
    }
    if let Some(ref to_ts) = to {
        sql.push_str(" AND started_at <= ?");
        params_vec.push(Box::new(to_ts.clone()));
    }

    sql.push_str(" ORDER BY started_at DESC, id DESC LIMIT ? OFFSET ?");
    params_vec.push(Box::new(args.limit));
    params_vec.push(Box::new(args.offset));

    let params_refs: Vec<&dyn rusqlite::ToSql> = params_vec.iter().map(|b| b.as_ref()).collect();

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(params_refs.as_slice(), |row| {
        Ok(ListTaskRow {
            id: row.get(0)?,
            tag: row.get(1)?,
            command: row.get(2)?,
            work_dir: row.get(3)?,
            started_at: row.get(4)?,
            ended_at: row.get(5)?,
            duration_ms: row.get(6)?,
            status: row.get(7)?,
            env_vars: row.get(8)?,
        })
    })?;

    let mut task_rows = Vec::new();
    for row in rows {
        task_rows.push(row?);
    }

    if task_rows.is_empty() {
        println!("no tasks found");
        return Ok(());
    }

    match args.output {
        ListOutput::Table => print_list_rows_table(&task_rows),
        ListOutput::Plain => {
            for row in &task_rows {
                let env_info = row.env_vars.as_deref().unwrap_or("-");
                println!(
                    "id={} tag={} status={} started_at={} command={} env={}",
                    row.id,
                    row.tag.as_deref().unwrap_or("-"),
                    row.status,
                    row.started_at,
                    row.command,
                    env_info
                );
            }
        }
    }

    Ok(())
}

pub fn handle_tags(conn: &Connection, args: TagsArgs) -> Result<()> {
    let from = normalize_time_arg(args.from.as_deref(), false)?;
    let to = normalize_time_arg(args.to.as_deref(), true)?;
    let tag_rows = query_tag_rows(conn, &args, from.as_deref(), to.as_deref())?;

    if tag_rows.is_empty() {
        println!("no tags found");
        return Ok(());
    }

    match args.output {
        TagsOutput::Table => print_tags_rows_table(&tag_rows),
        TagsOutput::Plain => {
            for row in &tag_rows {
                println!("{} ({})", row.tag, row.task_count);
            }
        }
        TagsOutput::Json => {
            println!("[");
            for (i, row) in tag_rows.iter().enumerate() {
                println!(
                    "  {{\"tag\":\"{}\",\"count\":{},\"last_task_id\":{},\"last_started_at\":\"{}\"}}{}",
                    json_escape(&row.tag),
                    row.task_count,
                    row.last_task_id,
                    json_escape(&row.last_started_at),
                    if i < tag_rows.len() - 1 { "," } else { "" }
                );
            }
            println!("]");
        }
    }

    Ok(())
}

fn query_tag_rows(
    conn: &Connection,
    args: &TagsArgs,
    from: Option<&str>,
    to: Option<&str>,
) -> Result<Vec<TagRow>> {
    let mut sql = String::from(
        r#"
        WITH filtered AS (
            SELECT id, tag, started_at
            FROM tasks
            WHERE tag IS NOT NULL AND tag <> ''
        "#,
    );
    let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

    if let Some(from_ts) = from {
        sql.push_str(" AND started_at >= ?");
        params_vec.push(Box::new(from_ts.to_string()));
    }
    if let Some(to_ts) = to {
        sql.push_str(" AND started_at <= ?");
        params_vec.push(Box::new(to_ts.to_string()));
    }

    sql.push_str(
        r#"
        ),
        ranked AS (
            SELECT
                id,
                tag,
                started_at,
                COUNT(*) OVER (PARTITION BY tag) AS task_count,
                ROW_NUMBER() OVER (PARTITION BY tag ORDER BY started_at DESC, id DESC) AS row_num
            FROM filtered
        )
        SELECT tag, task_count, id, started_at
        FROM ranked
        WHERE row_num = 1
        ORDER BY started_at DESC, tag ASC
        LIMIT ? OFFSET ?
        "#,
    );
    params_vec.push(Box::new(args.limit));
    params_vec.push(Box::new(args.offset));

    let params_refs: Vec<&dyn rusqlite::ToSql> = params_vec.iter().map(|b| b.as_ref()).collect();

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(params_refs.as_slice(), |row| {
        Ok(TagRow {
            tag: row.get(0)?,
            task_count: row.get(1)?,
            last_task_id: row.get(2)?,
            last_started_at: row.get(3)?,
        })
    })?;

    let mut tag_rows = Vec::new();
    for row in rows {
        let row = row?;
        if let Some(ref grep) = args.grep {
            if !row.tag.contains(grep) {
                continue;
            }
        }
        tag_rows.push(row);
    }

    Ok(tag_rows)
}

pub fn handle_analyze(conn: &Connection, args: AnalyzeArgs) -> Result<()> {
    let from = normalize_time_arg(args.from.as_deref(), false)?;
    let to = normalize_time_arg(args.to.as_deref(), true)?;

    let filter = AnalysisFilter {
        tag: args.tag.clone(),
        from,
        to,
        top_tags: args.top_tags,
    };

    let analysis = collect_analysis(conn, &filter)?;
    println!("{}", render_analyze_output(&analysis, args.json));

    Ok(())
}

fn render_analyze_output(analysis: &AnalysisReport, json: bool) -> String {
    if json {
        render_analysis_json(analysis)
    } else {
        render_analysis_plain(analysis)
    }
}

pub fn handle_clear(conn: &Connection, args: ClearArgs) -> Result<()> {
    validate_clear_args(&args)?;

    let from = normalize_time_arg(args.from.as_deref(), false)?;
    let to = normalize_time_arg(args.to.as_deref(), true)?;

    let mut sql = String::from("DELETE FROM tasks WHERE 1=1");
    let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

    if let Some(ref id) = args.task_id {
        sql.push_str(" AND id = ?");
        params_vec.push(Box::new(*id));
    }
    if let Some(ref tag) = args.tag {
        sql.push_str(" AND tag = ?");
        params_vec.push(Box::new(tag.clone()));
    }
    if let Some(ref from_ts) = from {
        sql.push_str(" AND started_at >= ?");
        params_vec.push(Box::new(from_ts.clone()));
    }
    if let Some(ref to_ts) = to {
        sql.push_str(" AND started_at <= ?");
        params_vec.push(Box::new(to_ts.clone()));
    }

    let params_refs: Vec<&dyn rusqlite::ToSql> = params_vec.iter().map(|b| b.as_ref()).collect();

    let count = conn.execute(&sql, params_refs.as_slice())?;
    println!("cleared {} task(s)", count);

    Ok(())
}

pub fn handle_retry(conn: &Connection, args: RetryArgs, config: &Config) -> Result<()> {
    let (command, work_dir, tag) = get_task_info(conn, args.task_id)?;

    let command_parts = shell_words::split(&command).map_err(|e| {
        crate::error::LogexError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            e.to_string(),
        ))
    })?;

    let run_args = RunArgs {
        tag,
        cwd: Some(std::path::PathBuf::from(work_dir)),
        command: command_parts,
        live: args.live,
        wait_for: None,
        env_files: vec![],
        env_vars: vec![],
    };

    let (task_id, status) = run_task(conn, run_args, config)?;
    println!("task_id={task_id} status={status}");

    Ok(())
}

fn resolve_context_window(context: Option<usize>, before: usize, after: usize) -> (usize, usize) {
    if let Some(c) = context {
        return (c, c);
    }
    (before, after)
}

fn row_matches(row: &QueryLogRow, patterns: &[String], case_sensitive: bool, invert: bool) -> bool {
    if patterns.is_empty() {
        return !invert;
    }

    let search_text = if case_sensitive {
        format!(
            "{} {} {} {} {}",
            row.message,
            row.level,
            row.stream,
            row.status,
            row.tag.as_deref().unwrap_or("")
        )
    } else {
        format!(
            "{} {} {} {} {}",
            row.message,
            row.level,
            row.stream,
            row.status,
            row.tag.as_deref().unwrap_or("")
        )
        .to_lowercase()
    };

    let matched = patterns.iter().any(|p| search_text.contains(p));
    if invert { !matched } else { matched }
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
            println!("total_logs={}", total);
            for (level, count) in by_level {
                println!("level={} count={}", level, count);
            }
        }
        QueryOutput::Json => {
            println!("{{\"total\":{}}}", rows.len());
        }
    }
}

fn fetch_tail_start_id(
    conn: &Connection,
    args: &QueryArgs,
    from: Option<&str>,
    to: Option<&str>,
    offset: i64,
) -> Result<Option<i64>> {
    let mut stmt = conn.prepare(
        r#"SELECT l.id
           FROM task_logs l
           JOIN tasks t ON t.id = l.task_id
           WHERE (?1 IS NULL OR t.id = ?1)
             AND (?2 IS NULL OR t.tag = ?2)
             AND (?3 IS NULL OR l.ts >= ?3)
             AND (?4 IS NULL OR l.ts <= ?4)
             AND (?5 IS NULL OR l.level = ?5)
             AND (?6 IS NULL OR t.status = ?6)
           ORDER BY l.id DESC
           LIMIT 1 OFFSET ?7"#,
    )?;

    let result = stmt
        .query_row(
            params![
                args.task_id.as_ref(),
                args.tag.as_ref(),
                from,
                to,
                args.level.as_ref(),
                args.status.as_ref(),
                offset
            ],
            |row| row.get::<_, i64>(0),
        )
        .optional()?;

    Ok(result)
}

fn fetch_log_rows(
    conn: &Connection,
    args: &QueryArgs,
    from: Option<&str>,
    to: Option<&str>,
    last_log_id: i64,
) -> Result<Vec<QueryLogRow>> {
    let use_fts = !args.grep.is_empty() && args.grep.len() == 1 && !args.case_sensitive;

    if use_fts {
        fetch_log_rows_fts(conn, args, from, to, last_log_id, &args.grep[0])
    } else {
        fetch_log_rows_standard(conn, args, from, to, last_log_id)
    }
}

fn fetch_log_rows_fts(
    conn: &Connection,
    args: &QueryArgs,
    from: Option<&str>,
    to: Option<&str>,
    last_log_id: i64,
    pattern: &str,
) -> Result<Vec<QueryLogRow>> {
    let mut stmt = conn.prepare(
        r#"SELECT l.id, l.task_id, t.tag, l.ts, l.stream, l.level, l.message, t.status
           FROM task_logs l
           JOIN tasks t ON t.id = l.task_id
           WHERE l.id IN (SELECT rowid FROM task_logs_fts WHERE message MATCH ?1)
             AND (?2 IS NULL OR t.id = ?2)
             AND (?3 IS NULL OR t.tag = ?3)
             AND (?4 IS NULL OR l.ts >= ?4)
             AND (?5 IS NULL OR l.ts <= ?5)
             AND (?6 IS NULL OR l.level = ?6)
             AND (?7 IS NULL OR t.status = ?7)
             AND l.id > ?8
           ORDER BY l.ts ASC, l.id ASC"#,
    )?;

    let rows = stmt.query_map(
        params![
            pattern,
            args.task_id.as_ref(),
            args.tag.as_ref(),
            from,
            to,
            args.level.as_ref(),
            args.status.as_ref(),
            last_log_id
        ],
        |row| {
            Ok(QueryLogRow {
                id: row.get(0)?,
                task_id: row.get(1)?,
                tag: row.get(2)?,
                ts: row.get(3)?,
                stream: row.get(4)?,
                level: row.get(5)?,
                message: row.get(6)?,
                status: row.get(7)?,
            })
        },
    )?;

    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

fn fetch_log_rows_standard(
    conn: &Connection,
    args: &QueryArgs,
    from: Option<&str>,
    to: Option<&str>,
    last_log_id: i64,
) -> Result<Vec<QueryLogRow>> {
    let mut stmt = conn.prepare(
        r#"SELECT l.id, l.task_id, t.tag, l.ts, l.stream, l.level, l.message, t.status
           FROM task_logs l
           JOIN tasks t ON t.id = l.task_id
           WHERE (?1 IS NULL OR t.id = ?1)
             AND (?2 IS NULL OR t.tag = ?2)
             AND (?3 IS NULL OR l.ts >= ?3)
             AND (?4 IS NULL OR l.ts <= ?4)
             AND (?5 IS NULL OR l.level = ?5)
             AND (?6 IS NULL OR t.status = ?6)
             AND l.id > ?7
           ORDER BY l.ts ASC, l.id ASC"#,
    )?;

    let rows = stmt.query_map(
        params![
            args.task_id.as_ref(),
            args.tag.as_ref(),
            from,
            to,
            args.level.as_ref(),
            args.status.as_ref(),
            last_log_id
        ],
        |row| {
            Ok(QueryLogRow {
                id: row.get(0)?,
                task_id: row.get(1)?,
                tag: row.get(2)?,
                ts: row.get(3)?,
                stream: row.get(4)?,
                level: row.get(5)?,
                message: row.get(6)?,
                status: row.get(7)?,
            })
        },
    )?;

    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

fn fetch_export_log_rows(
    conn: &Connection,
    args: &ExportArgs,
    from: Option<&str>,
    to: Option<&str>,
) -> Result<Vec<QueryLogRow>> {
    let mut stmt = conn.prepare(
        r#"SELECT l.id, l.task_id, t.tag, l.ts, l.stream, l.level, l.message, t.status
           FROM task_logs l
           JOIN tasks t ON t.id = l.task_id
           WHERE (?1 IS NULL OR t.id = ?1)
             AND (?2 IS NULL OR t.tag = ?2)
             AND (?3 IS NULL OR l.ts >= ?3)
             AND (?4 IS NULL OR l.ts <= ?4)
             AND (?5 IS NULL OR l.level = ?5)
             AND (?6 IS NULL OR t.status = ?6)
           ORDER BY l.ts ASC, l.id ASC"#,
    )?;

    let rows = stmt.query_map(
        params![
            args.task_id.as_ref(),
            args.tag.as_ref(),
            from,
            to,
            args.level.as_ref(),
            args.status.as_ref(),
        ],
        |row| {
            Ok(QueryLogRow {
                id: row.get(0)?,
                task_id: row.get(1)?,
                tag: row.get(2)?,
                ts: row.get(3)?,
                stream: row.get(4)?,
                level: row.get(5)?,
                message: row.get(6)?,
                status: row.get(7)?,
            })
        },
    )?;

    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

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

        let rows = query_tag_rows(
            &conn,
            &TagsArgs {
                from: None,
                to: None,
                grep: None,
                output: TagsOutput::Table,
                limit: 50,
                offset: 0,
            },
            None,
            None,
        )
        .unwrap();

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

        let rows = query_tag_rows(
            &conn,
            &TagsArgs {
                from: None,
                to: None,
                grep: None,
                output: TagsOutput::Table,
                limit: 50,
                offset: 0,
            },
            None,
            None,
        )
        .unwrap();

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
}
