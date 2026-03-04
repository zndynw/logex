use std::collections::VecDeque;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Command as ProcessCommand, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use chrono::{Local, LocalResult, NaiveDate, NaiveDateTime, TimeZone};
use clap::{Parser, Subcommand, ValueEnum};
use comfy_table::{
    modifiers::UTF8_ROUND_CORNERS, presets::UTF8_FULL_CONDENSED, Attribute, Cell, Color,
    ContentArrangement, Table,
};
use rusqlite::{params, Connection, OptionalExtension};

fn main() {
    let cli = Cli::parse();
    if let Err(err) = run(cli) {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}

fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let (_db_path, conn) = init_storage()?;

    match cli.command {
        Command::Run(args) => {
            let (task_id, status) = run_task(&conn, args)?;
            println!("task_id={task_id} status={status}");
        }
        Command::Query(args) => {
            query_logs(&conn, args)?;
        }
        Command::List(args) => {
            list_tasks(&conn, args)?;
        }
        Command::Analyze(args) => {
            analyze_logs(&conn, args)?;
        }
        Command::Clear(args) => {
            clear_logs(&conn, args)?;
        }
    }

    Ok(())
}

fn init_storage() -> Result<(PathBuf, Connection), Box<dyn std::error::Error>> {
    let mut logex_dir = dirs::home_dir().ok_or("cannot locate user home directory")?;
    logex_dir.push(".logex");
    std::fs::create_dir_all(&logex_dir)?;

    let db_path = logex_dir.join("logex.db");
    let conn = Connection::open(&db_path)?;
    conn.execute_batch(
        r#"
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tag TEXT,
            command TEXT NOT NULL,
            work_dir TEXT NOT NULL,
            started_at TEXT NOT NULL,
            ended_at TEXT,
            duration_ms INTEGER,
            exit_code INTEGER,
            status TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS task_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id INTEGER NOT NULL,
            ts TEXT NOT NULL,
            stream TEXT NOT NULL,
            level TEXT NOT NULL,
            message TEXT NOT NULL,
            FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_tasks_tag_started_at ON tasks(tag, started_at);
        CREATE INDEX IF NOT EXISTS idx_tasks_status_started_at ON tasks(status, started_at);
        CREATE INDEX IF NOT EXISTS idx_task_logs_task_id_ts ON task_logs(task_id, ts);
        CREATE INDEX IF NOT EXISTS idx_task_logs_level_ts ON task_logs(level, ts);
        "#,
    )?;

    Ok((db_path, conn))
}

fn now_rfc3339() -> String {
    Local::now().to_rfc3339()
}

fn normalize_time_input(input: &str, is_end: bool) -> Result<String, Box<dyn std::error::Error>> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(input) {
        return Ok(dt.with_timezone(&Local).to_rfc3339());
    }

    if let Ok(naive) = NaiveDateTime::parse_from_str(input, "%Y-%m-%d %H:%M:%S") {
        return local_naive_to_rfc3339(naive);
    }

    if let Ok(naive) = NaiveDateTime::parse_from_str(input, "%Y-%m-%d %H:%M") {
        return local_naive_to_rfc3339(naive);
    }

    if let Ok(date) = NaiveDate::parse_from_str(input, "%Y-%m-%d") {
        let naive = if is_end {
            date.and_hms_opt(23, 59, 59).ok_or("invalid date")?
        } else {
            date.and_hms_opt(0, 0, 0).ok_or("invalid date")?
        };
        return local_naive_to_rfc3339(naive);
    }

    Err(format!(
        "invalid time format: {input}, supported: RFC3339 | YYYY-MM-DD | YYYY-MM-DD HH:MM[:SS]"
    )
    .into())
}

fn local_naive_to_rfc3339(naive: NaiveDateTime) -> Result<String, Box<dyn std::error::Error>> {
    let local_dt = match Local.from_local_datetime(&naive) {
        LocalResult::Single(dt) => dt,
        LocalResult::Ambiguous(dt, _) => dt,
        LocalResult::None => {
            return Err(format!("local time does not exist: {naive}").into());
        }
    };
    Ok(local_dt.to_rfc3339())
}

fn normalize_time_arg(
    input: Option<&str>,
    is_end: bool,
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    input.map(|v| normalize_time_input(v, is_end)).transpose()
}

fn detect_level(stream: &str) -> &'static str {
    match stream {
        "stdout" => "info",
        "stderr" => "error",
        _ => "unknown",
    }
}

fn resolve_work_dir(cwd: Option<&PathBuf>) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let work_dir = match cwd {
        Some(dir) => dir.clone(),
        None => std::env::current_dir()?,
    };

    if !work_dir.exists() {
        return Err(format!("work dir does not exist: {}", work_dir.display()).into());
    }
    if !work_dir.is_dir() {
        return Err(format!("work dir is not a directory: {}", work_dir.display()).into());
    }

    Ok(work_dir)
}

fn run_task(conn: &Connection, args: RunArgs) -> Result<(i64, String), Box<dyn std::error::Error>> {
    let work_dir = resolve_work_dir(args.cwd.as_ref())?;
    let command_text = args.command.join(" ");
    let started_at = now_rfc3339();

    conn.execute(
        "INSERT INTO tasks(tag, command, work_dir, started_at, status) VALUES(?1, ?2, ?3, ?4, 'running')",
        params![args.tag, command_text, work_dir.display().to_string(), started_at],
    )?;
    let task_id = conn.last_insert_rowid();

    let mut child = ProcessCommand::new(&args.command[0])
        .args(&args.command[1..])
        .current_dir(&work_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    let stdout = child.stdout.take().ok_or("failed to capture stdout")?;
    let stderr = child.stderr.take().ok_or("failed to capture stderr")?;

    let (tx, rx) = mpsc::channel::<(String, String)>();

    let tx_out = tx.clone();
    let handle_out = thread::spawn(move || {
        let reader = BufReader::new(stdout);
        for line in reader.lines().map_while(Result::ok) {
            let _ = tx_out.send(("stdout".to_string(), line));
        }
    });

    let tx_err = tx.clone();
    let handle_err = thread::spawn(move || {
        let reader = BufReader::new(stderr);
        for line in reader.lines().map_while(Result::ok) {
            let _ = tx_err.send(("stderr".to_string(), line));
        }
    });

    drop(tx);
    for (stream, message) in rx {
        let ts = now_rfc3339();
        let level = detect_level(&stream);
        conn.execute(
            "INSERT INTO task_logs(task_id, ts, stream, level, message) VALUES(?1, ?2, ?3, ?4, ?5)",
            params![task_id, ts, stream, level, message],
        )?;
    }

    let status = child.wait()?;
    let _ = handle_out.join();
    let _ = handle_err.join();

    let ended_at = now_rfc3339();
    let duration_ms = chrono::DateTime::parse_from_rfc3339(&ended_at)?.timestamp_millis()
        - chrono::DateTime::parse_from_rfc3339(&started_at)?.timestamp_millis();
    let exit_code = status.code().unwrap_or(-1);
    let final_status = if status.success() {
        "success"
    } else {
        "failed"
    };

    conn.execute(
        "UPDATE tasks SET ended_at=?1, duration_ms=?2, exit_code=?3, status=?4 WHERE id=?5",
        params![ended_at, duration_ms, exit_code, final_status, task_id],
    )?;

    Ok((task_id, final_status.to_string()))
}

fn query_logs(conn: &Connection, args: QueryArgs) -> Result<(), Box<dyn std::error::Error>> {
    let from = normalize_time_arg(args.from.as_deref(), false)?;
    let to = normalize_time_arg(args.to.as_deref(), true)?;
    let (before_ctx, after_ctx) =
        resolve_context_window(args.context, args.before_context, args.after_context);
    let grep_pattern = args.grep.map(|v| v.to_lowercase());
    let mut last_log_id = 0_i64;
    let mut pending_after = 0_usize;
    let mut before_buffer: VecDeque<QueryLogRow> = VecDeque::new();
    let mut follow_table_header_printed = false;

    if args.follow && args.tail > 0 {
        let mut tail_stmt = conn.prepare(
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

        let offset = args.tail.saturating_sub(1) as i64;
        let tail_start_id: Option<i64> = tail_stmt
            .query_row(
                params![
                    args.task_id.as_ref(),
                    args.tag.as_ref(),
                    from.as_ref(),
                    to.as_ref(),
                    args.level.as_ref(),
                    args.status.as_ref(),
                    offset
                ],
                |row| row.get::<_, i64>(0),
            )
            .optional()?;

        if let Some(start_id) = tail_start_id {
            last_log_id = start_id.saturating_sub(1);
        }
    }

    loop {
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
                from.as_ref(),
                to.as_ref(),
                args.level.as_ref(),
                args.status.as_ref(),
                last_log_id
            ],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                ))
            },
        )?;

        let mut saw_new_rows = false;
        let mut current_rows = Vec::new();
        for row in rows {
            let (log_id, task_id, tag, ts, stream, level, message, status) = row?;
            last_log_id = log_id;
            saw_new_rows = true;
            current_rows.push(QueryLogRow {
                log_id,
                task_id,
                tag,
                ts,
                stream,
                level,
                message,
                status,
            });
        }

        match args.view {
            QueryView::Summary => {
                if args.output == QueryOutput::Json {
                    print_query_summary_json(&current_rows);
                } else {
                    print_query_summary(&current_rows);
                }
            }
            QueryView::Detail => {
                if grep_pattern.is_none() {
                    match args.output {
                        QueryOutput::Plain => print_detail_rows(&current_rows, false),
                        QueryOutput::Table => {
                            if args.follow {
                                print_detail_rows_follow_table(
                                    current_rows.iter().map(|row| (row, false)),
                                    &mut follow_table_header_printed,
                                );
                            } else {
                                print_detail_rows(&current_rows, true);
                            }
                        }
                        QueryOutput::Json => print_detail_rows_json(&current_rows),
                    }
                } else {
                    let mut table_rows: Vec<(QueryLogRow, bool)> = Vec::new();

                    for row in current_rows {
                        let matched = matches_grep(&row, grep_pattern.as_deref());
                        if matched {
                            for buffered in before_buffer.drain(..) {
                                match args.output {
                                    QueryOutput::Plain => print_detail_row(&buffered, false, true),
                                    QueryOutput::Table => table_rows.push((buffered, true)),
                                    QueryOutput::Json => print_detail_row_json(&buffered, true),
                                }
                            }
                            match args.output {
                                QueryOutput::Plain => print_detail_row(&row, false, false),
                                QueryOutput::Table => table_rows.push((row.clone(), false)),
                                QueryOutput::Json => print_detail_row_json(&row, false),
                            }
                            pending_after = after_ctx;
                            continue;
                        }

                        if pending_after > 0 {
                            match args.output {
                                QueryOutput::Plain => print_detail_row(&row, false, true),
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
                            );
                        } else {
                            print_detail_rows_table(
                                table_rows
                                    .iter()
                                    .map(|(row, is_context)| (row, *is_context)),
                            );
                        }
                    }
                }
            }
        }

        if !args.follow {
            break;
        }

        if !saw_new_rows {
            thread::sleep(Duration::from_millis(args.poll_ms));
        }
    }

    Ok(())
}

fn list_tasks(conn: &Connection, args: ListArgs) -> Result<(), Box<dyn std::error::Error>> {
    let from = normalize_time_arg(args.from.as_deref(), false)?;
    let to = normalize_time_arg(args.to.as_deref(), true)?;
    let mut stmt = conn.prepare(
        r#"SELECT id, tag, command, work_dir, started_at, ended_at, duration_ms, status
           FROM tasks
           WHERE (?1 IS NULL OR tag = ?1)
             AND (?2 IS NULL OR started_at >= ?2)
             AND (?3 IS NULL OR started_at <= ?3)
           ORDER BY started_at DESC, id DESC
           LIMIT ?4 OFFSET ?5"#,
    )?;

    let rows = stmt.query_map(
        params![args.tag, from, to, args.limit, args.offset],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, Option<i64>>(6)?,
                row.get::<_, String>(7)?,
            ))
        },
    )?;

    let mut task_rows = Vec::new();
    for row in rows {
        let (id, tag, command, work_dir, started_at, ended_at, duration_ms, status) = row?;
        task_rows.push(ListTaskRow {
            id,
            tag,
            status,
            work_dir,
            started_at,
            ended_at,
            duration_ms,
            command,
        });
    }

    match args.output {
        ListOutput::Plain => {
            for row in task_rows {
                println!(
                    "id={} tag={} status={} work_dir={} started_at={} ended_at={} cmd={}",
                    row.id,
                    row.tag.as_deref().unwrap_or("-"),
                    row.status,
                    row.work_dir,
                    row.started_at,
                    row.ended_at.as_deref().unwrap_or("-"),
                    row.command
                );
            }
        }
        ListOutput::Table => print_list_rows_table(&task_rows),
    }

    Ok(())
}

#[derive(Debug, Clone)]
struct ListTaskRow {
    id: i64,
    tag: Option<String>,
    status: String,
    work_dir: String,
    started_at: String,
    ended_at: Option<String>,
    duration_ms: Option<i64>,
    command: String,
}

fn print_list_rows_table(rows: &[ListTaskRow]) {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL_CONDENSED)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![
            Cell::new("ID").add_attribute(Attribute::Bold),
            Cell::new("Tag").add_attribute(Attribute::Bold),
            Cell::new("Status").add_attribute(Attribute::Bold),
            Cell::new("Duration").add_attribute(Attribute::Bold),
            Cell::new("Started At").add_attribute(Attribute::Bold),
            Cell::new("Ended At").add_attribute(Attribute::Bold),
            Cell::new("Work Dir").add_attribute(Attribute::Bold),
            Cell::new("Command").add_attribute(Attribute::Bold),
        ]);

    for row in rows {
        let status_cell = match row.status.as_str() {
            "success" => Cell::new(&row.status).fg(Color::Green),
            "failed" => Cell::new(&row.status).fg(Color::Red),
            "running" => Cell::new(&row.status).fg(Color::Yellow),
            _ => Cell::new(&row.status),
        };

        table.add_row(vec![
            Cell::new(row.id).fg(Color::Cyan),
            Cell::new(row.tag.as_deref().unwrap_or("-")),
            status_cell,
            Cell::new(format_duration(row.duration_ms)),
            Cell::new(format_rfc3339_millis(&row.started_at)),
            Cell::new(
                row.ended_at
                    .as_deref()
                    .map(format_rfc3339_millis)
                    .unwrap_or_else(|| "-".to_string()),
            ),
            Cell::new(&row.work_dir),
            Cell::new(&row.command),
        ]);
    }

    println!("{table}");
}

fn format_rfc3339_millis(value: &str) -> String {
    chrono::DateTime::parse_from_rfc3339(value)
        .map(|dt| {
            dt.with_timezone(&Local)
                .format("%Y-%m-%d %H:%M:%S%.3f")
                .to_string()
        })
        .unwrap_or_else(|_| value.to_string())
}

fn format_duration(duration_ms: Option<i64>) -> String {
    match duration_ms {
        Some(ms) if ms >= 0 => format!("{:.3}s", (ms as f64) / 1000.0),
        _ => "-".to_string(),
    }
}

#[derive(Debug, Clone)]
struct QueryLogRow {
    log_id: i64,
    task_id: i64,
    tag: Option<String>,
    ts: String,
    stream: String,
    level: String,
    message: String,
    status: String,
}

fn print_detail_rows(rows: &[QueryLogRow], table: bool) {
    if table {
        print_detail_rows_table(rows.iter().map(|row| (row, false)));
    } else {
        for row in rows {
            print_detail_row(row, table, false);
        }
    }
}

fn print_detail_row(row: &QueryLogRow, table: bool, is_context: bool) {
    if table {
        print_detail_rows_table(std::iter::once((row, is_context)));
    } else {
        println!(
            "log_id={} task_id={} tag={} ts={} stream={} level={} status={}{} msg={}",
            row.log_id,
            row.task_id,
            row.tag.as_deref().unwrap_or("-"),
            row.ts,
            row.stream,
            row.level,
            row.status,
            if is_context { " [ctx]" } else { "" },
            row.message
        );
    }
}

fn print_detail_rows_table<'a>(rows: impl IntoIterator<Item = (&'a QueryLogRow, bool)>) {
    let table = build_detail_rows_table(rows);
    println!("{table}");
}

fn build_detail_rows_table<'a>(rows: impl IntoIterator<Item = (&'a QueryLogRow, bool)>) -> Table {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL_CONDENSED)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![
            Cell::new("Log ID").add_attribute(Attribute::Bold),
            Cell::new("Task ID").add_attribute(Attribute::Bold),
            Cell::new("Tag").add_attribute(Attribute::Bold),
            Cell::new("Timestamp").add_attribute(Attribute::Bold),
            Cell::new("Stream").add_attribute(Attribute::Bold),
            Cell::new("Level").add_attribute(Attribute::Bold),
            Cell::new("Status").add_attribute(Attribute::Bold),
            Cell::new("Type").add_attribute(Attribute::Bold),
            Cell::new("Message").add_attribute(Attribute::Bold),
        ]);

    for (row, is_context) in rows {
        let level_cell = match row.level.as_str() {
            "error" => Cell::new(&row.level).fg(Color::Red),
            "warn" => Cell::new(&row.level).fg(Color::Yellow),
            "info" => Cell::new(&row.level).fg(Color::Green),
            _ => Cell::new(&row.level),
        };
        let status_cell = match row.status.as_str() {
            "success" => Cell::new(&row.status).fg(Color::Green),
            "failed" => Cell::new(&row.status).fg(Color::Red),
            "running" => Cell::new(&row.status).fg(Color::Yellow),
            _ => Cell::new(&row.status),
        };

        table.add_row(vec![
            Cell::new(row.log_id).fg(Color::Cyan),
            Cell::new(row.task_id).fg(Color::Cyan),
            Cell::new(row.tag.as_deref().unwrap_or("-")),
            Cell::new(format_rfc3339_millis(&row.ts)),
            Cell::new(&row.stream),
            level_cell,
            status_cell,
            Cell::new(if is_context { "ctx" } else { "hit" }),
            Cell::new(&row.message),
        ]);
    }

    table
}

fn print_detail_rows_follow_table<'a>(
    rows: impl IntoIterator<Item = (&'a QueryLogRow, bool)>,
    header_printed: &mut bool,
) {
    if !*header_printed {
        println!(
            "{:<8} {:<8} {:<12} {:<23} {:<8} {:<8} {:<8} {:<4} {}",
            "Log ID", "Task ID", "Tag", "Timestamp", "Stream", "Level", "Status", "Type", "Message"
        );
        println!("{}", "-".repeat(120));
        *header_printed = true;
    }

    for (row, is_context) in rows {
        println!(
            "{:<8} {:<8} {:<12} {:<23} {:<8} {:<8} {:<8} {:<4} {}",
            row.log_id,
            row.task_id,
            row.tag.as_deref().unwrap_or("-"),
            format_rfc3339_millis(&row.ts),
            row.stream,
            row.level,
            row.status,
            if is_context { "ctx" } else { "hit" },
            row.message
        );
    }
}

fn print_query_summary(rows: &[QueryLogRow]) {
    use std::collections::BTreeMap;

    let mut by_task: BTreeMap<i64, (Option<String>, i64, i64, i64, String, String, String)> =
        BTreeMap::new();
    for row in rows {
        let entry = by_task.entry(row.task_id).or_insert_with(|| {
            (
                row.tag.clone(),
                0,
                0,
                0,
                row.ts.clone(),
                row.ts.clone(),
                row.status.clone(),
            )
        });
        entry.1 += 1;
        if row.level == "error" {
            entry.2 += 1;
        }
        if row.level == "info" {
            entry.3 += 1;
        }
        if row.ts < entry.4 {
            entry.4 = row.ts.clone();
        }
        if row.ts > entry.5 {
            entry.5 = row.ts.clone();
        }
        entry.6 = row.status.clone();
    }

    for (task_id, (tag, total, error, info, first_ts, last_ts, status)) in by_task {
        println!(
            "task_id={} tag={} status={} log_total={} info={} error={} first_ts={} last_ts={}",
            task_id,
            tag.as_deref().unwrap_or("-"),
            status,
            total,
            info,
            error,
            first_ts,
            last_ts
        );
    }
}

fn print_detail_rows_json(rows: &[QueryLogRow]) {
    for row in rows {
        print_detail_row_json(row, false);
    }
}

fn print_detail_row_json(row: &QueryLogRow, is_context: bool) {
    println!(
        "{{\"log_id\":{},\"task_id\":{},\"tag\":{},\"ts\":\"{}\",\"stream\":\"{}\",\"level\":\"{}\",\"status\":\"{}\",\"is_context\":{},\"message\":\"{}\"}}",
        row.log_id,
        row.task_id,
        json_opt_string(row.tag.as_deref()),
        json_escape(&row.ts),
        json_escape(&row.stream),
        json_escape(&row.level),
        json_escape(&row.status),
        is_context,
        json_escape(&row.message)
    );
}

fn print_query_summary_json(rows: &[QueryLogRow]) {
    use std::collections::BTreeMap;

    let mut by_task: BTreeMap<i64, (Option<String>, i64, i64, i64, String, String, String)> =
        BTreeMap::new();
    for row in rows {
        let entry = by_task.entry(row.task_id).or_insert_with(|| {
            (
                row.tag.clone(),
                0,
                0,
                0,
                row.ts.clone(),
                row.ts.clone(),
                row.status.clone(),
            )
        });
        entry.1 += 1;
        if row.level == "error" {
            entry.2 += 1;
        }
        if row.level == "info" {
            entry.3 += 1;
        }
        if row.ts < entry.4 {
            entry.4 = row.ts.clone();
        }
        if row.ts > entry.5 {
            entry.5 = row.ts.clone();
        }
        entry.6 = row.status.clone();
    }

    for (task_id, (tag, total, error, info, first_ts, last_ts, status)) in by_task {
        println!(
            "{{\"task_id\":{},\"tag\":{},\"status\":\"{}\",\"log_total\":{},\"info\":{},\"error\":{},\"first_ts\":\"{}\",\"last_ts\":\"{}\"}}",
            task_id,
            json_opt_string(tag.as_deref()),
            json_escape(&status),
            total,
            info,
            error,
            json_escape(&first_ts),
            json_escape(&last_ts)
        );
    }
}

fn json_opt_string(value: Option<&str>) -> String {
    match value {
        Some(v) => format!("\"{}\"", json_escape(v)),
        None => "null".to_string(),
    }
}

fn json_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

fn matches_grep(row: &QueryLogRow, pattern: Option<&str>) -> bool {
    let Some(p) = pattern else {
        return true;
    };

    row.message.to_lowercase().contains(p)
        || row.level.to_lowercase().contains(p)
        || row.stream.to_lowercase().contains(p)
        || row.status.to_lowercase().contains(p)
        || row.tag.as_deref().unwrap_or("-").to_lowercase().contains(p)
}

fn resolve_context_window(context: Option<usize>, before: usize, after: usize) -> (usize, usize) {
    if let Some(c) = context {
        return (c, c);
    }
    (before, after)
}

fn analyze_logs(conn: &Connection, args: AnalyzeArgs) -> Result<(), Box<dyn std::error::Error>> {
    if args.json {
        println!("[TODO] analyze --json");
        return Ok(());
    }

    let from = normalize_time_arg(args.from.as_deref(), false)?;
    let to = normalize_time_arg(args.to.as_deref(), true)?;

    let mut level_stmt = conn.prepare(
        r#"SELECT l.level, COUNT(*)
           FROM task_logs l
           JOIN tasks t ON t.id = l.task_id
           WHERE (?1 IS NULL OR t.tag = ?1)
             AND (?2 IS NULL OR l.ts >= ?2)
             AND (?3 IS NULL OR l.ts <= ?3)
           GROUP BY l.level"#,
    )?;

    let level_rows = level_stmt
        .query_map(params![args.tag, from.as_ref(), to.as_ref()], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?;

    let mut error_count = 0_i64;
    let mut warn_count = 0_i64;
    let mut info_count = 0_i64;
    let mut unknown_count = 0_i64;

    for row in level_rows {
        let (level, count) = row?;
        match level.as_str() {
            "error" => error_count = count,
            "warn" => warn_count = count,
            "info" => info_count = count,
            _ => unknown_count += count,
        }
    }

    let total_logs = error_count + warn_count + info_count + unknown_count;

    let mut task_stmt = conn.prepare(
        r#"SELECT status, COUNT(*)
           FROM tasks
           WHERE (?1 IS NULL OR tag = ?1)
             AND (?2 IS NULL OR started_at >= ?2)
             AND (?3 IS NULL OR started_at <= ?3)
             AND status IN ('success', 'failed')
           GROUP BY status"#,
    )?;

    let task_rows = task_stmt.query_map(params![args.tag, from.as_ref(), to.as_ref()], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    })?;

    let mut success_tasks = 0_i64;
    let mut failed_tasks = 0_i64;
    for row in task_rows {
        let (status, count) = row?;
        match status.as_str() {
            "success" => success_tasks = count,
            "failed" => failed_tasks = count,
            _ => {}
        }
    }

    println!("analyze_result");
    println!("log_total={}", total_logs);
    println!(
        "level=error count={} ratio={:.2}%",
        error_count,
        calc_ratio(error_count, total_logs)
    );
    println!(
        "level=warn count={} ratio={:.2}%",
        warn_count,
        calc_ratio(warn_count, total_logs)
    );
    println!(
        "level=info count={} ratio={:.2}%",
        info_count,
        calc_ratio(info_count, total_logs)
    );
    println!(
        "level=unknown count={} ratio={:.2}%",
        unknown_count,
        calc_ratio(unknown_count, total_logs)
    );
    println!(
        "task_success={} task_failed={}",
        success_tasks, failed_tasks
    );

    Ok(())
}

fn calc_ratio(count: i64, total: i64) -> f64 {
    if total <= 0 {
        return 0.0;
    }

    (count as f64) * 100.0 / (total as f64)
}

fn validate_clear_args(args: &ClearArgs) -> Result<(), Box<dyn std::error::Error>> {
    let has_scoped_filter =
        args.task_id.is_some() || args.tag.is_some() || args.from.is_some() || args.to.is_some();

    if args.all {
        if !args.yes {
            return Err("clear --all requires --yes confirmation".into());
        }
        return Ok(());
    }

    if !has_scoped_filter {
        return Err("refuse to clear without filter; use --all --yes for full cleanup".into());
    }

    Ok(())
}

fn clear_logs(conn: &Connection, args: ClearArgs) -> Result<(), Box<dyn std::error::Error>> {
    validate_clear_args(&args)?;
    let from = normalize_time_arg(args.from.as_deref(), false)?;
    let to = normalize_time_arg(args.to.as_deref(), true)?;

    let tx = conn.unchecked_transaction()?;

    let log_count: i64 = tx.query_row(
        r#"SELECT COUNT(*)
           FROM task_logs
           WHERE task_id IN (
               SELECT id
               FROM tasks
               WHERE (?1 = 1)
                  OR (
                        (?2 IS NULL OR id = ?2)
                    AND (?3 IS NULL OR tag = ?3)
                    AND (?4 IS NULL OR started_at >= ?4)
                    AND (?5 IS NULL OR started_at <= ?5)
                  )
           )"#,
        params![
            if args.all { 1 } else { 0 },
            args.task_id,
            args.tag,
            from.as_ref(),
            to.as_ref()
        ],
        |row| row.get(0),
    )?;

    let task_count: i64 = tx.query_row(
        r#"SELECT COUNT(*)
           FROM tasks
           WHERE (?1 = 1)
              OR (
                    (?2 IS NULL OR id = ?2)
                AND (?3 IS NULL OR tag = ?3)
                AND (?4 IS NULL OR started_at >= ?4)
                AND (?5 IS NULL OR started_at <= ?5)
              )"#,
        params![
            if args.all { 1 } else { 0 },
            args.task_id,
            args.tag,
            from.as_ref(),
            to.as_ref()
        ],
        |row| row.get(0),
    )?;

    tx.execute(
        r#"DELETE FROM tasks
           WHERE (?1 = 1)
              OR (
                    (?2 IS NULL OR id = ?2)
                AND (?3 IS NULL OR tag = ?3)
                AND (?4 IS NULL OR started_at >= ?4)
                AND (?5 IS NULL OR started_at <= ?5)
              )"#,
        params![
            if args.all { 1 } else { 0 },
            args.task_id,
            args.tag,
            from.as_ref(),
            to.as_ref()
        ],
    )?;

    tx.commit()?;

    println!("cleared_tasks={} cleared_logs={}", task_count, log_count);
    Ok(())
}

#[derive(Debug, Parser)]
#[command(name = "logex")]
#[command(version, about = "执行命令并管理日志", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    #[command(about = "执行命令并记录任务与日志")]
    Run(RunArgs),
    #[command(about = "按条件查询日志（支持实时跟随）")]
    Query(QueryArgs),
    #[command(about = "列出任务摘要信息")]
    List(ListArgs),
    #[command(about = "统计日志等级占比与任务结果")]
    Analyze(AnalyzeArgs),
    #[command(about = "按条件清理任务与日志")]
    Clear(ClearArgs),
}

#[derive(Debug, clap::Args)]
struct RunArgs {
    #[arg(short, long, help = "任务标签（单值）")]
    tag: Option<String>,

    #[arg(short = 'C', long, help = "命令执行目录，默认当前目录")]
    cwd: Option<PathBuf>,

    #[arg(
        required = true,
        trailing_var_arg = true,
        help = "要执行的命令与参数（需放在 -- 之后）"
    )]
    command: Vec<String>,
}

#[derive(Debug, clap::Args)]
struct QueryArgs {
    #[arg(short = 'i', long = "id", help = "按任务 ID 过滤")]
    task_id: Option<i64>,
    #[arg(short = 't', long, help = "按标签过滤")]
    tag: Option<String>,
    #[arg(
        short = 'f',
        long,
        help = "起始时间（支持 RFC3339 / YYYY-MM-DD / YYYY-MM-DD HH:MM[:SS]）"
    )]
    from: Option<String>,
    #[arg(
        short = 'T',
        long,
        help = "结束时间（支持 RFC3339 / YYYY-MM-DD / YYYY-MM-DD HH:MM[:SS]）"
    )]
    to: Option<String>,
    #[arg(short = 'l', long, help = "按日志等级过滤（error/info/warn/unknown）")]
    level: Option<String>,
    #[arg(short = 's', long, help = "按任务状态过滤（success/failed）")]
    status: Option<String>,
    #[arg(short = 'v', long, value_enum, default_value_t = QueryView::Detail, help = "输出视图（detail|summary）")]
    view: QueryView,
    #[arg(short = 'o', long, value_enum, default_value_t = QueryOutput::Plain, help = "输出格式（plain|table|json）")]
    output: QueryOutput,
    #[arg(
        short = 'g',
        long,
        help = "类似 grep 的关键词匹配（匹配 message/level/stream/status/tag）"
    )]
    grep: Option<String>,
    #[arg(
        short = 'A',
        long,
        default_value_t = 0,
        help = "输出匹配行之后的 N 行上下文"
    )]
    after_context: usize,
    #[arg(
        short = 'B',
        long,
        default_value_t = 0,
        help = "输出匹配行之前的 N 行上下文"
    )]
    before_context: usize,
    #[arg(
        short = 'C',
        long,
        help = "同时设置前后上下文 N 行（等价于 -A N -B N）"
    )]
    context: Option<usize>,
    #[arg(short = 'F', long, help = "持续轮询并实时输出新增日志")]
    follow: bool,
    #[arg(
        short = 'n',
        long,
        default_value_t = 10,
        help = "follow 模式启动时先显示最后 N 行历史日志"
    )]
    tail: usize,
    #[arg(
        short = 'p',
        long,
        default_value_t = 500,
        help = "follow 模式轮询间隔（毫秒）"
    )]
    poll_ms: u64,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
enum QueryView {
    Detail,
    Summary,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
enum QueryOutput {
    Plain,
    Table,
    Json,
}

#[derive(Debug, clap::Args)]
struct ListArgs {
    #[arg(short = 't', long, help = "按标签过滤")]
    tag: Option<String>,
    #[arg(
        short = 'f',
        long,
        help = "起始时间（支持 RFC3339 / YYYY-MM-DD / YYYY-MM-DD HH:MM[:SS]）"
    )]
    from: Option<String>,
    #[arg(
        short = 'T',
        long,
        help = "结束时间（支持 RFC3339 / YYYY-MM-DD / YYYY-MM-DD HH:MM[:SS]）"
    )]
    to: Option<String>,
    #[arg(long, value_enum, default_value_t = ListOutput::Plain, help = "输出格式（plain|table）")]
    output: ListOutput,
    #[arg(short = 'l', long, default_value_t = 50, help = "返回条数上限")]
    limit: i64,
    #[arg(short = 'o', long, default_value_t = 0, help = "分页偏移量")]
    offset: i64,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
enum ListOutput {
    Plain,
    Table,
}

#[derive(Debug, clap::Args)]
struct AnalyzeArgs {
    #[arg(short = 't', long, help = "按标签过滤")]
    tag: Option<String>,
    #[arg(
        short = 'f',
        long,
        help = "起始时间（支持 RFC3339 / YYYY-MM-DD / YYYY-MM-DD HH:MM[:SS]）"
    )]
    from: Option<String>,
    #[arg(
        short = 'T',
        long,
        help = "结束时间（支持 RFC3339 / YYYY-MM-DD / YYYY-MM-DD HH:MM[:SS]）"
    )]
    to: Option<String>,
    #[arg(short = 'j', long, help = "以 JSON 格式输出（当前为占位）")]
    json: bool,
}

#[derive(Debug, clap::Args)]
struct ClearArgs {
    #[arg(short = 'i', long = "id", help = "按任务 ID 清理")]
    task_id: Option<i64>,
    #[arg(short = 't', long, help = "按标签清理")]
    tag: Option<String>,
    #[arg(
        short = 'f',
        long,
        help = "起始时间（支持 RFC3339 / YYYY-MM-DD / YYYY-MM-DD HH:MM[:SS]）"
    )]
    from: Option<String>,
    #[arg(
        short = 'T',
        long,
        help = "结束时间（支持 RFC3339 / YYYY-MM-DD / YYYY-MM-DD HH:MM[:SS]）"
    )]
    to: Option<String>,
    #[arg(short = 'a', long, help = "全量清理（必须配合 --yes）")]
    all: bool,
    #[arg(short = 'y', long, help = "确认执行高风险清理")]
    yes: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_run_args() {
        let cli = Cli::try_parse_from([
            "logex",
            "run",
            "-t",
            "test",
            "-C",
            "/tmp",
            "--",
            "bash",
            "script.sh",
        ])
        .expect("run args should parse");

        match cli.command {
            Command::Run(args) => {
                assert_eq!(args.tag.as_deref(), Some("test"));
                assert_eq!(args.cwd.as_deref(), Some(std::path::Path::new("/tmp")));
                assert_eq!(args.command, vec!["bash", "script.sh"]);
            }
            _ => panic!("expected run command"),
        }
    }

    #[test]
    fn parse_query_defaults() {
        let cli = Cli::try_parse_from(["logex", "query"]).expect("query args should parse");

        match cli.command {
            Command::Query(args) => {
                assert!(args.task_id.is_none());
                assert!(!args.follow);
                assert_eq!(args.view, QueryView::Detail);
                assert_eq!(args.output, QueryOutput::Plain);
                assert!(args.grep.is_none());
                assert_eq!(args.after_context, 0);
                assert_eq!(args.before_context, 0);
                assert!(args.context.is_none());
                assert_eq!(args.tail, 10);
                assert_eq!(args.poll_ms, 500);
            }
            _ => panic!("expected query command"),
        }
    }

    #[test]
    fn query_context_and_grep_args_parse() {
        let cli = Cli::try_parse_from([
            "logex", "query", "-g", "error", "-C", "3", "-o", "table", "-n", "20", "-v", "summary",
        ])
        .expect("query grep/context args should parse");

        match cli.command {
            Command::Query(args) => {
                assert_eq!(args.grep.as_deref(), Some("error"));
                assert_eq!(args.context, Some(3));
                assert_eq!(args.output, QueryOutput::Table);
                assert_eq!(args.tail, 20);
                assert_eq!(args.view, QueryView::Summary);
            }
            _ => panic!("expected query command"),
        }
    }

    #[test]
    fn resolve_context_window_prefers_c() {
        assert_eq!(resolve_context_window(Some(2), 1, 5), (2, 2));
        assert_eq!(resolve_context_window(None, 1, 5), (1, 5));
    }

    #[test]
    fn parse_list_output_table() {
        let cli =
            Cli::try_parse_from(["logex", "list", "--output", "table", "-l", "10", "-o", "2"])
                .expect("list args should parse");

        match cli.command {
            Command::List(args) => {
                assert_eq!(args.output, ListOutput::Table);
                assert_eq!(args.limit, 10);
                assert_eq!(args.offset, 2);
            }
            _ => panic!("expected list command"),
        }
    }

    #[test]
    fn detail_table_uses_list_style_and_expected_headers() {
        let rows = vec![QueryLogRow {
            log_id: 1,
            task_id: 42,
            tag: Some("demo".to_string()),
            ts: "2026-03-01T10:20:30.123+08:00".to_string(),
            stream: "stdout".to_string(),
            level: "info".to_string(),
            message: "hello".to_string(),
            status: "success".to_string(),
        }];

        let table = build_detail_rows_table(rows.iter().map(|row| (row, false)));
        let rendered = table.to_string();

        assert!(rendered.contains("Log ID"));
        assert!(rendered.contains("Task ID"));
        assert!(rendered.contains("Timestamp"));
        assert!(rendered.contains("Type"));
        assert!(rendered.contains("Message"));
        assert!(rendered.contains("hit"));
        assert!(rendered.contains("hello"));
    }

    #[test]
    fn detect_level_by_stream() {
        assert_eq!(detect_level("stdout"), "info");
        assert_eq!(detect_level("stderr"), "error");
        assert_eq!(detect_level("other"), "unknown");
    }

    #[test]
    fn resolve_work_dir_with_missing_path_should_fail() {
        let result = resolve_work_dir(Some(&PathBuf::from("/path/does/not/exist")));
        assert!(result.is_err());
    }

    #[test]
    fn calc_ratio_handles_zero_total() {
        assert_eq!(calc_ratio(10, 0), 0.0);
        assert_eq!(calc_ratio(1, 4), 25.0);
    }

    #[test]
    fn clear_all_without_yes_should_fail() {
        let args = ClearArgs {
            task_id: None,
            tag: None,
            from: None,
            to: None,
            all: true,
            yes: false,
        };

        let err = validate_clear_args(&args);
        assert!(err.is_err());
    }

    #[test]
    fn clear_without_any_filter_should_fail() {
        let args = ClearArgs {
            task_id: None,
            tag: None,
            from: None,
            to: None,
            all: false,
            yes: false,
        };

        let err = validate_clear_args(&args);
        assert!(err.is_err());
    }

    #[test]
    fn normalize_time_input_supports_simple_formats() {
        let from_day = normalize_time_input("2026-03-01", false).expect("date should parse");
        assert!(from_day.contains("T00:00:00"));

        let to_day = normalize_time_input("2026-03-01", true).expect("date should parse");
        assert!(to_day.contains("T23:59:59"));

        let minute =
            normalize_time_input("2026-03-01 12:30", false).expect("minute format should parse");
        assert!(minute.contains("T12:30:00"));
    }
}
