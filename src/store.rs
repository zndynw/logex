use crate::Result;
use crate::analyzer::{DurationAnalysis, LogAnalysis, TagAnalysis, TaskAnalysis};
use crate::exporter::TaskExportInfo;
use crate::filters::{LogRowQuery, NormalizedTimeRange};
use crate::formatter::{ListTaskRow, QueryLogRow, TagRow};
use rusqlite::{Connection, OptionalExtension, Row, params};

#[derive(Debug, Clone, Default)]
pub struct TaskListFilter {
    pub tag: Option<String>,
    pub status: Option<String>,
    pub lineage_filter: LineageFilter,
    pub limit: i64,
    pub offset: i64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum LineageFilter {
    #[default]
    All,
    Triggered,
    RetryOnly,
}

#[derive(Debug, Clone, Default)]
pub struct DashboardStats {
    pub total: i64,
    pub running: i64,
    pub success: i64,
    pub failed: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskRunRecord {
    pub command: String,
    pub command_json: Option<String>,
    pub work_dir: String,
    pub tag: Option<String>,
    pub shell: Option<String>,
    pub pid: Option<u32>,
}

pub fn fetch_available_tags(conn: &Connection, limit: i64) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        r#"SELECT tag
           FROM tasks
           WHERE tag IS NOT NULL AND tag <> ''
           GROUP BY tag
           ORDER BY MAX(started_at) DESC, tag ASC
           LIMIT ?1"#,
    )?;

    let rows = stmt.query_map(params![limit], |row| row.get::<_, String>(0))?;

    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

pub fn fetch_tag_rows(conn: &Connection, filter: &crate::filters::TagListFilter) -> Result<Vec<TagRow>> {
    let mut sql = String::from(
        r#"
        WITH filtered AS (
            SELECT id, tag, started_at
            FROM tasks
            WHERE tag IS NOT NULL AND tag <> ''
        "#,
    );
    let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

    if let Some(from_ts) = &filter.time_range.from {
        sql.push_str(" AND started_at >= ?");
        params_vec.push(Box::new(from_ts.to_string()));
    }
    if let Some(to_ts) = &filter.time_range.to {
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
    params_vec.push(Box::new(filter.pagination.limit));
    params_vec.push(Box::new(filter.pagination.offset));

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
        if let Some(ref grep) = filter.grep && !row.tag.contains(grep) {
            continue;
        }
        tag_rows.push(row);
    }

    Ok(tag_rows)
}

pub fn fetch_task_list(conn: &Connection, filter: &TaskListFilter) -> Result<Vec<ListTaskRow>> {
    fetch_task_list_with_range(
        conn,
        filter.tag.as_deref(),
        None,
        &NormalizedTimeRange::default(),
        filter.lineage_filter,
        filter.limit,
        filter.offset,
    )
}

pub fn fetch_task_list_with_range(
    conn: &Connection,
    tag: Option<&str>,
    status: Option<&str>,
    time_range: &NormalizedTimeRange,
    lineage_filter: LineageFilter,
    limit: i64,
    offset: i64,
) -> Result<Vec<ListTaskRow>> {
    let mut stmt = conn.prepare(
        r#"SELECT id, tag, command, shell, work_dir, started_at, ended_at, duration_ms, pid, parent_task_id, retry_of_task_id, trigger_type, status, env_vars
           FROM tasks
           WHERE (?1 IS NULL OR tag = ?1)
             AND (?2 IS NULL OR status = ?2)
             AND (?3 IS NULL OR started_at >= ?3)
             AND (?4 IS NULL OR started_at <= ?4)
             AND (
                    ?5 = 'all'
                    OR (?5 = 'triggered' AND (parent_task_id IS NOT NULL OR retry_of_task_id IS NOT NULL OR (trigger_type IS NOT NULL AND trigger_type <> 'manual')))
                    OR (?5 = 'retry' AND (retry_of_task_id IS NOT NULL OR trigger_type = 'retry'))
                 )
           ORDER BY started_at DESC, id DESC
           LIMIT ?6 OFFSET ?7"#,
    )?;

    let rows = stmt.query_map(
        params![
            tag,
            status,
            time_range.from.as_deref(),
            time_range.to.as_deref(),
            lineage_filter.as_sql_value(),
            limit,
            offset
        ],
        map_task_list_row,
    )?;

    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

impl LineageFilter {
    pub fn as_sql_value(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Triggered => "triggered",
            Self::RetryOnly => "retry",
        }
    }
}

fn map_task_list_row(row: &Row<'_>) -> rusqlite::Result<ListTaskRow> {
    Ok(ListTaskRow {
        id: row.get(0)?,
        tag: row.get(1)?,
        command: row.get(2)?,
        shell: row.get(3)?,
        work_dir: row.get(4)?,
        started_at: row.get(5)?,
        ended_at: row.get(6)?,
        duration_ms: row.get(7)?,
        pid: row.get(8)?,
        parent_task_id: row.get(9)?,
        retry_of_task_id: row.get(10)?,
        trigger_type: row.get(11)?,
        status: row.get(12)?,
        env_vars: row.get(13)?,
    })
}

pub fn fetch_dashboard_stats(conn: &Connection, tag: Option<&str>) -> Result<DashboardStats> {
    let mut stmt = conn.prepare(
        r#"SELECT
                COUNT(*) AS total,
                COALESCE(SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END), 0) AS running,
                COALESCE(SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END), 0) AS success,
                COALESCE(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END), 0) AS failed
           FROM tasks
           WHERE (?1 IS NULL OR tag = ?1)"#,
    )?;

    let stats = stmt.query_row(params![tag], |row| {
        Ok(DashboardStats {
            total: row.get(0)?,
            running: row.get(1)?,
            success: row.get(2)?,
            failed: row.get(3)?,
        })
    })?;

    Ok(stats)
}

pub fn fetch_log_analysis_summary(
    conn: &Connection,
    tag: Option<&str>,
    time_range: &NormalizedTimeRange,
) -> Result<LogAnalysis> {
    let mut analysis = LogAnalysis::default();

    let mut level_stmt = conn.prepare(
        r#"SELECT l.level, COUNT(*) FROM task_logs l JOIN tasks t ON t.id = l.task_id
           WHERE (?1 IS NULL OR t.tag = ?1)
             AND (?2 IS NULL OR l.ts >= ?2)
             AND (?3 IS NULL OR l.ts <= ?3)
           GROUP BY l.level"#,
    )?;

    let level_rows = level_stmt.query_map(
        params![tag, time_range.from.as_deref(), time_range.to.as_deref()],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
    )?;

    for row in level_rows {
        let (level, count) = row?;
        match level.as_str() {
            "error" => analysis.error = count,
            "warn" => analysis.warn = count,
            "info" => analysis.info = count,
            _ => analysis.unknown += count,
        }
    }
    analysis.total = analysis.error + analysis.warn + analysis.info + analysis.unknown;

    let mut stream_stmt = conn.prepare(
        r#"SELECT l.stream, COUNT(*) FROM task_logs l JOIN tasks t ON t.id = l.task_id
           WHERE (?1 IS NULL OR t.tag = ?1)
             AND (?2 IS NULL OR l.ts >= ?2)
             AND (?3 IS NULL OR l.ts <= ?3)
           GROUP BY l.stream"#,
    )?;

    let stream_rows = stream_stmt.query_map(
        params![tag, time_range.from.as_deref(), time_range.to.as_deref()],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
    )?;

    for row in stream_rows {
        let (stream, count) = row?;
        match stream.as_str() {
            "stdout" => analysis.stdout = count,
            "stderr" => analysis.stderr = count,
            _ => analysis.other_streams += count,
        }
    }

    let ts_range: Option<(Option<String>, Option<String>)> = conn
        .prepare(
            r#"SELECT MIN(l.ts), MAX(l.ts) FROM task_logs l JOIN tasks t ON t.id = l.task_id
               WHERE (?1 IS NULL OR t.tag = ?1)
                 AND (?2 IS NULL OR l.ts >= ?2)
                 AND (?3 IS NULL OR l.ts <= ?3)"#,
        )?
        .query_row(
            params![tag, time_range.from.as_deref(), time_range.to.as_deref()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;

    if let Some((first_ts, last_ts)) = ts_range {
        analysis.first_ts = first_ts;
        analysis.last_ts = last_ts;
    }

    Ok(analysis)
}

pub fn fetch_task_analysis_summary(
    conn: &Connection,
    tag: Option<&str>,
    time_range: &NormalizedTimeRange,
) -> Result<TaskAnalysis> {
    let mut analysis = TaskAnalysis::default();
    let mut stmt = conn.prepare(
        r#"SELECT status, COUNT(*) FROM tasks
           WHERE (?1 IS NULL OR tag = ?1)
             AND (?2 IS NULL OR started_at >= ?2)
             AND (?3 IS NULL OR started_at <= ?3)
           GROUP BY status"#,
    )?;

    let rows = stmt.query_map(
        params![tag, time_range.from.as_deref(), time_range.to.as_deref()],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
    )?;

    for row in rows {
        let (status, count) = row?;
        match status.as_str() {
            "running" => analysis.running = count,
            "success" => analysis.success = count,
            "failed" => analysis.failed = count,
            _ => {}
        }
    }

    analysis.total = analysis.running + analysis.success + analysis.failed;
    Ok(analysis)
}

pub fn fetch_duration_analysis_summary(
    conn: &Connection,
    tag: Option<&str>,
    time_range: &NormalizedTimeRange,
) -> Result<DurationAnalysis> {
    conn.prepare(
        r#"SELECT COUNT(*), MIN(duration_ms), AVG(duration_ms), MAX(duration_ms)
           FROM tasks
           WHERE (?1 IS NULL OR tag = ?1)
             AND (?2 IS NULL OR started_at >= ?2)
             AND (?3 IS NULL OR started_at <= ?3)
             AND status IN ('success', 'failed')
             AND duration_ms IS NOT NULL"#,
    )?
    .query_row(
        params![tag, time_range.from.as_deref(), time_range.to.as_deref()],
        |row| {
            Ok(DurationAnalysis {
                finished_count: row.get(0)?,
                min_ms: row.get(1)?,
                avg_ms: row.get(2)?,
                max_ms: row.get(3)?,
            })
        },
    )
    .map_err(Into::into)
}

pub fn fetch_top_tag_analysis(
    conn: &Connection,
    tag: Option<&str>,
    time_range: &NormalizedTimeRange,
    limit: usize,
) -> Result<Vec<TagAnalysis>> {
    if limit == 0 {
        return Ok(Vec::new());
    }

    let mut stmt = conn.prepare(
        r#"SELECT COALESCE(NULLIF(t.tag, ''), '(untagged)') AS tag,
                  COUNT(DISTINCT t.id) AS task_count,
                  COUNT(l.id) AS log_count,
                  COALESCE(SUM(CASE WHEN l.level = 'error' THEN 1 ELSE 0 END), 0) AS error_count,
                  COALESCE(SUM(CASE WHEN l.level = 'warn' THEN 1 ELSE 0 END), 0) AS warn_count,
                  COALESCE(SUM(CASE WHEN l.level = 'info' THEN 1 ELSE 0 END), 0) AS info_count,
                  COALESCE(SUM(CASE WHEN l.level NOT IN ('error', 'warn', 'info') THEN 1 ELSE 0 END), 0) AS unknown_count,
                  MAX(t.started_at) AS last_started_at
           FROM tasks t
           LEFT JOIN task_logs l ON l.task_id = t.id
             AND (?2 IS NULL OR l.ts >= ?2)
             AND (?3 IS NULL OR l.ts <= ?3)
           WHERE (?1 IS NULL OR t.tag = ?1)
             AND (?2 IS NULL OR t.started_at >= ?2)
             AND (?3 IS NULL OR t.started_at <= ?3)
           GROUP BY COALESCE(NULLIF(t.tag, ''), '(untagged)')
           ORDER BY error_count DESC, log_count DESC, task_count DESC, tag ASC
           LIMIT ?4"#,
    )?;

    let rows = stmt.query_map(
        params![tag, time_range.from.as_deref(), time_range.to.as_deref(), limit as i64],
        |row| {
            Ok(TagAnalysis {
                tag: row.get(0)?,
                task_count: row.get(1)?,
                log_count: row.get(2)?,
                error_count: row.get(3)?,
                warn_count: row.get(4)?,
                info_count: row.get(5)?,
                unknown_count: row.get(6)?,
                last_started_at: row.get(7)?,
            })
        },
    )?;

    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

pub fn fetch_task_detail(conn: &Connection, task_id: i64) -> Result<Option<TaskExportInfo>> {
    conn.query_row(
        "SELECT id, tag, command, command_json, shell, work_dir, started_at, ended_at, duration_ms, pid, parent_task_id, retry_of_task_id, trigger_type, exit_code, status, env_vars FROM tasks WHERE id = ?1",
        params![task_id],
        |row| {
            Ok(TaskExportInfo {
                id: row.get(0)?,
                tag: row.get(1)?,
                command: row.get(2)?,
                command_json: row.get(3)?,
                shell: row.get(4)?,
                work_dir: row.get(5)?,
                started_at: row.get(6)?,
                ended_at: row.get(7)?,
                duration_ms: row.get(8)?,
                pid: row.get(9)?,
                parent_task_id: row.get(10)?,
                retry_of_task_id: row.get(11)?,
                trigger_type: row.get(12)?,
                exit_code: row.get(13)?,
                status: row.get(14)?,
                env_vars: row.get(15)?,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

pub fn fetch_task_run_record(conn: &Connection, task_id: i64) -> Result<Option<TaskRunRecord>> {
    conn.query_row(
        "SELECT command, command_json, work_dir, tag, shell, pid FROM tasks WHERE id = ?1",
        params![task_id],
        |row| {
            Ok(TaskRunRecord {
                command: row.get(0)?,
                command_json: row.get(1)?,
                work_dir: row.get(2)?,
                tag: row.get(3)?,
                shell: row.get(4)?,
                pid: row.get(5)?,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

pub fn fetch_task_status(conn: &Connection, task_id: i64) -> Result<Option<String>> {
    conn.query_row(
        "SELECT status FROM tasks WHERE id = ?1",
        params![task_id],
        |row| row.get::<_, String>(0),
    )
    .optional()
    .map_err(Into::into)
}

pub fn fetch_task_logs(
    conn: &Connection,
    task_id: i64,
    after_log_id: i64,
) -> Result<Vec<QueryLogRow>> {
    fetch_log_rows(
        conn,
        &LogRowQuery {
            task_id: Some(task_id),
            tag: None,
            level: None,
            status: None,
            time_range: NormalizedTimeRange::default(),
        },
        after_log_id,
    )
}

pub fn fetch_log_rows(
    conn: &Connection,
    query: &LogRowQuery,
    after_log_id: i64,
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

    let rows = stmt.query_map(params![
        query.task_id,
        query.tag.as_deref(),
        query.time_range.from.as_deref(),
        query.time_range.to.as_deref(),
        query.level.as_deref(),
        query.status.as_deref(),
        after_log_id
    ], |row| {
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
    })?;

    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

pub fn fetch_log_rows_fts(
    conn: &Connection,
    query: &LogRowQuery,
    after_log_id: i64,
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
            query.task_id,
            query.tag.as_deref(),
            query.time_range.from.as_deref(),
            query.time_range.to.as_deref(),
            query.level.as_deref(),
            query.status.as_deref(),
            after_log_id
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

pub fn fetch_tail_start_id(
    conn: &Connection,
    query: &LogRowQuery,
    offset: i64,
) -> Result<Option<i64>> {
    conn.prepare(
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
    )?
    .query_row(
        params![
            query.task_id,
            query.tag.as_deref(),
            query.time_range.from.as_deref(),
            query.time_range.to.as_deref(),
            query.level.as_deref(),
            query.status.as_deref(),
            offset
        ],
        |row| row.get::<_, i64>(0),
    )
    .optional()
    .map_err(Into::into)
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
            CREATE VIRTUAL TABLE task_logs_fts USING fts5(
                message,
                content=task_logs,
                content_rowid=id
            );
            CREATE TRIGGER task_logs_ai AFTER INSERT ON task_logs BEGIN
                INSERT INTO task_logs_fts(rowid, message) VALUES (new.id, new.message);
            END;
            CREATE TRIGGER task_logs_ad AFTER DELETE ON task_logs BEGIN
                DELETE FROM task_logs_fts WHERE rowid = old.id;
            END;
            CREATE TRIGGER task_logs_au AFTER UPDATE ON task_logs BEGIN
                DELETE FROM task_logs_fts WHERE rowid = old.id;
                INSERT INTO task_logs_fts(rowid, message) VALUES (new.id, new.message);
            END;
            "#,
        )
        .unwrap();
        conn
    }

    #[test]
    fn fetch_log_rows_applies_shared_query_filters() {
        let conn = setup_conn();
        conn.execute(
            "INSERT INTO tasks(tag, command, shell, work_dir, started_at, status) VALUES(?1, ?2, ?3, ?4, ?5, ?6)",
            params!["demo", "cargo test", "pwsh", ".", "2026-03-21T12:00:00+08:00", "failed"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO tasks(tag, command, shell, work_dir, started_at, status) VALUES(?1, ?2, ?3, ?4, ?5, ?6)",
            params!["other", "cargo build", "pwsh", ".", "2026-03-21T12:00:00+08:00", "success"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO task_logs(task_id, ts, stream, level, message) VALUES(?1, ?2, ?3, ?4, ?5)",
            params![1, "2026-03-21T12:00:05+08:00", "stderr", "error", "boom"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO task_logs(task_id, ts, stream, level, message) VALUES(?1, ?2, ?3, ?4, ?5)",
            params![2, "2026-03-21T12:00:05+08:00", "stdout", "info", "ok"],
        )
        .unwrap();

        let rows = fetch_log_rows(
            &conn,
            &LogRowQuery {
                task_id: None,
                tag: Some("demo".into()),
                level: Some("error".into()),
                status: Some("failed".into()),
                time_range: NormalizedTimeRange {
                    from: Some("2026-03-21T12:00:00+08:00".into()),
                    to: Some("2026-03-21T12:01:00+08:00".into()),
                },
            },
            0,
        )
        .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].task_id, 1);
        assert_eq!(rows[0].message, "boom");
    }

    #[test]
    fn fetch_task_run_record_reads_structured_retry_source_fields() {
        let conn = setup_conn();
        conn.execute(
            "INSERT INTO tasks(tag, command, command_json, shell, work_dir, started_at, pid, status) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                "demo",
                "cargo test --lib",
                "[\"cargo\",\"test\",\"--lib\"]",
                "pwsh",
                ".",
                "2026-03-21T12:00:00+08:00",
                4321,
                "success"
            ],
        )
        .unwrap();

        let record = fetch_task_run_record(&conn, 1).unwrap().unwrap();

        assert_eq!(record.command, "cargo test --lib");
        assert_eq!(record.command_json.as_deref(), Some("[\"cargo\",\"test\",\"--lib\"]"));
        assert_eq!(record.shell.as_deref(), Some("pwsh"));
        assert_eq!(record.pid, Some(4321));
        assert_eq!(record.tag.as_deref(), Some("demo"));
    }

    #[test]
    fn fetch_task_status_returns_current_status() {
        let conn = setup_conn();
        conn.execute(
            "INSERT INTO tasks(tag, command, shell, work_dir, started_at, status) VALUES(?1, ?2, ?3, ?4, ?5, ?6)",
            params!["demo", "cargo test", "pwsh", ".", "2026-03-21T12:00:00+08:00", "running"],
        )
        .unwrap();

        let status = fetch_task_status(&conn, 1).unwrap();

        assert_eq!(status.as_deref(), Some("running"));
    }

    #[test]
    fn fetch_task_analysis_summary_counts_statuses() {
        let conn = setup_conn();
        for (status, started_at) in [
            ("running", "2026-03-21T12:00:00+08:00"),
            ("success", "2026-03-21T12:01:00+08:00"),
            ("failed", "2026-03-21T12:02:00+08:00"),
        ] {
            conn.execute(
                "INSERT INTO tasks(tag, command, shell, work_dir, started_at, status) VALUES(?1, ?2, ?3, ?4, ?5, ?6)",
                params!["demo", "cargo test", "pwsh", ".", started_at, status],
            )
            .unwrap();
        }

        let summary = fetch_task_analysis_summary(
            &conn,
            Some("demo"),
            &NormalizedTimeRange {
                from: Some("2026-03-21T12:00:00+08:00".into()),
                to: Some("2026-03-21T12:05:00+08:00".into()),
            },
        )
        .unwrap();

        assert_eq!(summary.total, 3);
        assert_eq!(summary.running, 1);
        assert_eq!(summary.success, 1);
        assert_eq!(summary.failed, 1);
    }

    #[test]
    fn fetch_log_analysis_summary_counts_levels_streams_and_window() {
        let conn = setup_conn();
        conn.execute(
            "INSERT INTO tasks(tag, command, shell, work_dir, started_at, status) VALUES(?1, ?2, ?3, ?4, ?5, ?6)",
            params!["demo", "cargo test", "pwsh", ".", "2026-03-21T12:00:00+08:00", "failed"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO task_logs(task_id, ts, stream, level, message) VALUES(?1, ?2, ?3, ?4, ?5)",
            params![1, "2026-03-21T12:00:05+08:00", "stdout", "info", "start"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO task_logs(task_id, ts, stream, level, message) VALUES(?1, ?2, ?3, ?4, ?5)",
            params![1, "2026-03-21T12:00:10+08:00", "stderr", "warn", "retry soon"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO task_logs(task_id, ts, stream, level, message) VALUES(?1, ?2, ?3, ?4, ?5)",
            params![1, "2026-03-21T12:00:20+08:00", "stderr", "error", "failed hard"],
        )
        .unwrap();

        let summary = fetch_log_analysis_summary(
            &conn,
            Some("demo"),
            &NormalizedTimeRange {
                from: Some("2026-03-21T12:00:00+08:00".into()),
                to: Some("2026-03-21T12:01:00+08:00".into()),
            },
        )
        .unwrap();

        assert_eq!(summary.total, 3);
        assert_eq!(summary.info, 1);
        assert_eq!(summary.warn, 1);
        assert_eq!(summary.error, 1);
        assert_eq!(summary.stdout, 1);
        assert_eq!(summary.stderr, 2);
        assert_eq!(summary.first_ts.as_deref(), Some("2026-03-21T12:00:05+08:00"));
        assert_eq!(summary.last_ts.as_deref(), Some("2026-03-21T12:00:20+08:00"));
    }

    #[test]
    fn fetch_duration_analysis_summary_uses_finished_tasks_only() {
        let conn = setup_conn();
        conn.execute(
            "INSERT INTO tasks(tag, command, shell, work_dir, started_at, duration_ms, status) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params!["demo", "cargo test", "pwsh", ".", "2026-03-21T12:00:00+08:00", 1000, "success"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO tasks(tag, command, shell, work_dir, started_at, duration_ms, status) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params!["demo", "cargo build", "pwsh", ".", "2026-03-21T12:01:00+08:00", 3000, "failed"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO tasks(tag, command, shell, work_dir, started_at, duration_ms, status) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params!["demo", "cargo run", "pwsh", ".", "2026-03-21T12:02:00+08:00", 9000, "running"],
        )
        .unwrap();

        let summary = fetch_duration_analysis_summary(
            &conn,
            Some("demo"),
            &NormalizedTimeRange {
                from: Some("2026-03-21T12:00:00+08:00".into()),
                to: Some("2026-03-21T12:05:00+08:00".into()),
            },
        )
        .unwrap();

        assert_eq!(summary.finished_count, 2);
        assert_eq!(summary.min_ms, Some(1000));
        assert_eq!(summary.max_ms, Some(3000));
        assert_eq!(summary.avg_ms, Some(2000.0));
    }

    #[test]
    fn fetch_top_tag_analysis_orders_by_error_and_log_volume() {
        let conn = setup_conn();
        conn.execute(
            "INSERT INTO tasks(tag, command, shell, work_dir, started_at, status) VALUES(?1, ?2, ?3, ?4, ?5, ?6)",
            params!["alpha", "cargo test", "pwsh", ".", "2026-03-21T12:00:00+08:00", "failed"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO tasks(tag, command, shell, work_dir, started_at, status) VALUES(?1, ?2, ?3, ?4, ?5, ?6)",
            params!["beta", "cargo build", "pwsh", ".", "2026-03-21T12:01:00+08:00", "success"],
        )
        .unwrap();
        for (task_id, level, message) in [
            (1_i64, "error", "a1"),
            (1_i64, "warn", "a2"),
            (2_i64, "info", "b1"),
            (2_i64, "info", "b2"),
        ] {
            conn.execute(
                "INSERT INTO task_logs(task_id, ts, stream, level, message) VALUES(?1, ?2, ?3, ?4, ?5)",
                params![task_id, "2026-03-21T12:00:10+08:00", "stderr", level, message],
            )
            .unwrap();
        }

        let rows = fetch_top_tag_analysis(
            &conn,
            None,
            &NormalizedTimeRange {
                from: Some("2026-03-21T12:00:00+08:00".into()),
                to: Some("2026-03-21T12:05:00+08:00".into()),
            },
            5,
        )
        .unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].tag, "alpha");
        assert_eq!(rows[0].error_count, 1);
        assert_eq!(rows[1].tag, "beta");
        assert_eq!(rows[1].log_count, 2);
    }

    #[test]
    fn fetch_log_rows_fts_applies_message_match_and_shared_filters() {
        let conn = setup_conn();
        conn.execute(
            "INSERT INTO tasks(tag, command, shell, work_dir, started_at, status) VALUES(?1, ?2, ?3, ?4, ?5, ?6)",
            params!["demo", "cargo test", "pwsh", ".", "2026-03-21T12:00:00+08:00", "failed"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO tasks(tag, command, shell, work_dir, started_at, status) VALUES(?1, ?2, ?3, ?4, ?5, ?6)",
            params!["other", "cargo build", "pwsh", ".", "2026-03-21T12:00:00+08:00", "failed"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO task_logs(task_id, ts, stream, level, message) VALUES(?1, ?2, ?3, ?4, ?5)",
            params![1, "2026-03-21T12:00:05+08:00", "stderr", "error", "timeout while deploy"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO task_logs(task_id, ts, stream, level, message) VALUES(?1, ?2, ?3, ?4, ?5)",
            params![2, "2026-03-21T12:00:06+08:00", "stderr", "error", "timeout while deploy"],
        )
        .unwrap();

        let rows = fetch_log_rows_fts(
            &conn,
            &LogRowQuery {
                task_id: None,
                tag: Some("demo".into()),
                level: Some("error".into()),
                status: Some("failed".into()),
                time_range: NormalizedTimeRange {
                    from: Some("2026-03-21T12:00:00+08:00".into()),
                    to: Some("2026-03-21T12:01:00+08:00".into()),
                },
            },
            0,
            "timeout",
        )
        .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].task_id, 1);
        assert!(rows[0].message.contains("timeout"));
    }

    #[test]
    fn fetch_tail_start_id_respects_shared_filters_and_offset() {
        let conn = setup_conn();
        conn.execute(
            "INSERT INTO tasks(tag, command, shell, work_dir, started_at, status) VALUES(?1, ?2, ?3, ?4, ?5, ?6)",
            params!["demo", "cargo test", "pwsh", ".", "2026-03-21T12:00:00+08:00", "failed"],
        )
        .unwrap();
        for (id, ts, level, message) in [
            (1_i64, "2026-03-21T12:00:01+08:00", "info", "a"),
            (1_i64, "2026-03-21T12:00:02+08:00", "error", "b"),
            (1_i64, "2026-03-21T12:00:03+08:00", "error", "c"),
        ] {
            conn.execute(
                "INSERT INTO task_logs(task_id, ts, stream, level, message) VALUES(?1, ?2, ?3, ?4, ?5)",
                params![id, ts, "stderr", level, message],
            )
            .unwrap();
        }

        let query = LogRowQuery {
            task_id: Some(1),
            tag: Some("demo".into()),
            level: Some("error".into()),
            status: Some("failed".into()),
            time_range: NormalizedTimeRange {
                from: Some("2026-03-21T12:00:00+08:00".into()),
                to: Some("2026-03-21T12:01:00+08:00".into()),
            },
        };

        let latest = fetch_tail_start_id(&conn, &query, 0).unwrap();
        let previous = fetch_tail_start_id(&conn, &query, 1).unwrap();

        assert_eq!(latest, Some(3));
        assert_eq!(previous, Some(2));
    }

    #[test]
    fn fetch_task_list_can_filter_retry_lineage() {
        let conn = setup_conn();
        conn.execute(
            "INSERT INTO tasks(tag, command, shell, work_dir, started_at, pid, retry_of_task_id, trigger_type, status) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                "demo",
                "cargo test",
                "pwsh",
                ".",
                "2026-03-21T12:00:00+08:00",
                1001,
                4,
                "retry",
                "failed"
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO tasks(tag, command, shell, work_dir, started_at, pid, trigger_type, status) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                "demo",
                "cargo build",
                "pwsh",
                ".",
                "2026-03-21T12:01:00+08:00",
                1002,
                "manual",
                "success"
            ],
        )
        .unwrap();

        let rows = fetch_task_list(
            &conn,
            &TaskListFilter {
                tag: None,
                status: None,
                lineage_filter: LineageFilter::RetryOnly,
                limit: 10,
                offset: 0,
            },
        )
        .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].retry_of_task_id, Some(4));
        assert_eq!(rows[0].trigger_type.as_deref(), Some("retry"));
    }

    #[test]
    fn fetch_task_list_can_filter_triggered_lineage() {
        let conn = setup_conn();
        conn.execute(
            "INSERT INTO tasks(tag, command, shell, work_dir, started_at, pid, parent_task_id, trigger_type, status) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                "demo",
                "cargo run",
                "pwsh",
                ".",
                "2026-03-21T12:02:00+08:00",
                1003,
                9,
                "dependency",
                "success"
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO tasks(tag, command, shell, work_dir, started_at, pid, trigger_type, status) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                "demo",
                "cargo build",
                "pwsh",
                ".",
                "2026-03-21T12:03:00+08:00",
                1004,
                "manual",
                "success"
            ],
        )
        .unwrap();

        let rows = fetch_task_list(
            &conn,
            &TaskListFilter {
                tag: None,
                status: None,
                lineage_filter: LineageFilter::Triggered,
                limit: 10,
                offset: 0,
            },
        )
        .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].parent_task_id, Some(9));
        assert_eq!(rows[0].trigger_type.as_deref(), Some("dependency"));
    }

    #[test]
    fn fetch_task_list_with_range_applies_time_window() {
        let conn = setup_conn();
        conn.execute(
            "INSERT INTO tasks(tag, command, shell, work_dir, started_at, pid, trigger_type, status) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                "demo",
                "cargo test",
                "pwsh",
                ".",
                "2026-03-21T11:59:59+08:00",
                1001,
                "manual",
                "success"
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO tasks(tag, command, shell, work_dir, started_at, pid, trigger_type, status) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                "demo",
                "cargo build",
                "pwsh",
                ".",
                "2026-03-21T12:00:30+08:00",
                1002,
                "manual",
                "success"
            ],
        )
        .unwrap();

        let rows = fetch_task_list_with_range(
            &conn,
            Some("demo"),
            None,
            &NormalizedTimeRange {
                from: Some("2026-03-21T12:00:00+08:00".into()),
                to: Some("2026-03-21T12:01:00+08:00".into()),
            },
            LineageFilter::All,
            10,
            0,
        )
        .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].command, "cargo build");
    }
}
