use crate::Result;
use crate::exporter::TaskExportInfo;
use crate::formatter::{ListTaskRow, QueryLogRow};
use rusqlite::{Connection, OptionalExtension, params};

#[derive(Debug, Clone, Default)]
pub struct TaskListFilter {
    pub tag: Option<String>,
    pub status: Option<String>,
    pub limit: i64,
    pub offset: i64,
}

#[derive(Debug, Clone, Default)]
pub struct DashboardStats {
    pub total: i64,
    pub running: i64,
    pub success: i64,
    pub failed: i64,
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

pub fn fetch_task_list(conn: &Connection, filter: &TaskListFilter) -> Result<Vec<ListTaskRow>> {
    let mut stmt = conn.prepare(
        r#"SELECT id, tag, command, work_dir, started_at, ended_at, duration_ms, status, env_vars
           FROM tasks
           WHERE (?1 IS NULL OR tag = ?1) AND (?2 IS NULL OR status = ?2)
           ORDER BY started_at DESC, id DESC
           LIMIT ?3 OFFSET ?4"#,
    )?;

    let rows = stmt.query_map(
        params![
            filter.tag.as_ref(),
            filter.status.as_ref(),
            filter.limit,
            filter.offset
        ],
        |row| {
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
        },
    )?;

    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
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

pub fn fetch_task_detail(conn: &Connection, task_id: i64) -> Result<Option<TaskExportInfo>> {
    conn.query_row(
        "SELECT id, tag, command, work_dir, started_at, ended_at, duration_ms, exit_code, status, env_vars FROM tasks WHERE id = ?1",
        params![task_id],
        |row| {
            Ok(TaskExportInfo {
                id: row.get(0)?,
                tag: row.get(1)?,
                command: row.get(2)?,
                work_dir: row.get(3)?,
                started_at: row.get(4)?,
                ended_at: row.get(5)?,
                duration_ms: row.get(6)?,
                exit_code: row.get(7)?,
                status: row.get(8)?,
                env_vars: row.get(9)?,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

pub fn fetch_task_logs(
    conn: &Connection,
    task_id: i64,
    after_log_id: i64,
) -> Result<Vec<QueryLogRow>> {
    let mut stmt = conn.prepare(
        r#"SELECT l.id, l.task_id, t.tag, l.ts, l.stream, l.level, l.message, t.status
           FROM task_logs l
           JOIN tasks t ON t.id = l.task_id
           WHERE l.task_id = ?1 AND l.id > ?2
           ORDER BY l.ts ASC, l.id ASC"#,
    )?;

    let rows = stmt.query_map(params![task_id, after_log_id], |row| {
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
