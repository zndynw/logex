use crate::Result;
use crate::utils::{format_duration, format_rfc3339_millis, json_escape};
use crate::store::{
    fetch_duration_analysis_summary, fetch_log_analysis_summary, fetch_task_analysis_summary,
    fetch_top_tag_analysis,
};
use rusqlite::Connection;

#[derive(Debug, Clone)]
pub struct AnalysisFilter {
    pub tag: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
    pub top_tags: usize,
}

#[derive(Debug, Clone, Default)]
pub struct AnalysisReport {
    pub logs: LogAnalysis,
    pub tasks: TaskAnalysis,
    pub durations: DurationAnalysis,
    pub top_tags: Vec<TagAnalysis>,
}

#[derive(Debug, Clone, Default)]
pub struct LogAnalysis {
    pub total: i64,
    pub error: i64,
    pub warn: i64,
    pub info: i64,
    pub unknown: i64,
    pub stdout: i64,
    pub stderr: i64,
    pub other_streams: i64,
    pub first_ts: Option<String>,
    pub last_ts: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct TaskAnalysis {
    pub total: i64,
    pub running: i64,
    pub success: i64,
    pub failed: i64,
}

#[derive(Debug, Clone, Default)]
pub struct DurationAnalysis {
    pub finished_count: i64,
    pub min_ms: Option<i64>,
    pub max_ms: Option<i64>,
    pub avg_ms: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct TagAnalysis {
    pub tag: String,
    pub task_count: i64,
    pub log_count: i64,
    pub error_count: i64,
    pub warn_count: i64,
    pub info_count: i64,
    pub unknown_count: i64,
    pub last_started_at: Option<String>,
}

pub fn collect_analysis(conn: &Connection, filter: &AnalysisFilter) -> Result<AnalysisReport> {
    let mut report = AnalysisReport::default();
    report.logs = collect_log_analysis(conn, filter)?;
    report.tasks = collect_task_analysis(conn, filter)?;
    report.durations = collect_duration_analysis(conn, filter)?;
    report.top_tags = collect_top_tags(conn, filter)?;
    Ok(report)
}

pub fn render_analysis_plain(report: &AnalysisReport) -> String {
    let mut out = String::new();
    out.push_str("analyze_result\n");
    out.push_str(&format!("log_total={}\n", report.logs.total));
    out.push_str(&format!(
        "log_window first_ts={} last_ts={}\n",
        report
            .logs
            .first_ts
            .as_deref()
            .map(format_rfc3339_millis)
            .unwrap_or_else(|| "-".to_string()),
        report
            .logs
            .last_ts
            .as_deref()
            .map(format_rfc3339_millis)
            .unwrap_or_else(|| "-".to_string())
    ));
    out.push_str(&format!(
        "level=error count={} ratio={:.2}%\n",
        report.logs.error,
        calc_ratio(report.logs.error, report.logs.total)
    ));
    out.push_str(&format!(
        "level=warn count={} ratio={:.2}%\n",
        report.logs.warn,
        calc_ratio(report.logs.warn, report.logs.total)
    ));
    out.push_str(&format!(
        "level=info count={} ratio={:.2}%\n",
        report.logs.info,
        calc_ratio(report.logs.info, report.logs.total)
    ));
    out.push_str(&format!(
        "level=unknown count={} ratio={:.2}%\n",
        report.logs.unknown,
        calc_ratio(report.logs.unknown, report.logs.total)
    ));
    out.push_str(&format!(
        "stream=stdout count={} ratio={:.2}%\n",
        report.logs.stdout,
        calc_ratio(report.logs.stdout, report.logs.total)
    ));
    out.push_str(&format!(
        "stream=stderr count={} ratio={:.2}%\n",
        report.logs.stderr,
        calc_ratio(report.logs.stderr, report.logs.total)
    ));
    out.push_str(&format!(
        "stream=other count={} ratio={:.2}%\n",
        report.logs.other_streams,
        calc_ratio(report.logs.other_streams, report.logs.total)
    ));
    out.push_str(&format!(
        "task_total={} task_running={} task_success={} task_failed={} success_rate={:.2}% failure_rate={:.2}%\n",
        report.tasks.total,
        report.tasks.running,
        report.tasks.success,
        report.tasks.failed,
        calc_ratio(report.tasks.success, report.tasks.success + report.tasks.failed),
        calc_ratio(report.tasks.failed, report.tasks.success + report.tasks.failed)
    ));
    out.push_str(&format!(
        "duration finished={} min={} avg={} max={}\n",
        report.durations.finished_count,
        format_duration(report.durations.min_ms),
        report
            .durations
            .avg_ms
            .map(|value| format_duration(Some(value.round() as i64)))
            .unwrap_or_else(|| "-".to_string()),
        format_duration(report.durations.max_ms)
    ));

    if report.top_tags.is_empty() {
        out.push_str("top_tags=none\n");
    } else {
        for tag in &report.top_tags {
            out.push_str(&format!(
                "top_tag tag={} tasks={} logs={} error={} warn={} info={} unknown={} last_started_at={}\n",
                tag.tag,
                tag.task_count,
                tag.log_count,
                tag.error_count,
                tag.warn_count,
                tag.info_count,
                tag.unknown_count,
                tag.last_started_at
                    .as_deref()
                    .map(format_rfc3339_millis)
                    .unwrap_or_else(|| "-".to_string())
            ));
        }
    }

    out
}

pub fn render_analysis_json(report: &AnalysisReport) -> String {
    let mut out = String::from("{");
    out.push_str("\"logs\":{");
    out.push_str(&format!("\"total\":{},", report.logs.total));
    out.push_str(&format!("\"error\":{},", report.logs.error));
    out.push_str(&format!("\"warn\":{},", report.logs.warn));
    out.push_str(&format!("\"info\":{},", report.logs.info));
    out.push_str(&format!("\"unknown\":{},", report.logs.unknown));
    out.push_str(&format!("\"stdout\":{},", report.logs.stdout));
    out.push_str(&format!("\"stderr\":{},", report.logs.stderr));
    out.push_str(&format!("\"other_streams\":{},", report.logs.other_streams));
    out.push_str(&format!(
        "\"first_ts\":{},\"last_ts\":{}",
        json_opt_string(report.logs.first_ts.as_deref()),
        json_opt_string(report.logs.last_ts.as_deref())
    ));
    out.push_str("},");

    out.push_str("\"tasks\":{");
    out.push_str(&format!("\"total\":{},", report.tasks.total));
    out.push_str(&format!("\"running\":{},", report.tasks.running));
    out.push_str(&format!("\"success\":{},", report.tasks.success));
    out.push_str(&format!("\"failed\":{},", report.tasks.failed));
    out.push_str(&format!(
        "\"success_rate\":{:.4},\"failure_rate\":{:.4}",
        calc_ratio(
            report.tasks.success,
            report.tasks.success + report.tasks.failed
        ),
        calc_ratio(
            report.tasks.failed,
            report.tasks.success + report.tasks.failed
        )
    ));
    out.push_str("},");

    out.push_str("\"durations\":{");
    out.push_str(&format!(
        "\"finished_count\":{},",
        report.durations.finished_count
    ));
    out.push_str(&format!(
        "\"min_ms\":{},\"avg_ms\":{},\"max_ms\":{}",
        json_opt_i64(report.durations.min_ms),
        report
            .durations
            .avg_ms
            .map(|value| format!("{value:.4}"))
            .unwrap_or_else(|| "null".to_string()),
        json_opt_i64(report.durations.max_ms)
    ));
    out.push_str("},");

    out.push_str("\"top_tags\":[");
    for (index, tag) in report.top_tags.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push_str(&format!(
            "{{\"tag\":\"{}\",\"task_count\":{},\"log_count\":{},\"error_count\":{},\"warn_count\":{},\"info_count\":{},\"unknown_count\":{},\"last_started_at\":{}}}",
            json_escape(&tag.tag),
            tag.task_count,
            tag.log_count,
            tag.error_count,
            tag.warn_count,
            tag.info_count,
            tag.unknown_count,
            json_opt_string(tag.last_started_at.as_deref())
        ));
    }
    out.push_str("]}");
    out
}

fn collect_log_analysis(conn: &Connection, filter: &AnalysisFilter) -> Result<LogAnalysis> {
    fetch_log_analysis_summary(
        conn,
        filter.tag.as_deref(),
        &crate::filters::NormalizedTimeRange {
            from: filter.from.clone(),
            to: filter.to.clone(),
        },
    )
}

fn collect_task_analysis(conn: &Connection, filter: &AnalysisFilter) -> Result<TaskAnalysis> {
    fetch_task_analysis_summary(
        conn,
        filter.tag.as_deref(),
        &crate::filters::NormalizedTimeRange {
            from: filter.from.clone(),
            to: filter.to.clone(),
        },
    )
}

fn collect_duration_analysis(
    conn: &Connection,
    filter: &AnalysisFilter,
) -> Result<DurationAnalysis> {
    fetch_duration_analysis_summary(
        conn,
        filter.tag.as_deref(),
        &crate::filters::NormalizedTimeRange {
            from: filter.from.clone(),
            to: filter.to.clone(),
        },
    )
}

fn collect_top_tags(conn: &Connection, filter: &AnalysisFilter) -> Result<Vec<TagAnalysis>> {
    fetch_top_tag_analysis(
        conn,
        filter.tag.as_deref(),
        &crate::filters::NormalizedTimeRange {
            from: filter.from.clone(),
            to: filter.to.clone(),
        },
        filter.top_tags,
    )
}

fn calc_ratio(count: i64, total: i64) -> f64 {
    if total <= 0 {
        0.0
    } else {
        (count as f64) * 100.0 / (total as f64)
    }
}

fn json_opt_string(value: Option<&str>) -> String {
    match value {
        Some(value) => format!("\"{}\"", json_escape(value)),
        None => "null".to_string(),
    }
}

fn json_opt_i64(value: Option<i64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "null".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::params;

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
                status TEXT NOT NULL
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

        conn.execute(
            "INSERT INTO tasks(tag, command, work_dir, started_at, ended_at, duration_ms, exit_code, status) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params!["demo", "cargo test", ".", "2026-03-21T10:00:00+08:00", "2026-03-21T10:01:00+08:00", 60_000, 0, "success"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO tasks(tag, command, work_dir, started_at, ended_at, duration_ms, exit_code, status) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params!["demo", "cargo run", ".", "2026-03-21T11:00:00+08:00", "2026-03-21T11:02:00+08:00", 120_000, 1, "failed"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO tasks(tag, command, work_dir, started_at, ended_at, duration_ms, exit_code, status) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params!["ops", "ping", ".", "2026-03-21T12:00:00+08:00", Option::<String>::None, Option::<i64>::None, Option::<i32>::None, "running"],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO task_logs(task_id, ts, stream, level, message) VALUES(?1, ?2, ?3, ?4, ?5)",
            params![1, "2026-03-21T10:00:01+08:00", "stdout", "info", "start"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO task_logs(task_id, ts, stream, level, message) VALUES(?1, ?2, ?3, ?4, ?5)",
            params![2, "2026-03-21T11:00:01+08:00", "stderr", "error", "boom"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO task_logs(task_id, ts, stream, level, message) VALUES(?1, ?2, ?3, ?4, ?5)",
            params![2, "2026-03-21T11:00:02+08:00", "stdout", "warn", "retry"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO task_logs(task_id, ts, stream, level, message) VALUES(?1, ?2, ?3, ?4, ?5)",
            params![3, "2026-03-21T12:00:01+08:00", "stdout", "info", "running"],
        )
        .unwrap();

        conn
    }

    #[test]
    fn collects_core_analysis_metrics() {
        let conn = setup_conn();
        let report = collect_analysis(
            &conn,
            &AnalysisFilter {
                tag: None,
                from: None,
                to: None,
                top_tags: 5,
            },
        )
        .unwrap();

        assert_eq!(report.logs.total, 4);
        assert_eq!(report.logs.error, 1);
        assert_eq!(report.tasks.total, 3);
        assert_eq!(report.tasks.running, 1);
        assert_eq!(report.tasks.success, 1);
        assert_eq!(report.tasks.failed, 1);
        assert_eq!(report.durations.finished_count, 2);
        assert_eq!(report.top_tags.first().unwrap().tag, "demo");
    }

    #[test]
    fn renders_json_with_expected_sections() {
        let conn = setup_conn();
        let report = collect_analysis(
            &conn,
            &AnalysisFilter {
                tag: Some("demo".into()),
                from: None,
                to: None,
                top_tags: 3,
            },
        )
        .unwrap();

        let rendered = render_analysis_json(&report);
        assert!(rendered.contains("\"logs\""));
        assert!(rendered.contains("\"tasks\""));
        assert!(rendered.contains("\"durations\""));
        assert!(rendered.contains("\"top_tags\""));
    }
}
