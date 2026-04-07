use crate::cli::ExportFormat;
use crate::formatter::QueryLogRow;
use crate::utils::{format_duration, format_rfc3339_millis, json_escape};

#[derive(Debug, Clone)]
pub struct TaskExportInfo {
    pub id: i64,
    pub tag: Option<String>,
    pub command: String,
    pub command_json: Option<String>,
    pub shell: Option<String>,
    pub work_dir: String,
    pub started_at: String,
    pub ended_at: Option<String>,
    pub duration_ms: Option<i64>,
    pub pid: Option<i64>,
    pub parent_task_id: Option<i64>,
    pub retry_of_task_id: Option<i64>,
    pub trigger_type: Option<String>,
    pub exit_code: Option<i32>,
    pub status: String,
    pub env_vars: Option<String>,
}

pub fn render_export(
    format: ExportFormat,
    rows: &[QueryLogRow],
    task: Option<&TaskExportInfo>,
) -> String {
    match format {
        ExportFormat::Txt => render_txt(rows, task),
        ExportFormat::Json => render_json(rows, task),
        ExportFormat::Csv => render_csv(rows),
        ExportFormat::Html => render_html(rows, task),
    }
}

fn render_txt(rows: &[QueryLogRow], task: Option<&TaskExportInfo>) -> String {
    let mut out = String::new();
    out.push_str("logex export\n");
    out.push_str(&format!("log_count={}\n", rows.len()));

    if let Some(task) = task {
        out.push_str(&format!("task_id={}\n", task.id));
        out.push_str(&format!(
            "task_tag={}\n",
            task.tag.as_deref().unwrap_or("-")
        ));
        out.push_str(&format!("task_status={}\n", task.status));
        out.push_str(&format!("task_command={}\n", task.command));
        if let Some(ref command_json) = task.command_json {
            out.push_str(&format!("task_command_json={}\n", command_json));
        }
        out.push_str(&format!(
            "task_shell={}\n",
            task.shell.as_deref().unwrap_or("-")
        ));
        out.push_str(&format!("task_work_dir={}\n", task.work_dir));
        out.push_str(&format!("task_started_at={}\n", task.started_at));
        out.push_str(&format!(
            "task_ended_at={}\n",
            task.ended_at.as_deref().unwrap_or("-")
        ));
        out.push_str(&format!(
            "task_duration={}\n",
            format_duration(task.duration_ms)
        ));
        out.push_str(&format!(
            "task_pid={}\n",
            task.pid
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string())
        ));
        out.push_str(&format!(
            "task_parent_task_id={}\n",
            task.parent_task_id
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string())
        ));
        out.push_str(&format!(
            "task_retry_of_task_id={}\n",
            task.retry_of_task_id
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string())
        ));
        out.push_str(&format!(
            "task_trigger_type={}\n",
            task.trigger_type.as_deref().unwrap_or("-")
        ));
        out.push_str(&format!(
            "task_exit_code={}\n",
            task.exit_code
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string())
        ));
        if let Some(ref env) = task.env_vars {
            out.push_str(&format!("task_env_vars={}\n", env));
        }
    }

    out.push('\n');
    for row in rows {
        out.push_str(&format!(
            "id={} task_id={} tag={} ts={} stream={} level={} status={} message={}\n",
            row.id,
            row.task_id,
            row.tag.as_deref().unwrap_or("-"),
            row.ts,
            row.stream,
            row.level,
            row.status,
            row.message
        ));
    }

    out
}

fn render_json(rows: &[QueryLogRow], task: Option<&TaskExportInfo>) -> String {
    let mut out = String::new();
    out.push('{');
    out.push_str(&format!("\"log_count\":{},", rows.len()));

    match task {
        Some(task) => {
            out.push_str("\"task\":{");
            out.push_str(&format!("\"id\":{},", task.id));
            out.push_str(&format!(
                "\"tag\":{},",
                json_opt_string(task.tag.as_deref())
            ));
            out.push_str(&format!("\"command\":\"{}\",", json_escape(&task.command)));
            out.push_str(&format!(
                "\"command_json\":{},",
                json_opt_string(task.command_json.as_deref())
            ));
            out.push_str(&format!(
                "\"shell\":{},",
                json_opt_string(task.shell.as_deref())
            ));
            out.push_str(&format!(
                "\"work_dir\":\"{}\",",
                json_escape(&task.work_dir)
            ));
            out.push_str(&format!(
                "\"started_at\":\"{}\",",
                json_escape(&task.started_at)
            ));
            out.push_str(&format!(
                "\"ended_at\":{},",
                json_opt_string(task.ended_at.as_deref())
            ));
            out.push_str(&format!(
                "\"duration_ms\":{},",
                task.duration_ms
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "null".to_string())
            ));
            out.push_str(&format!(
                "\"pid\":{},",
                task.pid
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "null".to_string())
            ));
            out.push_str(&format!(
                "\"parent_task_id\":{},",
                task.parent_task_id
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "null".to_string())
            ));
            out.push_str(&format!(
                "\"retry_of_task_id\":{},",
                task.retry_of_task_id
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "null".to_string())
            ));
            out.push_str(&format!(
                "\"trigger_type\":{},",
                json_opt_string(task.trigger_type.as_deref())
            ));
            out.push_str(&format!(
                "\"exit_code\":{},",
                task.exit_code
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "null".to_string())
            ));
            out.push_str(&format!("\"status\":\"{}\",", json_escape(&task.status)));
            out.push_str(&format!(
                "\"env_vars\":{}",
                json_opt_string(task.env_vars.as_deref())
            ));
            out.push_str("},");
        }
        None => out.push_str("\"task\":null,"),
    }

    out.push_str("\"logs\":[");
    for (index, row) in rows.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }

        out.push_str(&format!(
            "{{\"id\":{},\"task_id\":{},\"tag\":{},\"ts\":\"{}\",\"stream\":\"{}\",\"level\":\"{}\",\"status\":\"{}\",\"message\":\"{}\"}}",
            row.id,
            row.task_id,
            json_opt_string(row.tag.as_deref()),
            json_escape(&row.ts),
            json_escape(&row.stream),
            json_escape(&row.level),
            json_escape(&row.status),
            json_escape(&row.message)
        ));
    }
    out.push_str("]}");
    out
}

fn render_csv(rows: &[QueryLogRow]) -> String {
    let mut out = String::from("id,task_id,tag,ts,stream,level,status,message\n");
    for row in rows {
        out.push_str(&format!(
            "{},{},{},{},{},{},{},{}\n",
            row.id,
            row.task_id,
            csv_escape(row.tag.as_deref().unwrap_or("")),
            csv_escape(&row.ts),
            csv_escape(&row.stream),
            csv_escape(&row.level),
            csv_escape(&row.status),
            csv_escape(&row.message)
        ));
    }
    out
}

fn render_html(rows: &[QueryLogRow], task: Option<&TaskExportInfo>) -> String {
    let report = build_log_report(rows);
    let mut out = String::from(
        "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"utf-8\"><title>logex export</title>\
<style>\
body{font-family:\"Segoe UI\",sans-serif;margin:24px;background:#f5f7fb;color:#18212b;}\
h1,h2{margin:0 0 12px;}\
.meta,.logs{border-collapse:collapse;width:100%;background:#fff;box-shadow:0 8px 24px rgba(0,0,0,.06);margin-bottom:24px;}\
th,td{border:1px solid #d9e2ec;padding:10px 12px;text-align:left;vertical-align:top;}\
th{background:#eef3f8;}\
.summary{margin:0 0 16px;color:#52606d;}\
.level-error{color:#b42318;font-weight:600;}\
.level-warn{color:#b54708;font-weight:600;}\
.level-info{color:#027a48;font-weight:600;}\
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;margin:0 0 24px;}\
.card{background:#fff;border:1px solid #d9e2ec;box-shadow:0 8px 24px rgba(0,0,0,.06);padding:14px 16px;}\
.card-label{font-size:12px;color:#52606d;text-transform:uppercase;letter-spacing:.04em;}\
.card-value{font-size:26px;font-weight:700;margin-top:4px;}\
.section-list{background:#fff;border:1px solid #d9e2ec;box-shadow:0 8px 24px rgba(0,0,0,.06);padding:14px 18px;margin:0 0 24px;}\
.section-list ul{margin:8px 0 0;padding-left:18px;}\
.section-list li{margin:6px 0;}\
</style></head><body>",
    );

    out.push_str("<h1>logex export</h1>");
    out.push_str(&format!(
        "<p class=\"summary\">log_count={}</p>",
        rows.len()
    ));

    out.push_str("<h2>Log Summary</h2><div class=\"cards\">");
    append_summary_card(&mut out, "Total Logs", &report.total.to_string());
    append_summary_card(&mut out, "Errors", &report.error.to_string());
    append_summary_card(&mut out, "Warnings", &report.warn.to_string());
    append_summary_card(&mut out, "Stdout", &report.stdout.to_string());
    append_summary_card(&mut out, "Stderr", &report.stderr.to_string());
    append_summary_card(
        &mut out,
        "Window",
        &format!(
            "{} -> {}",
            report.first_ts.as_deref().unwrap_or("-"),
            report.last_ts.as_deref().unwrap_or("-")
        ),
    );
    out.push_str("</div>");

    if let Some(task) = task {
        out.push_str("<h2>Task</h2><table class=\"meta\">");
        append_html_meta_row(&mut out, "ID", &task.id.to_string());
        append_html_meta_row(&mut out, "Tag", task.tag.as_deref().unwrap_or("-"));
        append_html_meta_row(&mut out, "Status", &task.status);
        append_html_meta_row(&mut out, "Command", &task.command);
        append_html_meta_row(
            &mut out,
            "Command JSON",
            task.command_json.as_deref().unwrap_or("-"),
        );
        append_html_meta_row(&mut out, "Shell", task.shell.as_deref().unwrap_or("-"));
        append_html_meta_row(&mut out, "Work Dir", &task.work_dir);
        append_html_meta_row(
            &mut out,
            "Started At",
            &format_rfc3339_millis(&task.started_at),
        );
        append_html_meta_row(
            &mut out,
            "Ended At",
            &task
                .ended_at
                .as_deref()
                .map(format_rfc3339_millis)
                .unwrap_or_else(|| "-".to_string()),
        );
        append_html_meta_row(&mut out, "Duration", &format_duration(task.duration_ms));
        append_html_meta_row(
            &mut out,
            "PID",
            &task
                .pid
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
        );
        append_html_meta_row(
            &mut out,
            "Parent Task",
            &task
                .parent_task_id
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
        );
        append_html_meta_row(
            &mut out,
            "Retry Of",
            &task
                .retry_of_task_id
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
        );
        append_html_meta_row(
            &mut out,
            "Trigger Type",
            task.trigger_type.as_deref().unwrap_or("-"),
        );
        append_html_meta_row(
            &mut out,
            "Exit Code",
            &task
                .exit_code
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
        );
        append_html_meta_row(
            &mut out,
            "Env Vars",
            task.env_vars.as_deref().unwrap_or("-"),
        );
        out.push_str("</table>");

        out.push_str("<h2>Lineage</h2><div class=\"section-list\"><ul>");
        out.push_str(&format!(
            "<li>Trigger type: <strong>{}</strong></li>",
            escape_html(task.trigger_type.as_deref().unwrap_or("-"))
        ));
        out.push_str(&format!(
            "<li>Parent task: <strong>{}</strong></li>",
            task.parent_task_id
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string())
        ));
        out.push_str(&format!(
            "<li>Retry of task: <strong>{}</strong></li>",
            task.retry_of_task_id
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string())
        ));
        out.push_str("</ul></div>");
    }

    if !report.highlights.is_empty() {
        out.push_str("<h2>Key Findings</h2><div class=\"section-list\"><ul>");
        for item in &report.highlights {
            out.push_str(&format!("<li>{}</li>", escape_html(item)));
        }
        out.push_str("</ul></div>");
    }

    out.push_str("<h2>Logs</h2><table class=\"logs\"><thead><tr>\
<th>ID</th><th>Task</th><th>Tag</th><th>Time</th><th>Stream</th><th>Level</th><th>Status</th><th>Message</th>\
</tr></thead><tbody>");

    for row in rows {
        let level_class = match row.level.as_str() {
            "error" => "level-error",
            "warn" => "level-warn",
            "info" => "level-info",
            _ => "",
        };

        out.push_str("<tr>");
        out.push_str(&format!("<td>{}</td>", row.id));
        out.push_str(&format!("<td>{}</td>", row.task_id));
        out.push_str(&format!(
            "<td>{}</td>",
            escape_html(row.tag.as_deref().unwrap_or("-"))
        ));
        out.push_str(&format!(
            "<td>{}</td>",
            escape_html(&format_rfc3339_millis(&row.ts))
        ));
        out.push_str(&format!("<td>{}</td>", escape_html(&row.stream)));
        out.push_str(&format!(
            "<td class=\"{}\">{}</td>",
            level_class,
            escape_html(&row.level)
        ));
        out.push_str(&format!("<td>{}</td>", escape_html(&row.status)));
        out.push_str(&format!("<td>{}</td>", escape_html(&row.message)));
        out.push_str("</tr>");
    }

    out.push_str("</tbody></table></body></html>");
    out
}

#[derive(Debug, Default)]
struct LogReport {
    total: usize,
    error: usize,
    warn: usize,
    stdout: usize,
    stderr: usize,
    first_ts: Option<String>,
    last_ts: Option<String>,
    highlights: Vec<String>,
}

fn build_log_report(rows: &[QueryLogRow]) -> LogReport {
    let mut report = LogReport {
        total: rows.len(),
        ..LogReport::default()
    };

    for row in rows {
        match row.level.as_str() {
            "error" => {
                report.error += 1;
                if report.highlights.len() < 5 {
                    report.highlights.push(format!(
                        "[error] {} {}",
                        format_rfc3339_millis(&row.ts),
                        row.message
                    ));
                }
            }
            "warn" => {
                report.warn += 1;
                if report.highlights.len() < 5 {
                    report.highlights.push(format!(
                        "[warn] {} {}",
                        format_rfc3339_millis(&row.ts),
                        row.message
                    ));
                }
            }
            _ => {}
        }

        match row.stream.as_str() {
            "stdout" => report.stdout += 1,
            "stderr" => report.stderr += 1,
            _ => {}
        }

        let ts = format_rfc3339_millis(&row.ts);
        if report.first_ts.is_none() {
            report.first_ts = Some(ts.clone());
        }
        report.last_ts = Some(ts);
    }

    if report.highlights.is_empty() && !rows.is_empty() {
        report.highlights.push("No error/warn entries found in this export.".to_string());
    }

    report
}

fn append_html_meta_row(out: &mut String, key: &str, value: &str) {
    out.push_str(&format!(
        "<tr><th>{}</th><td>{}</td></tr>",
        escape_html(key),
        escape_html(value)
    ));
}

fn append_summary_card(out: &mut String, label: &str, value: &str) {
    out.push_str(&format!(
        "<div class=\"card\"><div class=\"card-label\">{}</div><div class=\"card-value\">{}</div></div>",
        escape_html(label),
        escape_html(value)
    ));
}

fn csv_escape(value: &str) -> String {
    if value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn escape_html(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(ch),
        }
    }
    out
}

fn json_opt_string(value: Option<&str>) -> String {
    match value {
        Some(value) => format!("\"{}\"", json_escape(value)),
        None => "null".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_row() -> QueryLogRow {
        QueryLogRow {
            id: 1,
            task_id: 7,
            tag: Some("demo".into()),
            ts: "2026-03-21T12:00:00+08:00".into(),
            stream: "stderr".into(),
            level: "error".into(),
            message: "hello, \"world\"".into(),
            status: "failed".into(),
        }
    }

    #[test]
    fn csv_export_escapes_quotes_and_commas() {
        let rendered = render_export(ExportFormat::Csv, &[sample_row()], None);
        assert!(rendered.contains("\"hello, \"\"world\"\"\""));
    }

    #[test]
    fn json_export_includes_task_metadata() {
        let task = TaskExportInfo {
            id: 7,
            tag: Some("demo".into()),
            command: "cargo test".into(),
            command_json: Some("[\"cargo\",\"test\"]".into()),
            shell: Some("bash".into()),
            work_dir: "C:/tmp".into(),
            started_at: "2026-03-21T12:00:00+08:00".into(),
            ended_at: Some("2026-03-21T12:01:00+08:00".into()),
            duration_ms: Some(60_000),
            pid: Some(1234),
            parent_task_id: Some(3),
            retry_of_task_id: Some(5),
            trigger_type: Some("retry".into()),
            exit_code: Some(1),
            status: "failed".into(),
            env_vars: None,
        };

        let rendered = render_export(ExportFormat::Json, &[sample_row()], Some(&task));
        assert!(rendered.contains("\"task\":{"));
        assert!(rendered.contains("\"command\":\"cargo test\""));
        assert!(rendered.contains("\"shell\":\"bash\""));
        assert!(rendered.contains("\"pid\":1234"));
        assert!(rendered.contains("\"parent_task_id\":3"));
        assert!(rendered.contains("\"retry_of_task_id\":5"));
        assert!(rendered.contains("\"trigger_type\":\"retry\""));
        assert!(rendered.contains("\"logs\":["));
    }

    #[test]
    fn html_export_includes_task_env_vars() {
        let task = TaskExportInfo {
            id: 7,
            tag: Some("demo".into()),
            command: "cargo test".into(),
            command_json: Some("[\"cargo\",\"test\"]".into()),
            shell: Some("bash".into()),
            work_dir: "C:/tmp".into(),
            started_at: "2026-03-21T12:00:00+08:00".into(),
            ended_at: Some("2026-03-21T12:01:00+08:00".into()),
            duration_ms: Some(60_000),
            pid: Some(1234),
            parent_task_id: Some(3),
            retry_of_task_id: Some(5),
            trigger_type: Some("retry".into()),
            exit_code: Some(1),
            status: "failed".into(),
            env_vars: Some("FOO=bar\nTOKEN=<redacted>".into()),
        };

        let rendered = render_export(ExportFormat::Html, &[sample_row()], Some(&task));

        assert!(rendered.contains("<th>Env Vars</th>"));
        assert!(rendered.contains("<th>Shell</th><td>bash</td>"));
        assert!(rendered.contains("<th>PID</th><td>1234</td>"));
        assert!(rendered.contains("<th>Parent Task</th><td>3</td>"));
        assert!(rendered.contains("<th>Retry Of</th><td>5</td>"));
        assert!(rendered.contains("<th>Trigger Type</th><td>retry</td>"));
        assert!(rendered.contains("<h2>Log Summary</h2>"));
        assert!(rendered.contains("<h2>Lineage</h2>"));
        assert!(rendered.contains("<h2>Key Findings</h2>"));
        assert!(rendered.contains("FOO=bar"));
        assert!(rendered.contains("TOKEN=&lt;redacted&gt;"));
    }

    #[test]
    fn html_export_surfaces_error_and_warn_highlights() {
        let task = TaskExportInfo {
            id: 7,
            tag: Some("demo".into()),
            command: "cargo test".into(),
            command_json: Some("[\"cargo\",\"test\"]".into()),
            shell: Some("bash".into()),
            work_dir: "C:/tmp".into(),
            started_at: "2026-03-21T12:00:00+08:00".into(),
            ended_at: Some("2026-03-21T12:01:00+08:00".into()),
            duration_ms: Some(60_000),
            pid: Some(1234),
            parent_task_id: Some(3),
            retry_of_task_id: Some(5),
            trigger_type: Some("retry".into()),
            exit_code: Some(1),
            status: "failed".into(),
            env_vars: None,
        };

        let rows = vec![
            QueryLogRow {
                id: 1,
                task_id: 7,
                tag: Some("demo".into()),
                ts: "2026-03-21T12:00:00+08:00".into(),
                stream: "stdout".into(),
                level: "warn".into(),
                message: "retrying after transient failure".into(),
                status: "failed".into(),
            },
            QueryLogRow {
                id: 2,
                task_id: 7,
                tag: Some("demo".into()),
                ts: "2026-03-21T12:00:03+08:00".into(),
                stream: "stderr".into(),
                level: "error".into(),
                message: "database locked during write".into(),
                status: "failed".into(),
            },
        ];

        let rendered = render_export(ExportFormat::Html, &rows, Some(&task));

        assert!(rendered.contains("retrying after transient failure"));
        assert!(rendered.contains("database locked during write"));
        assert!(rendered.contains("Total Logs"));
        assert!(rendered.contains("Warnings"));
        assert!(rendered.contains("Errors"));
    }
}
