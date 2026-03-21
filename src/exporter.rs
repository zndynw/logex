use crate::cli::ExportFormat;
use crate::formatter::QueryLogRow;
use crate::utils::{format_duration, format_rfc3339_millis, json_escape};

#[derive(Debug, Clone)]
pub struct TaskExportInfo {
    pub id: i64,
    pub tag: Option<String>,
    pub command: String,
    pub work_dir: String,
    pub started_at: String,
    pub ended_at: Option<String>,
    pub duration_ms: Option<i64>,
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
</style></head><body>",
    );

    out.push_str("<h1>logex export</h1>");
    out.push_str(&format!(
        "<p class=\"summary\">log_count={}</p>",
        rows.len()
    ));

    if let Some(task) = task {
        out.push_str("<h2>Task</h2><table class=\"meta\">");
        append_html_meta_row(&mut out, "ID", &task.id.to_string());
        append_html_meta_row(&mut out, "Tag", task.tag.as_deref().unwrap_or("-"));
        append_html_meta_row(&mut out, "Status", &task.status);
        append_html_meta_row(&mut out, "Command", &task.command);
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
            "Exit Code",
            &task
                .exit_code
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
        );
        out.push_str("</table>");
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

fn append_html_meta_row(out: &mut String, key: &str, value: &str) {
    out.push_str(&format!(
        "<tr><th>{}</th><td>{}</td></tr>",
        escape_html(key),
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
            work_dir: "C:/tmp".into(),
            started_at: "2026-03-21T12:00:00+08:00".into(),
            ended_at: Some("2026-03-21T12:01:00+08:00".into()),
            duration_ms: Some(60_000),
            exit_code: Some(1),
            status: "failed".into(),
            env_vars: None,
        };

        let rendered = render_export(ExportFormat::Json, &[sample_row()], Some(&task));
        assert!(rendered.contains("\"task\":{"));
        assert!(rendered.contains("\"command\":\"cargo test\""));
        assert!(rendered.contains("\"logs\":["));
    }
}
