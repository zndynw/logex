use crate::cli::{QueryArgs, QuerySearchField};
use crate::utils::*;
use comfy_table::{
    Attribute, Cell, Color, ContentArrangement, Table, modifiers::UTF8_ROUND_CORNERS,
    presets::UTF8_FULL_CONDENSED,
};

#[derive(Debug, Clone)]
pub struct QueryLogRow {
    pub id: i64,
    pub task_id: i64,
    pub tag: Option<String>,
    pub ts: String,
    pub stream: String,
    pub level: String,
    pub message: String,
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct ListTaskRow {
    pub id: i64,
    pub tag: Option<String>,
    pub status: String,
    pub work_dir: String,
    pub started_at: String,
    pub ended_at: Option<String>,
    pub duration_ms: Option<i64>,
    pub command: String,
    pub env_vars: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TagRow {
    pub tag: String,
    pub task_count: i64,
    pub last_started_at: String,
    pub last_task_id: i64,
}

const ANSI_HIGHLIGHT_START: &str = "\x1b[30;43m";
const ANSI_HIGHLIGHT_END: &str = "\x1b[0m";

#[derive(Debug, Clone)]
pub struct QueryHighlighter {
    patterns: Vec<String>,
    fields: Vec<QuerySearchField>,
    case_sensitive: bool,
}

impl QueryHighlighter {
    pub fn from_query_args(args: &QueryArgs) -> Option<Self> {
        if args.grep.is_empty() {
            return None;
        }

        let fields = if args.grep_fields.is_empty() {
            vec![
                QuerySearchField::Message,
                QuerySearchField::Level,
                QuerySearchField::Stream,
                QuerySearchField::Status,
                QuerySearchField::Tag,
                QuerySearchField::TaskId,
                QuerySearchField::Timestamp,
            ]
        } else {
            args.grep_fields.clone()
        };

        Some(Self {
            patterns: args.grep.clone(),
            fields,
            case_sensitive: args.case_sensitive,
        })
    }

    fn applies_to(&self, field: QuerySearchField) -> bool {
        self.fields.contains(&field)
    }

    pub fn highlight(&self, field: QuerySearchField, value: &str) -> String {
        if !self.applies_to(field) {
            return value.to_string();
        }

        let mut ranges = Vec::new();
        for pattern in &self.patterns {
            if pattern.is_empty() {
                continue;
            }
            ranges.extend(find_match_ranges(value, pattern, self.case_sensitive));
        }

        if ranges.is_empty() {
            return value.to_string();
        }

        ranges.sort_by_key(|(start, end)| (*start, *end));
        let mut merged = Vec::new();
        for (start, end) in ranges {
            if let Some((_, last_end)) = merged.last_mut() {
                if start <= *last_end {
                    *last_end = (*last_end).max(end);
                    continue;
                }
            }
            merged.push((start, end));
        }

        let mut rendered = String::with_capacity(
            value.len() + merged.len() * (ANSI_HIGHLIGHT_START.len() + ANSI_HIGHLIGHT_END.len()),
        );
        let mut cursor = 0;
        for (start, end) in merged {
            rendered.push_str(&value[cursor..start]);
            rendered.push_str(ANSI_HIGHLIGHT_START);
            rendered.push_str(&value[start..end]);
            rendered.push_str(ANSI_HIGHLIGHT_END);
            cursor = end;
        }
        rendered.push_str(&value[cursor..]);
        rendered
    }
}

pub fn print_detail_row(
    row: &QueryLogRow,
    is_context: bool,
    plain: bool,
    highlighter: Option<&QueryHighlighter>,
) {
    let prefix = if is_context { "[ctx]" } else { "" };
    let tag = highlight_field(
        highlighter,
        QuerySearchField::Tag,
        row.tag.as_deref().unwrap_or("-"),
    );
    let ts = highlight_field(highlighter, QuerySearchField::Timestamp, &row.ts);
    let stream = highlight_field(highlighter, QuerySearchField::Stream, &row.stream);
    let level = highlight_field(highlighter, QuerySearchField::Level, &row.level);
    let status = highlight_field(highlighter, QuerySearchField::Status, &row.status);
    let message = highlight_field(highlighter, QuerySearchField::Message, &row.message);
    let task_id = highlight_field(
        highlighter,
        QuerySearchField::TaskId,
        &row.task_id.to_string(),
    );

    if plain {
        println!(
            "{} id={} task_id={} tag={} ts={} stream={} level={} status={} message={}",
            prefix, row.id, task_id, tag, ts, stream, level, status, message
        );
    } else {
        let formatted_ts = highlight_field(
            highlighter,
            QuerySearchField::Timestamp,
            &format_rfc3339_millis(&row.ts),
        );
        println!(
            "{} {} [{}] {} {}",
            prefix, formatted_ts, level, tag, message
        );
    }
}

pub fn print_detail_row_json(row: &QueryLogRow, is_context: bool) {
    println!(
        "{{\"id\":{},\"task_id\":{},\"tag\":{},\"ts\":\"{}\",\"stream\":\"{}\",\"level\":\"{}\",\"status\":\"{}\",\"message\":\"{}\",\"is_context\":{}}}",
        row.id,
        row.task_id,
        json_opt_string(row.tag.as_deref()),
        json_escape(&row.ts),
        json_escape(&row.stream),
        json_escape(&row.level),
        json_escape(&row.status),
        json_escape(&row.message),
        is_context
    );
}

pub fn print_detail_rows_table<'a>(
    rows: impl Iterator<Item = (&'a QueryLogRow, bool)>,
    highlighter: Option<&QueryHighlighter>,
) {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL_CONDENSED)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![
            Cell::new("ID").add_attribute(Attribute::Bold),
            Cell::new("Task").add_attribute(Attribute::Bold),
            Cell::new("Tag").add_attribute(Attribute::Bold),
            Cell::new("Time").add_attribute(Attribute::Bold),
            Cell::new("Level").add_attribute(Attribute::Bold),
            Cell::new("Status").add_attribute(Attribute::Bold),
            Cell::new("Message").add_attribute(Attribute::Bold),
        ]);

    for (row, is_context) in rows {
        let level_text = highlight_field(highlighter, QuerySearchField::Level, &row.level);
        let level_cell = if level_text != row.level {
            Cell::new(level_text)
        } else {
            match row.level.as_str() {
                "error" => Cell::new(&row.level).fg(Color::Red),
                "warn" => Cell::new(&row.level).fg(Color::Yellow),
                "info" => Cell::new(&row.level).fg(Color::Green),
                _ => Cell::new(&row.level),
            }
        };

        let status_text = highlight_field(highlighter, QuerySearchField::Status, &row.status);
        let status_cell = if status_text != row.status {
            Cell::new(status_text)
        } else {
            match row.status.as_str() {
                "success" => Cell::new(&row.status).fg(Color::Green),
                "failed" => Cell::new(&row.status).fg(Color::Red),
                "running" => Cell::new(&row.status).fg(Color::Yellow),
                _ => Cell::new(&row.status),
            }
        };

        let mut id_cell = Cell::new(row.id).fg(Color::Cyan);
        if is_context {
            id_cell = id_cell.fg(Color::DarkGrey);
        }

        table.add_row(vec![
            id_cell,
            Cell::new(highlight_field(
                highlighter,
                QuerySearchField::TaskId,
                &row.task_id.to_string(),
            ))
            .fg(Color::Cyan),
            Cell::new(highlight_field(
                highlighter,
                QuerySearchField::Tag,
                row.tag.as_deref().unwrap_or("-"),
            )),
            Cell::new(highlight_field(
                highlighter,
                QuerySearchField::Timestamp,
                &format_rfc3339_millis(&row.ts),
            )),
            level_cell,
            status_cell,
            Cell::new(highlight_field(
                highlighter,
                QuerySearchField::Message,
                &row.message,
            )),
        ]);
    }

    println!("{table}");
}

pub fn print_detail_rows_follow_table<'a>(
    rows: impl Iterator<Item = (&'a QueryLogRow, bool)>,
    header_printed: &mut bool,
    highlighter: Option<&QueryHighlighter>,
) {
    if !*header_printed {
        println!(
            "{:<8} {:<8} {:<12} {:<24} {:<8} {:<10} {}",
            "ID", "Task", "Tag", "Time", "Level", "Status", "Message"
        );
        *header_printed = true;
    }

    for (row, _is_context) in rows {
        let task_id = highlight_field(
            highlighter,
            QuerySearchField::TaskId,
            &row.task_id.to_string(),
        );
        let tag = highlight_field(
            highlighter,
            QuerySearchField::Tag,
            row.tag.as_deref().unwrap_or("-"),
        );
        let ts = highlight_field(
            highlighter,
            QuerySearchField::Timestamp,
            &format_rfc3339_millis(&row.ts),
        );
        let level = highlight_field(highlighter, QuerySearchField::Level, &row.level);
        let status = highlight_field(highlighter, QuerySearchField::Status, &row.status);
        let message = highlight_field(highlighter, QuerySearchField::Message, &row.message);
        println!(
            "{:<8} {:<8} {:<12} {:<24} {:<8} {:<10} {}",
            row.id, task_id, tag, ts, level, status, message
        );
    }
}

pub fn print_list_rows_table(rows: &[ListTaskRow]) {
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
            Cell::new("Env").add_attribute(Attribute::Bold),
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
            Cell::new(row.env_vars.as_deref().unwrap_or("-")),
        ]);
    }

    println!("{table}");
}

pub fn print_tags_rows_table(rows: &[TagRow]) {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL_CONDENSED)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![
            Cell::new("Tag").add_attribute(Attribute::Bold),
            Cell::new("Tasks").add_attribute(Attribute::Bold),
            Cell::new("Last ID").add_attribute(Attribute::Bold),
            Cell::new("Last Started At").add_attribute(Attribute::Bold),
        ]);

    for row in rows {
        table.add_row(vec![
            Cell::new(&row.tag),
            Cell::new(row.task_count).fg(Color::Cyan),
            Cell::new(row.last_task_id).fg(Color::Cyan),
            Cell::new(format_rfc3339_millis(&row.last_started_at)),
        ]);
    }

    println!("{table}");
}

fn highlight_field(
    highlighter: Option<&QueryHighlighter>,
    field: QuerySearchField,
    value: &str,
) -> String {
    highlighter
        .map(|highlighter| highlighter.highlight(field, value))
        .unwrap_or_else(|| value.to_string())
}

fn find_match_ranges(haystack: &str, pattern: &str, case_sensitive: bool) -> Vec<(usize, usize)> {
    if pattern.is_empty() {
        return Vec::new();
    }

    let pattern_chars: Vec<char> = if case_sensitive {
        pattern.chars().collect()
    } else {
        pattern.chars().flat_map(|ch| ch.to_lowercase()).collect()
    };

    if pattern_chars.is_empty() {
        return Vec::new();
    }

    let normalized = normalize_for_matching(haystack, case_sensitive);
    let mut matches = Vec::new();
    let mut start = 0;

    while start + pattern_chars.len() <= normalized.chars.len() {
        if normalized.chars[start..start + pattern_chars.len()] == pattern_chars[..] {
            matches.push((
                normalized.starts[start],
                normalized.ends[start + pattern_chars.len() - 1],
            ));
        }
        start += 1;
    }

    matches
}

struct NormalizedText {
    chars: Vec<char>,
    starts: Vec<usize>,
    ends: Vec<usize>,
}

fn normalize_for_matching(value: &str, case_sensitive: bool) -> NormalizedText {
    let mut chars = Vec::new();
    let mut starts = Vec::new();
    let mut ends = Vec::new();

    for (start, ch) in value.char_indices() {
        let end = start + ch.len_utf8();
        if case_sensitive {
            chars.push(ch);
            starts.push(start);
            ends.push(end);
        } else {
            for lower in ch.to_lowercase() {
                chars.push(lower);
                starts.push(start);
                ends.push(end);
            }
        }
    }

    NormalizedText {
        chars,
        starts,
        ends,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn highlighter_marks_case_insensitive_matches() {
        let args = QueryArgs {
            task_id: None,
            tag: None,
            from: None,
            to: None,
            level: None,
            status: None,
            view: crate::cli::QueryView::Detail,
            output: crate::cli::QueryOutput::Table,
            grep: vec!["timeout".into()],
            grep_mode: crate::cli::QueryMatchMode::Any,
            grep_fields: vec![QuerySearchField::Message],
            case_sensitive: false,
            invert_match: false,
            no_highlight: false,
            after_context: 0,
            before_context: 0,
            context: None,
            follow: false,
            tail: 10,
            poll_ms: 500,
        };

        let highlighter = QueryHighlighter::from_query_args(&args).unwrap();
        let rendered =
            highlighter.highlight(QuerySearchField::Message, "Connection TIMEOUT happened");
        assert!(rendered.contains(ANSI_HIGHLIGHT_START));
        assert!(rendered.contains("TIMEOUT"));
    }

    #[test]
    fn highlighter_respects_field_selection() {
        let args = QueryArgs {
            task_id: None,
            tag: None,
            from: None,
            to: None,
            level: None,
            status: None,
            view: crate::cli::QueryView::Detail,
            output: crate::cli::QueryOutput::Table,
            grep: vec!["deploy".into()],
            grep_mode: crate::cli::QueryMatchMode::Any,
            grep_fields: vec![QuerySearchField::Tag],
            case_sensitive: false,
            invert_match: false,
            no_highlight: false,
            after_context: 0,
            before_context: 0,
            context: None,
            follow: false,
            tail: 10,
            poll_ms: 500,
        };

        let highlighter = QueryHighlighter::from_query_args(&args).unwrap();
        assert!(
            highlighter
                .highlight(QuerySearchField::Tag, "deploy-prod")
                .contains(ANSI_HIGHLIGHT_START)
        );
        assert_eq!(
            highlighter.highlight(QuerySearchField::Message, "deploy-prod"),
            "deploy-prod"
        );
    }
}
