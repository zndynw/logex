use super::app::{App, FocusPane, InputMode};
use crate::cli::ExportFormat;
use crate::formatter::{QueryLogRow, task_lineage_label};
use crate::utils::{format_duration, format_rfc3339_millis};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, ListState, Paragraph, Wrap};

pub fn draw(frame: &mut ratatui::Frame<'_>, app: &App) {
    let area = frame.area();
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(10),
            Constraint::Length(1),
        ])
        .split(area);

    draw_header(frame, vertical[0], app);
    draw_body(frame, vertical[1], app);
    draw_footer(frame, vertical[2], app);
    draw_overlay(frame, app);
}

fn draw_header(frame: &mut ratatui::Frame<'_>, area: Rect, app: &App) {
    let lines = vec![
        Line::from(vec![
            Span::styled(
                "logex TUI",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw("  DB: "),
            Span::styled(app.db_label.clone(), Style::default().fg(Color::Gray)),
            Span::raw("  Refresh: "),
            Span::styled(
                format!(
                    "{}ms {}",
                    app.refresh_every.as_millis(),
                    if app.auto_refresh { "▶" } else { "⏸" }
                ),
                Style::default().fg(if app.auto_refresh {
                    Color::Green
                } else {
                    Color::Yellow
                }),
            ),
        ]),
        Line::from(vec![
            Span::raw("Filter: tag="),
            Span::styled(
                app.tag_filter.as_deref().unwrap_or("*"),
                Style::default().fg(Color::Green),
            ),
            Span::raw(" status="),
            Span::styled(
                app.status_filter.as_str(),
                Style::default().fg(Color::Green),
            ),
            Span::raw(" lineage="),
            Span::styled(
                app.lineage_filter.as_str(),
                Style::default().fg(Color::Magenta),
            ),
            Span::raw("  Tasks: "),
            Span::styled(
                format!("{}/{}", app.tasks.len(), app.dashboard.total),
                Style::default().fg(Color::Cyan),
            ),
            Span::raw("  Logs: "),
            Span::styled(
                app.analysis.logs.total.to_string(),
                Style::default().fg(Color::Cyan),
            ),
            Span::raw(" (E:"),
            Span::styled(
                app.analysis.logs.error.to_string(),
                Style::default().fg(Color::Red),
            ),
            Span::raw(" W:"),
            Span::styled(
                app.analysis.logs.warn.to_string(),
                Style::default().fg(Color::Yellow),
            ),
            Span::raw(")"),
        ]),
        Line::from(vec![
            Span::raw("Selected: "),
            Span::styled(
                app.selected_task_id()
                    .map(|id| format!("#{id}"))
                    .unwrap_or_else(|| "-".to_string()),
                Style::default().fg(Color::Magenta),
            ),
            Span::raw("  Focus: "),
            Span::styled(
                match app.focus {
                    FocusPane::Tasks => "Tasks",
                    FocusPane::Logs => "Logs",
                },
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw("  Follow: "),
            Span::styled(
                if app.follow_logs { "ON" } else { "OFF" },
                Style::default().fg(if app.follow_logs {
                    Color::Green
                } else {
                    Color::Gray
                }),
            ),
            Span::raw("  Search: "),
            Span::styled(
                app.search_query.as_deref().unwrap_or("-"),
                Style::default().fg(Color::LightCyan),
            ),
        ]),
    ];

    frame.render_widget(Paragraph::new(lines), area);
}

fn draw_body(frame: &mut ratatui::Frame<'_>, area: Rect, app: &App) {
    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(25), Constraint::Percentage(75)])
        .split(area);

    draw_task_list(frame, columns[0], app);

    let right = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(15), Constraint::Min(8)])
        .split(columns[1]);
    draw_task_detail(frame, right[0], app);
    draw_logs(frame, right[1], app);
}

fn draw_task_list(frame: &mut ratatui::Frame<'_>, area: Rect, app: &App) {
    let title = format!(
        " Tasks [{}] ",
        if app.focus == FocusPane::Tasks {
            "active"
        } else {
            "browse"
        }
    );
    let items = if app.tasks.is_empty() {
        vec![ListItem::new(Line::from("No tasks"))]
    } else {
        app.tasks
            .iter()
            .map(|task| {
                let status = match task.status.as_str() {
                    "running" => Span::styled("running", Style::default().fg(Color::Yellow)),
                    "success" => Span::styled("success", Style::default().fg(Color::Green)),
                    "failed" => Span::styled("failed", Style::default().fg(Color::Red)),
                    _ => Span::raw(task.status.clone()),
                };
                ListItem::new(Line::from(vec![
                    Span::styled(format!("{:>4}", task.id), Style::default().fg(Color::Cyan)),
                    Span::raw(" "),
                    status,
                    Span::raw(" "),
                    Span::raw(task.tag.as_deref().unwrap_or("-").to_string()),
                    Span::raw(" "),
                    Span::styled(
                        task_lineage_label(task).unwrap_or_else(|| "-".to_string()),
                        Style::default().fg(Color::Magenta),
                    ),
                ]))
            })
            .collect()
    };

    let list = List::new(items)
        .block(Block::default().borders(Borders::ALL).title(title))
        .highlight_style(
            Style::default()
                .bg(Color::Blue)
                .fg(Color::Black)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol(">");

    let mut state = ListState::default();
    if !app.tasks.is_empty() {
        state.select(Some(app.selected_task_index));
    }
    frame.render_stateful_widget(list, area, &mut state);
}

fn draw_task_detail(frame: &mut ratatui::Frame<'_>, area: Rect, app: &App) {
    let block = Block::default().borders(Borders::ALL).title(format!(
        " Detail [{}] ",
        if app.focus == FocusPane::Tasks {
            "sync"
        } else {
            "inspect"
        }
    ));

    let lines = if let Some(detail) = &app.detail {
        task_detail_lines(detail, &app.logs)
    } else {
        vec![Line::from("No task selected")]
    };

    frame.render_widget(
        Paragraph::new(lines)
            .block(block)
            .wrap(Wrap { trim: false }),
        area,
    );
}

fn task_detail_lines(
    detail: &crate::exporter::TaskExportInfo,
    logs: &[QueryLogRow],
) -> Vec<Line<'static>> {
    let summary = summarize_task_logs(logs);
    let mut lines = vec![
        Line::from(format!("ID:        {}", detail.id)),
        Line::from(format!(
            "Tag:       {}",
            detail.tag.as_deref().unwrap_or("-")
        )),
        Line::from(format!("Status:    {}", detail.status)),
        Line::from(format!("Shell:     {}", detail.shell.as_deref().unwrap_or("-"))),
        Line::from(format!(
            "PID:       {}",
            detail
                .pid
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string())
        )),
        Line::from(format!(
            "Parent:    {}",
            detail
                .parent_task_id
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string())
        )),
        Line::from(format!(
            "Retry Of:  {}",
            detail
                .retry_of_task_id
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string())
        )),
        Line::from(format!(
            "Trigger:   {}",
            detail.trigger_type.as_deref().unwrap_or("-")
        )),
        Line::from(format!(
            "Logs:      total={} stderr={} warn={} error={}",
            summary.total, summary.stderr, summary.warn, summary.error
        )),
    ];

    if let Some(signal) = summary.latest_signal {
        lines.push(Line::from(format!("Signal:    {}", signal)));
    }

    lines.extend([
        Line::from(format!("Command:   {}", detail.command)),
        Line::from(format!("Work Dir:  {}", detail.work_dir)),
        Line::from(format!(
            "Env:       {}",
            detail.env_vars.as_deref().unwrap_or("-")
        )),
        Line::from(format!(
            "Start:     {}",
            format_rfc3339_millis(&detail.started_at)
        )),
        Line::from(format!(
            "End:       {}",
            detail
                .ended_at
                .as_deref()
                .map(format_rfc3339_millis)
                .unwrap_or_else(|| "-".to_string())
        )),
        Line::from(format!(
            "Duration:  {}",
            format_duration(detail.duration_ms)
        )),
        Line::from(format!(
            "Exit Code: {}",
            detail
                .exit_code
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string())
        )),
    ]);

    lines
}

struct TaskLogSummary {
    total: usize,
    stderr: usize,
    warn: usize,
    error: usize,
    latest_signal: Option<String>,
}

fn summarize_task_logs(logs: &[QueryLogRow]) -> TaskLogSummary {
    let mut summary = TaskLogSummary {
        total: logs.len(),
        stderr: 0,
        warn: 0,
        error: 0,
        latest_signal: None,
    };

    for row in logs {
        if row.stream == "stderr" {
            summary.stderr += 1;
        }
        if row.level == "warn" {
            summary.warn += 1;
        }
        if row.level == "error" {
            summary.error += 1;
        }
    }

    summary.latest_signal = logs
        .iter()
        .rev()
        .find(|row| matches!(row.level.as_str(), "error" | "warn"))
        .map(|row| format!("{} {}", row.level, row.message));

    summary
}

fn draw_logs(frame: &mut ratatui::Frame<'_>, area: Rect, app: &App) {
    let filtered_logs: Vec<&QueryLogRow> = app
        .logs
        .iter()
        .filter(|row| app.matches_search(row))
        .collect();
    let title = format!(
        " Logs [{} | {} shown / {} total] ",
        if app.focus == FocusPane::Logs {
            "active"
        } else {
            "view"
        },
        filtered_logs.len(),
        app.logs.len()
    );

    let lines = if filtered_logs.is_empty() {
        vec![Line::from("No logs for selected task")]
    } else {
        filtered_logs
            .iter()
            .map(|row| {
                let level_style = match row.level.as_str() {
                    "error" => Style::default().fg(Color::Red),
                    "warn" => Style::default().fg(Color::Yellow),
                    "info" => Style::default().fg(Color::Green),
                    _ => Style::default().fg(Color::Gray),
                };

                Line::from(vec![
                    Span::styled(
                        format_rfc3339_millis(&row.ts),
                        Style::default().fg(Color::DarkGray),
                    ),
                    Span::raw(" "),
                    highlight_text(
                        &format!("{:<5}", row.level),
                        app.search_query.as_deref(),
                        level_style,
                    ),
                    Span::raw(" "),
                    highlight_text(
                        &format!("{:<6}", row.stream),
                        app.search_query.as_deref(),
                        Style::default().fg(Color::Blue),
                    ),
                    Span::raw(" "),
                    highlight_text(&row.message, app.search_query.as_deref(), Style::default()),
                ])
            })
            .collect()
    };

    let max_scroll = app.log_max_scroll();
    let scroll = if app.follow_logs {
        max_scroll
    } else {
        app.log_scroll.min(max_scroll)
    };

    let paragraph = Paragraph::new(lines)
        .block(Block::default().borders(Borders::ALL).title(title))
        .wrap(Wrap { trim: false })
        .scroll((scroll as u16, 0));

    frame.render_widget(paragraph, area);
}

fn draw_footer(frame: &mut ratatui::Frame<'_>, area: Rect, app: &App) {
    let message = format!(
        "{}  |  j/k Move  Tab Switch Pane  / Search  e Export  s Status  v Lineage  t Tag  T ClearTag  f Follow  r Refresh  q Quit",
        app.status_message
    );
    frame.render_widget(Clear, area);
    frame.render_widget(Paragraph::new(message), area);
}

fn draw_overlay(frame: &mut ratatui::Frame<'_>, app: &App) {
    if app.show_help {
        draw_help_overlay(frame);
        return;
    }

    match app.input_mode {
        InputMode::Normal => {}
        InputMode::Search => {
            let area = centered_rect(70, 3, frame.area());
            frame.render_widget(Clear, area);
            frame.render_widget(
                Paragraph::new(format!("Search logs: {}", app.search_buffer))
                    .block(Block::default().borders(Borders::ALL).title(" Search ")),
                area,
            );
        }
        InputMode::Export => {
            let area = centered_rect(60, 6, frame.area());
            frame.render_widget(Clear, area);
            let text = vec![
                Line::from("Export current task logs"),
                Line::from(format!(
                    "Task: {}",
                    app.selected_task_id()
                        .map(|id| format!("#{id}"))
                        .unwrap_or_else(|| "-".to_string())
                )),
                Line::from(format!(
                    "Format: {}  (1=txt 2=json 3=csv 4=html, h/l switch)",
                    export_format_name(app.export_format)
                )),
                Line::from("Enter = confirm, Esc = cancel"),
            ];
            frame.render_widget(
                Paragraph::new(text)
                    .block(Block::default().borders(Borders::ALL).title(" Export ")),
                area,
            );
        }
        InputMode::TagSelect => {
            let area = centered_rect(50, 12, frame.area());
            frame.render_widget(Clear, area);
            let items = tag_popup_items(&app.available_tags);
            let list = List::new(items)
                .block(Block::default().borders(Borders::ALL).title(" Tag Filter "))
                .highlight_style(
                    Style::default()
                        .bg(Color::Blue)
                        .fg(Color::Black)
                        .add_modifier(Modifier::BOLD),
                )
                .highlight_symbol(">");
            let mut state = ListState::default();
            state.select(Some(app.tag_popup_index.min(app.available_tags.len())));
            frame.render_stateful_widget(list, area, &mut state);
        }
        InputMode::RetryConfirm => {
            let area = centered_rect(50, 5, frame.area());
            frame.render_widget(Clear, area);
            let text = vec![
                Line::from(format!(
                    "Retry task #{}?",
                    app.selected_task_id().unwrap_or(0)
                )),
                Line::from(""),
                Line::from("Press y to confirm, n or Esc to cancel"),
            ];
            frame.render_widget(
                Paragraph::new(text).block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title(" Confirm Retry "),
                ),
                area,
            );
        }
    }
}

fn draw_help_overlay(frame: &mut ratatui::Frame<'_>) {
    let area = centered_rect(80, 22, frame.area());
    frame.render_widget(Clear, area);

    let help_text = vec![
        Line::from(Span::styled(
            "Keyboard Shortcuts",
            Style::default().add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
        Line::from("Navigation:"),
        Line::from("  Tab          Switch focus between Tasks and Logs"),
        Line::from("  j/k, ↓/↑     Move selection up/down"),
        Line::from("  u/d, PgUp/Dn Page up/down"),
        Line::from("  g/G, Home/End Go to first/last"),
        Line::from(""),
        Line::from("Actions:"),
        Line::from("  r            Manual refresh"),
        Line::from("  p            Pause/resume auto-refresh"),
        Line::from("  +/-          Increase/decrease refresh interval"),
        Line::from("  R            Retry selected task (with confirmation)"),
        Line::from("  e            Export selected task"),
        Line::from(""),
        Line::from("Filters:"),
        Line::from("  s            Cycle status filter (all/running/success/failed)"),
        Line::from("  v            Cycle lineage view (all/triggered/retry)"),
        Line::from("  t            Select tag filter"),
        Line::from("  T            Clear tag filter"),
        Line::from("  /            Search logs"),
        Line::from(""),
        Line::from("Other:"),
        Line::from("  f            Toggle log follow mode"),
        Line::from("  ?            Toggle this help"),
        Line::from("  q            Quit"),
    ];

    frame.render_widget(
        Paragraph::new(help_text)
            .block(Block::default().borders(Borders::ALL).title(" Help "))
            .wrap(Wrap { trim: false }),
        area,
    );
}

fn centered_rect(width_percent: u16, height: u16, area: Rect) -> Rect {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(1),
            Constraint::Length(height),
            Constraint::Min(1),
        ])
        .split(area);

    let horizontal = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - width_percent) / 2),
            Constraint::Percentage(width_percent),
            Constraint::Percentage((100 - width_percent) / 2),
        ])
        .split(vertical[1]);

    horizontal[1]
}

pub fn next_export_format(format: ExportFormat) -> ExportFormat {
    match format {
        ExportFormat::Txt => ExportFormat::Json,
        ExportFormat::Json => ExportFormat::Csv,
        ExportFormat::Csv => ExportFormat::Html,
        ExportFormat::Html => ExportFormat::Txt,
    }
}

pub fn previous_export_format(format: ExportFormat) -> ExportFormat {
    match format {
        ExportFormat::Txt => ExportFormat::Html,
        ExportFormat::Json => ExportFormat::Txt,
        ExportFormat::Csv => ExportFormat::Json,
        ExportFormat::Html => ExportFormat::Csv,
    }
}

fn export_format_name(format: ExportFormat) -> &'static str {
    match format {
        ExportFormat::Txt => "txt",
        ExportFormat::Json => "json",
        ExportFormat::Csv => "csv",
        ExportFormat::Html => "html",
    }
}

pub fn export_extension(format: ExportFormat) -> &'static str {
    export_format_name(format)
}

pub fn popup_index_for_tag(current: Option<&str>, available_tags: &[String]) -> usize {
    match current {
        None => 0,
        Some(current) => available_tags
            .iter()
            .position(|tag| tag == current)
            .map(|index| index + 1)
            .unwrap_or(0),
    }
}

pub fn selected_tag_from_popup_index(index: usize, available_tags: &[String]) -> Option<String> {
    if index == 0 {
        None
    } else {
        available_tags.get(index - 1).cloned()
    }
}

fn tag_popup_items(available_tags: &[String]) -> Vec<ListItem<'static>> {
    let mut items = Vec::with_capacity(available_tags.len() + 1);
    items.push(ListItem::new(Line::from(vec![
        Span::styled("*", Style::default().fg(Color::Green)),
        Span::raw(" all tags"),
    ])));
    for tag in available_tags {
        items.push(ListItem::new(Line::from(tag.clone())));
    }
    items
}

fn highlight_text(value: impl Into<String>, query: Option<&str>, base: Style) -> Span<'static> {
    let value = value.into();
    let Some(query) = query else {
        return Span::styled(value, base);
    };

    if query.is_empty() {
        return Span::styled(value, base);
    }

    let haystack = value.to_lowercase();
    let needle = query.to_lowercase();
    if haystack.contains(&needle) {
        Span::styled(
            value,
            base.bg(Color::Yellow)
                .fg(Color::Black)
                .add_modifier(Modifier::BOLD),
        )
    } else {
        Span::styled(value, base)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::TuiArgs;
    use crate::exporter::TaskExportInfo;
    use crate::config::Config;
    use std::path::PathBuf;

    fn sample_args() -> TuiArgs {
        TuiArgs {
            tag: None,
            refresh_ms: 1000,
            limit: 100,
        }
    }

    fn sample_log() -> QueryLogRow {
        QueryLogRow {
            id: 1,
            task_id: 42,
            tag: Some("fixture-api".into()),
            ts: "2026-03-21T12:00:00+08:00".into(),
            stream: "stderr".into(),
            level: "error".into(),
            message: "connection timeout while syncing index".into(),
            status: "failed".into(),
        }
    }

    #[test]
    fn search_matches_multiple_fields_case_insensitively() {
        let mut app = App::new(
            PathBuf::from("db.sqlite"),
            "db".into(),
            Config::default(),
            sample_args(),
        );
        app.search_query = Some("FIXTURE-api".into());
        assert!(app.matches_search(&sample_log()));

        app.search_query = Some("timeout".into());
        assert!(app.matches_search(&sample_log()));

        app.search_query = Some("running".into());
        assert!(!app.matches_search(&sample_log()));
    }

    #[test]
    fn export_format_cycles_round_trip() {
        assert_eq!(next_export_format(ExportFormat::Txt), ExportFormat::Json);
        assert_eq!(next_export_format(ExportFormat::Html), ExportFormat::Txt);
        assert_eq!(
            previous_export_format(ExportFormat::Txt),
            ExportFormat::Html
        );
        assert_eq!(
            previous_export_format(ExportFormat::Json),
            ExportFormat::Txt
        );
    }

    #[test]
    fn tag_popup_selection_maps_to_filter() {
        let tags = vec![
            "fixture-api".to_string(),
            "fixture-db".to_string(),
            "fixture-worker".to_string(),
        ];
        assert_eq!(popup_index_for_tag(None, &tags), 0);
        assert_eq!(popup_index_for_tag(Some("fixture-db"), &tags), 2);
        assert_eq!(selected_tag_from_popup_index(0, &tags), None);
        assert_eq!(
            selected_tag_from_popup_index(1, &tags),
            Some("fixture-api".to_string())
        );
        assert_eq!(
            selected_tag_from_popup_index(3, &tags),
            Some("fixture-worker".to_string())
        );
    }

    #[test]
    fn task_detail_lines_include_shell_and_pid() {
        let detail = TaskExportInfo {
            id: 7,
            tag: Some("demo".into()),
            command: "cargo test".into(),
            command_json: Some("[\"cargo\",\"test\"]".into()),
            shell: Some("bash".into()),
            work_dir: ".".into(),
            started_at: "2026-03-21T12:00:00+08:00".into(),
            ended_at: Some("2026-03-21T12:01:00+08:00".into()),
            duration_ms: Some(60_000),
            pid: Some(1234),
            parent_task_id: Some(3),
            retry_of_task_id: Some(5),
            trigger_type: Some("retry".into()),
            exit_code: Some(1),
            status: "failed".into(),
            env_vars: Some("FOO=bar".into()),
        };
        let logs = vec![];

        let rendered: Vec<String> = task_detail_lines(&detail, &logs)
            .into_iter()
            .map(|line| line.to_string())
            .collect();

        assert!(rendered.iter().any(|line| line.contains("Shell:     bash")));
        assert!(rendered.iter().any(|line| line.contains("PID:       1234")));
        assert!(rendered.iter().any(|line| line.contains("Parent:    3")));
        assert!(rendered.iter().any(|line| line.contains("Retry Of:  5")));
        assert!(rendered.iter().any(|line| line.contains("Trigger:   retry")));
    }

    #[test]
    fn task_lineage_label_prefers_retry_then_dependency_hint() {
        let retry_task = crate::formatter::ListTaskRow {
            id: 9,
            tag: Some("demo".into()),
            status: "failed".into(),
            shell: Some("pwsh".into()),
            work_dir: ".".into(),
            started_at: "2026-03-21T12:00:00+08:00".into(),
            ended_at: None,
            duration_ms: None,
            pid: Some(4321),
            parent_task_id: Some(7),
            retry_of_task_id: Some(7),
            trigger_type: Some("retry".into()),
            command: "cargo test".into(),
            env_vars: None,
        };
        let dependency_task = crate::formatter::ListTaskRow {
            id: 10,
            tag: Some("demo".into()),
            status: "success".into(),
            shell: Some("pwsh".into()),
            work_dir: ".".into(),
            started_at: "2026-03-21T12:10:00+08:00".into(),
            ended_at: None,
            duration_ms: None,
            pid: Some(5678),
            parent_task_id: Some(8),
            retry_of_task_id: None,
            trigger_type: Some("dependency".into()),
            command: "cargo build".into(),
            env_vars: None,
        };

        assert_eq!(task_lineage_label(&retry_task).as_deref(), Some("retry#7"));
        assert_eq!(task_lineage_label(&dependency_task).as_deref(), Some("wait#8"));
    }

    #[test]
    fn task_detail_lines_include_log_summary_and_latest_signal() {
        let detail = TaskExportInfo {
            id: 7,
            tag: Some("demo".into()),
            command: "cargo test".into(),
            command_json: Some("[\"cargo\",\"test\"]".into()),
            shell: Some("bash".into()),
            work_dir: ".".into(),
            started_at: "2026-03-21T12:00:00+08:00".into(),
            ended_at: Some("2026-03-21T12:01:00+08:00".into()),
            duration_ms: Some(60_000),
            pid: Some(1234),
            parent_task_id: Some(3),
            retry_of_task_id: Some(5),
            trigger_type: Some("retry".into()),
            exit_code: Some(1),
            status: "failed".into(),
            env_vars: Some("FOO=bar".into()),
        };
        let logs = vec![
            QueryLogRow {
                id: 1,
                task_id: 7,
                tag: Some("demo".into()),
                ts: "2026-03-21T12:00:01+08:00".into(),
                stream: "stdout".into(),
                level: "info".into(),
                message: "starting build".into(),
                status: "failed".into(),
            },
            QueryLogRow {
                id: 2,
                task_id: 7,
                tag: Some("demo".into()),
                ts: "2026-03-21T12:00:05+08:00".into(),
                stream: "stderr".into(),
                level: "warn".into(),
                message: "cache miss".into(),
                status: "failed".into(),
            },
            QueryLogRow {
                id: 3,
                task_id: 7,
                tag: Some("demo".into()),
                ts: "2026-03-21T12:00:09+08:00".into(),
                stream: "stderr".into(),
                level: "error".into(),
                message: "compile failed".into(),
                status: "failed".into(),
            },
        ];

        let rendered: Vec<String> = task_detail_lines(&detail, &logs)
            .into_iter()
            .map(|line| line.to_string())
            .collect();

        assert!(rendered.iter().any(|line| line.contains("Logs:      total=3 stderr=2 warn=1 error=1")));
        assert!(rendered.iter().any(|line| line.contains("Signal:    error compile failed")));
    }
}
