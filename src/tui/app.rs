use crate::Result;
use crate::analyzer::{AnalysisFilter, AnalysisReport, collect_analysis};
use crate::cli::ExportFormat;
use crate::config::Config;
use crate::executor::{get_task_info, run_task_with_origin};
use crate::exporter::{TaskExportInfo, render_export};
use crate::formatter::{ListTaskRow, QueryLogRow};
use crate::store::{
    DashboardStats, LineageFilter, TaskListFilter, fetch_available_tags, fetch_dashboard_stats,
    fetch_task_detail, fetch_task_list, fetch_task_logs,
};
use crate::utils::format_rfc3339_millis;
use crossterm::event::{KeyCode, KeyEvent};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver};
use std::time::{Duration, Instant};
use unicode_width::UnicodeWidthChar;

use super::draw::{
    build_detail_lines, compute_detail_height, export_extension, next_export_format,
    popup_index_for_tag, previous_export_format, rendered_line_count,
    selected_tag_from_popup_index,
};

pub const MIN_TASK_LIMIT: i64 = 10;
pub const MAX_TAGS_FETCH: i64 = 64;
pub const MIN_REFRESH_MS: u64 = 200;
pub const POLL_INTERVAL_MS: u64 = 50;
pub const PAGE_SCROLL_SIZE: usize = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FocusPane {
    Tasks,
    Logs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputMode {
    Normal,
    Search,
    Export,
    TagSelect,
    RetryConfirm,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusFilter {
    All,
    Running,
    Success,
    Failed,
}

impl LineageFilter {
    pub fn next(self) -> Self {
        match self {
            Self::All => Self::Triggered,
            Self::Triggered => Self::RetryOnly,
            Self::RetryOnly => Self::All,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Triggered => "triggered",
            Self::RetryOnly => "retry",
        }
    }
}

impl StatusFilter {
    pub fn next(self) -> Self {
        match self {
            Self::All => Self::Running,
            Self::Running => Self::Success,
            Self::Success => Self::Failed,
            Self::Failed => Self::All,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Running => "running",
            Self::Success => "success",
            Self::Failed => "failed",
        }
    }

    pub fn as_option(self) -> Option<&'static str> {
        match self {
            Self::All => None,
            Self::Running => Some("running"),
            Self::Success => Some("success"),
            Self::Failed => Some("failed"),
        }
    }
}

pub struct App {
    pub db_path: PathBuf,
    pub db_label: String,
    pub config: Config,
    pub refresh_every: Duration,
    pub last_refresh_at: Instant,
    pub auto_refresh: bool,
    pub focus: FocusPane,
    pub input_mode: InputMode,
    pub follow_logs: bool,
    pub status_filter: StatusFilter,
    pub lineage_filter: LineageFilter,
    pub tag_filter: Option<String>,
    pub available_tags: Vec<String>,
    pub tag_popup_index: usize,
    pub task_limit: i64,
    pub selected_task_index: usize,
    pub tasks: Vec<ListTaskRow>,
    pub dashboard: DashboardStats,
    pub analysis: AnalysisReport,
    pub detail: Option<TaskExportInfo>,
    pub detail_scroll: usize,
    pub detail_wrap_width: u16,
    pub detail_viewport_height: u16,
    pub logs: Vec<QueryLogRow>,
    pub log_scroll: usize,
    pub log_wrap_width: u16,
    pub log_viewport_height: u16,
    pub last_log_id: i64,
    pub search_query: Option<String>,
    pub search_buffer: String,
    pub export_format: ExportFormat,
    pub retry_rx: Option<Receiver<String>>,
    pub retry_in_progress: bool,
    pub status_message: String,
    pub show_help: bool,
}

impl App {
    pub fn new(
        db_path: PathBuf,
        db_label: String,
        config: Config,
        args: crate::cli::TuiArgs,
    ) -> Self {
        Self {
            db_path,
            db_label,
            config,
            refresh_every: Duration::from_millis(args.refresh_ms.max(MIN_REFRESH_MS)),
            last_refresh_at: Instant::now() - Duration::from_secs(60),
            auto_refresh: true,
            focus: FocusPane::Tasks,
            input_mode: InputMode::Normal,
            follow_logs: true,
            status_filter: StatusFilter::All,
            lineage_filter: LineageFilter::All,
            tag_filter: args.tag,
            available_tags: Vec::new(),
            tag_popup_index: 0,
            task_limit: args.limit.max(MIN_TASK_LIMIT),
            selected_task_index: 0,
            tasks: Vec::new(),
            dashboard: DashboardStats::default(),
            analysis: AnalysisReport::default(),
            detail: None,
            detail_scroll: 0,
            detail_wrap_width: 0,
            detail_viewport_height: 0,
            logs: Vec::new(),
            log_scroll: 0,
            log_wrap_width: 0,
            log_viewport_height: 0,
            last_log_id: 0,
            search_query: None,
            search_buffer: String::new(),
            export_format: ExportFormat::Html,
            retry_rx: None,
            retry_in_progress: false,
            status_message: "Press ? for help".to_string(),
            show_help: false,
        }
    }

    pub fn poll_timeout(&self) -> Duration {
        let elapsed = self.last_refresh_at.elapsed();
        if elapsed >= self.refresh_every {
            Duration::from_millis(POLL_INTERVAL_MS)
        } else {
            self.refresh_every - elapsed
        }
    }

    pub fn should_refresh(&self) -> bool {
        self.auto_refresh && self.last_refresh_at.elapsed() >= self.refresh_every
    }

    fn request_refresh(&mut self) {
        self.last_refresh_at = Instant::now() - self.refresh_every;
    }

    fn apply_filter_change(&mut self) {
        self.selected_task_index = 0;
        self.request_refresh();
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> Result<bool> {
        match self.input_mode {
            InputMode::Normal => self.handle_normal_key(key),
            InputMode::Search => self.handle_search_key(key),
            InputMode::Export => self.handle_export_key(key),
            InputMode::TagSelect => self.handle_tag_select_key(key),
            InputMode::RetryConfirm => self.handle_retry_confirm_key(key),
        }
    }

    fn handle_normal_key(&mut self, key: KeyEvent) -> Result<bool> {
        match key.code {
            KeyCode::Char('q') => return Ok(true),
            KeyCode::Char('?') => {
                self.show_help = !self.show_help;
            }
            KeyCode::Tab => {
                self.focus = match self.focus {
                    FocusPane::Tasks => FocusPane::Logs,
                    FocusPane::Logs => FocusPane::Tasks,
                };
            }
            KeyCode::Char('u')
                if key
                    .modifiers
                    .contains(crossterm::event::KeyModifiers::CONTROL) =>
            {
                self.scroll_detail_up(PAGE_SCROLL_SIZE);
            }
            KeyCode::Char('d')
                if key
                    .modifiers
                    .contains(crossterm::event::KeyModifiers::CONTROL) =>
            {
                self.scroll_detail_down(PAGE_SCROLL_SIZE);
            }
            KeyCode::Char('b')
                if key
                    .modifiers
                    .contains(crossterm::event::KeyModifiers::CONTROL) =>
            {
                self.detail_scroll = 0;
            }
            KeyCode::Char('f')
                if key
                    .modifiers
                    .contains(crossterm::event::KeyModifiers::CONTROL) =>
            {
                self.scroll_detail_to_end();
            }
            KeyCode::Char('r') => {
                self.request_refresh();
                self.status_message = "Manual refresh requested".to_string();
            }
            KeyCode::Char('p') => {
                self.auto_refresh = !self.auto_refresh;
                self.status_message = if self.auto_refresh {
                    "Auto-refresh enabled".to_string()
                } else {
                    "Auto-refresh paused".to_string()
                };
            }
            KeyCode::Char('R') => {
                if self.selected_task_id().is_some() {
                    self.input_mode = InputMode::RetryConfirm;
                    self.status_message = "Retry task? Press y to confirm, n to cancel".to_string();
                } else {
                    self.status_message = "No task selected".to_string();
                }
            }
            KeyCode::Char('f') => {
                self.follow_logs = !self.follow_logs;
                self.status_message = if self.follow_logs {
                    "Log follow enabled".to_string()
                } else {
                    "Log follow paused".to_string()
                };
                if self.follow_logs {
                    self.scroll_logs_to_end();
                }
            }
            KeyCode::Char('s') => {
                self.status_filter = self.status_filter.next();
                self.apply_filter_change();
                self.status_message = format!("Status filter: {}", self.status_filter.as_str());
            }
            KeyCode::Char('v') => {
                self.lineage_filter = self.lineage_filter.next();
                self.apply_filter_change();
                self.status_message = format!("Lineage view: {}", self.lineage_filter.as_str());
            }
            KeyCode::Char('t') => {
                self.input_mode = InputMode::TagSelect;
                self.tag_popup_index =
                    popup_index_for_tag(self.tag_filter.as_deref(), &self.available_tags);
                self.status_message = "Select tag: Enter=apply, Esc=cancel, T=clear".to_string();
            }
            KeyCode::Char('T') => {
                self.tag_filter = None;
                self.apply_filter_change();
                self.status_message = "Tag filter cleared".to_string();
            }
            KeyCode::Home | KeyCode::Char('g') => {
                if self.focus == FocusPane::Tasks {
                    self.selected_task_index = 0;
                } else {
                    self.log_scroll = 0;
                    self.follow_logs = false;
                }
            }
            KeyCode::End | KeyCode::Char('G') => {
                if self.focus == FocusPane::Tasks {
                    if !self.tasks.is_empty() {
                        self.selected_task_index = self.tasks.len().saturating_sub(1);
                    }
                } else {
                    self.follow_logs = true;
                    self.scroll_logs_to_end();
                }
            }
            KeyCode::Up | KeyCode::Char('k') => {
                if self.focus == FocusPane::Tasks {
                    self.selected_task_index = self.selected_task_index.saturating_sub(1);
                } else {
                    self.follow_logs = false;
                    self.scroll_logs_up(1);
                }
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if self.focus == FocusPane::Tasks {
                    if self.selected_task_index + 1 < self.tasks.len() {
                        self.selected_task_index += 1;
                    }
                } else {
                    self.follow_logs = false;
                    self.scroll_logs_down(1);
                }
            }
            KeyCode::PageUp | KeyCode::Char('u') => {
                if self.focus == FocusPane::Tasks {
                    self.selected_task_index =
                        self.selected_task_index.saturating_sub(PAGE_SCROLL_SIZE);
                } else {
                    self.follow_logs = false;
                    self.scroll_logs_up(PAGE_SCROLL_SIZE);
                }
            }
            KeyCode::PageDown | KeyCode::Char('d') => {
                if self.focus == FocusPane::Tasks {
                    self.selected_task_index = (self.selected_task_index + PAGE_SCROLL_SIZE)
                        .min(self.tasks.len().saturating_sub(1));
                } else {
                    self.follow_logs = false;
                    self.scroll_logs_down(PAGE_SCROLL_SIZE);
                }
            }
            KeyCode::Char('/') => {
                self.input_mode = InputMode::Search;
                self.search_buffer = self.search_query.clone().unwrap_or_default();
                self.status_message = "Search: type and press Enter, Esc to cancel".to_string();
            }
            KeyCode::Char('e') => {
                if self.selected_task_id().is_some() {
                    self.input_mode = InputMode::Export;
                    self.status_message =
                        "Export: h/l=format, Enter=confirm, Esc=cancel".to_string();
                } else {
                    self.status_message = "No task selected".to_string();
                }
            }
            KeyCode::Char('+') | KeyCode::Char('=') => {
                let new_ms = (self.refresh_every.as_millis() as u64 + 500).min(10000);
                self.refresh_every = Duration::from_millis(new_ms);
                self.status_message = format!("Refresh interval: {}ms", new_ms);
            }
            KeyCode::Char('-') => {
                let new_ms = (self.refresh_every.as_millis() as u64)
                    .saturating_sub(500)
                    .max(200);
                self.refresh_every = Duration::from_millis(new_ms);
                self.status_message = format!("Refresh interval: {}ms", new_ms);
            }
            _ => {}
        }
        Ok(false)
    }

    fn handle_tag_select_key(&mut self, key: KeyEvent) -> Result<bool> {
        let max_index = self.available_tags.len();
        match key.code {
            KeyCode::Esc => {
                self.input_mode = InputMode::Normal;
                self.status_message = "Tag selection canceled".to_string();
            }
            KeyCode::Char('T') => {
                self.input_mode = InputMode::Normal;
                self.tag_filter = None;
                self.apply_filter_change();
                self.status_message = "Tag filter cleared".to_string();
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.tag_popup_index = self.tag_popup_index.saturating_sub(1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if self.tag_popup_index < max_index {
                    self.tag_popup_index += 1;
                }
            }
            KeyCode::Char('g') => self.tag_popup_index = 0,
            KeyCode::Char('G') => self.tag_popup_index = max_index,
            KeyCode::Enter => {
                self.tag_filter =
                    selected_tag_from_popup_index(self.tag_popup_index, &self.available_tags);
                self.apply_filter_change();
                self.input_mode = InputMode::Normal;
                self.status_message = format!(
                    "Tag filter set to {}",
                    self.tag_filter.as_deref().unwrap_or("*")
                );
            }
            _ => {}
        }
        Ok(false)
    }

    fn handle_search_key(&mut self, key: KeyEvent) -> Result<bool> {
        match key.code {
            KeyCode::Esc => {
                self.input_mode = InputMode::Normal;
                self.search_buffer.clear();
                self.status_message = "Search canceled".to_string();
            }
            KeyCode::Enter => {
                let query = self.search_buffer.trim().to_string();
                self.search_query = if query.is_empty() {
                    None
                } else {
                    Some(query.clone())
                };
                self.input_mode = InputMode::Normal;
                self.follow_logs = true;
                self.scroll_logs_to_end();
                self.status_message = if query.is_empty() {
                    "Search cleared".to_string()
                } else {
                    format!("Search applied: {}", query)
                };
                self.search_buffer.clear();
            }
            KeyCode::Backspace => {
                self.search_buffer.pop();
            }
            KeyCode::Char(c) => {
                self.search_buffer.push(c);
            }
            _ => {}
        }
        Ok(false)
    }

    fn handle_export_key(&mut self, key: KeyEvent) -> Result<bool> {
        match key.code {
            KeyCode::Esc => {
                self.input_mode = InputMode::Normal;
                self.status_message = "Export canceled".to_string();
            }
            KeyCode::Left | KeyCode::Char('h') => {
                self.export_format = previous_export_format(self.export_format);
            }
            KeyCode::Right | KeyCode::Char('l') => {
                self.export_format = next_export_format(self.export_format);
            }
            KeyCode::Char('1') => self.export_format = ExportFormat::Txt,
            KeyCode::Char('2') => self.export_format = ExportFormat::Json,
            KeyCode::Char('3') => self.export_format = ExportFormat::Csv,
            KeyCode::Char('4') => self.export_format = ExportFormat::Html,
            KeyCode::Enter => {
                let output = self.export_current_task()?;
                self.input_mode = InputMode::Normal;
                self.status_message = format!("Exported current task to {}", output.display());
            }
            _ => {}
        }
        Ok(false)
    }

    fn handle_retry_confirm_key(&mut self, key: KeyEvent) -> Result<bool> {
        match key.code {
            KeyCode::Char('y') | KeyCode::Char('Y') => {
                self.input_mode = InputMode::Normal;
                self.start_retry_current_task()?;
            }
            KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => {
                self.input_mode = InputMode::Normal;
                self.status_message = "Retry canceled".to_string();
            }
            _ => {}
        }
        Ok(false)
    }

    pub fn refresh(&mut self, conn: &rusqlite::Connection) -> Result<()> {
        let previous_task_id = self.selected_task_id();
        self.available_tags = fetch_available_tags(conn, MAX_TAGS_FETCH)?;
        if let Some(tag) = self.tag_filter.as_deref() {
            if !self.available_tags.iter().any(|value| value == tag) {
                self.tag_filter = None;
            }
        }
        self.dashboard = fetch_dashboard_stats(conn, self.tag_filter.as_deref())?;
        self.tasks = fetch_task_list(
            conn,
            &TaskListFilter {
                tag: self.tag_filter.clone(),
                status: self.status_filter.as_option().map(str::to_string),
                lineage_filter: self.lineage_filter,
                limit: self.task_limit,
                offset: 0,
            },
        )?;

        if self.tasks.is_empty() {
            self.selected_task_index = 0;
            self.detail = None;
            self.detail_scroll = 0;
            self.logs.clear();
            self.last_log_id = 0;
            self.log_scroll = 0;
            self.analysis = collect_analysis(
                conn,
                &AnalysisFilter {
                    tag: self.tag_filter.clone(),
                    from: None,
                    to: None,
                    top_tags: 0,
                },
            )?;
            self.status_message = "No tasks match the current filter".to_string();
            self.last_refresh_at = Instant::now();
            return Ok(());
        }

        if let Some(previous_task_id) = previous_task_id {
            if let Some(index) = self
                .tasks
                .iter()
                .position(|task| task.id == previous_task_id)
            {
                self.selected_task_index = index;
            } else {
                self.selected_task_index = self
                    .selected_task_index
                    .min(self.tasks.len().saturating_sub(1));
                self.logs.clear();
                self.last_log_id = 0;
            }
        } else {
            self.selected_task_index = self
                .selected_task_index
                .min(self.tasks.len().saturating_sub(1));
        }

        let current_task_id = self.selected_task_id();
        if current_task_id != self.detail.as_ref().map(|detail| detail.id) {
            self.detail_scroll = 0;
            self.logs.clear();
            self.last_log_id = 0;
            self.log_scroll = 0;
        }

        if let Some(task_id) = current_task_id {
            self.detail = fetch_task_detail(conn, task_id)?;
            let new_logs = fetch_task_logs(conn, task_id, self.last_log_id)?;
            if let Some(last_row) = new_logs.last() {
                self.last_log_id = last_row.id;
            }
            self.logs.extend(new_logs);
            if self.follow_logs {
                self.scroll_logs_to_end();
            } else {
                self.clamp_log_scroll();
            }
        }

        self.analysis = collect_analysis(
            conn,
            &AnalysisFilter {
                tag: self.tag_filter.clone(),
                from: None,
                to: None,
                top_tags: 0,
            },
        )?;

        self.last_refresh_at = Instant::now();
        Ok(())
    }

    pub fn poll_background(&mut self, conn: &rusqlite::Connection) -> Result<()> {
        let mut completed = None;
        if let Some(rx) = &self.retry_rx {
            match rx.try_recv() {
                Ok(message) => completed = Some(message),
                Err(mpsc::TryRecvError::Empty) => {}
                Err(mpsc::TryRecvError::Disconnected) => {
                    completed = Some("Background retry channel disconnected".to_string());
                }
            }
        }

        if let Some(message) = completed {
            self.retry_rx = None;
            self.retry_in_progress = false;
            self.status_message = message;
            self.refresh(conn)?;
        }

        Ok(())
    }

    pub fn selected_task_id(&self) -> Option<i64> {
        self.tasks.get(self.selected_task_index).map(|task| task.id)
    }

    pub fn scroll_logs_to_end(&mut self) {
        self.log_scroll = self.log_max_scroll();
    }

    pub fn filtered_logs_len(&self) -> usize {
        self.logs
            .iter()
            .filter(|row| self.matches_search(row))
            .count()
    }

    pub fn matches_search(&self, row: &QueryLogRow) -> bool {
        let Some(query) = self.search_query.as_ref() else {
            return true;
        };

        let needle = query.to_lowercase();
        let tag = row.tag.as_deref().unwrap_or("").to_lowercase();
        row.message.to_lowercase().contains(&needle)
            || row.level.to_lowercase().contains(&needle)
            || row.stream.to_lowercase().contains(&needle)
            || row.status.to_lowercase().contains(&needle)
            || row.ts.to_lowercase().contains(&needle)
            || tag.contains(&needle)
    }

    pub fn update_viewport(&mut self, area: Rect) {
        let vertical = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Min(10),
                Constraint::Length(1),
            ])
            .split(area);

        let columns = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(25), Constraint::Percentage(75)])
            .split(vertical[1]);

        let right = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(compute_detail_height(
                    columns[1].height,
                    columns[1].width.saturating_sub(2),
                    &build_detail_lines(self.detail.as_ref(), &self.logs),
                )),
                Constraint::Min(8),
            ])
            .split(columns[1]);

        let detail_area = right[0];
        self.detail_wrap_width = detail_area.width.saturating_sub(2);
        self.detail_viewport_height = detail_area.height.saturating_sub(2);
        let logs_area = right[1];
        self.log_wrap_width = logs_area.width.saturating_sub(2);
        self.log_viewport_height = logs_area.height.saturating_sub(2);
        self.clamp_detail_scroll();
        self.clamp_log_scroll();
    }

    pub fn detail_max_scroll(&self) -> usize {
        rendered_line_count(
            &build_detail_lines(self.detail.as_ref(), &self.logs),
            self.detail_wrap_width,
        )
        .saturating_sub(self.detail_viewport_height as usize)
    }

    pub fn clamp_detail_scroll(&mut self) {
        self.detail_scroll = self.detail_scroll.min(self.detail_max_scroll());
    }

    fn scroll_detail_up(&mut self, lines: usize) {
        self.detail_scroll = self.detail_scroll.saturating_sub(lines);
    }

    fn scroll_detail_down(&mut self, lines: usize) {
        self.detail_scroll = self.detail_scroll.saturating_add(lines);
        self.clamp_detail_scroll();
    }

    fn scroll_detail_to_end(&mut self) {
        self.detail_scroll = self.detail_max_scroll();
    }

    pub fn log_max_scroll(&self) -> usize {
        self.log_rendered_line_count()
            .saturating_sub(self.log_viewport_height as usize)
    }

    pub fn clamp_log_scroll(&mut self) {
        self.log_scroll = self.log_scroll.min(self.log_max_scroll());
    }

    fn scroll_logs_up(&mut self, lines: usize) {
        self.log_scroll = self.log_scroll.saturating_sub(lines);
    }

    fn scroll_logs_down(&mut self, lines: usize) {
        self.log_scroll = self.log_scroll.saturating_add(lines);
        self.clamp_log_scroll();
    }

    fn log_rendered_line_count(&self) -> usize {
        if self.log_wrap_width == 0 {
            return 0;
        }

        let line_count: usize = self
            .logs
            .iter()
            .filter(|row| self.matches_search(row))
            .map(|row| wrapped_text_line_count(&self.format_log_line(row), self.log_wrap_width))
            .sum();

        line_count.max(1)
    }

    fn format_log_line(&self, row: &QueryLogRow) -> String {
        format!(
            "{} {:<5} {:<6} {}",
            format_rfc3339_millis(&row.ts),
            row.level,
            row.stream,
            row.message
        )
    }

    fn export_current_task(&self) -> Result<PathBuf> {
        let Some(detail) = self.detail.as_ref() else {
            return Err(std::io::Error::other("no task selected for export").into());
        };

        let mut output = PathBuf::from("exports");
        std::fs::create_dir_all(&output)?;
        output.push(format!(
            "task-{}.{}",
            detail.id,
            export_extension(self.export_format)
        ));

        let rendered = render_export(self.export_format, &self.logs, Some(detail));
        std::fs::write(&output, rendered)?;
        Ok(output)
    }

    fn start_retry_current_task(&mut self) -> Result<()> {
        if self.retry_in_progress {
            self.status_message = "Retry already running in background".to_string();
            return Ok(());
        }

        let Some(task_id) = self.selected_task_id() else {
            self.status_message = "No task selected to retry".to_string();
            return Ok(());
        };

        let db_path = self.db_path.clone();
        let config = self.config.clone();
        let (tx, rx) = mpsc::channel();
        self.retry_rx = Some(rx);
        self.retry_in_progress = true;
        self.status_message = format!("Retrying task {} in background...", task_id);

        std::thread::spawn(move || {
            let result = (|| -> Result<String> {
                let conn = rusqlite::Connection::open(&db_path)?;
                conn.execute_batch("PRAGMA foreign_keys = ON;")?;
                let task = get_task_info(&conn, task_id)?;

                let run_args = crate::cli::RunArgs {
                    tag: task.tag,
                    cwd: Some(PathBuf::from(task.work_dir)),
                    live: false,
                    background: false,
                    wait_for: None,
                    command: task.command_args,
                    env_files: vec![],
                    env_vars: vec![],
                };

                let (new_task_id, status) = run_task_with_origin(
                    &conn,
                    run_args,
                    &config,
                    crate::executor::TaskOrigin {
                        parent_task_id: Some(task_id),
                        retry_of_task_id: Some(task_id),
                        trigger_type: Some(crate::executor::TriggerType::Retry),
                    },
                )?;
                Ok(format!(
                    "Retried task {} as #{} ({})",
                    task_id, new_task_id, status
                ))
            })();

            let _ = tx.send(match result {
                Ok(message) => message,
                Err(err) => format!("Retry failed: {}", err),
            });
        });

        Ok(())
    }
}

fn wrapped_text_line_count(text: &str, max_width: u16) -> usize {
    if max_width == 0 {
        return 0;
    }
    if text.is_empty() {
        return 1;
    }

    let max_width = max_width as usize;
    let mut line_count = 1;
    let mut line_width = 0usize;
    let mut token = String::new();
    let mut token_is_whitespace = None;

    let flush_token = |token: &mut String,
                       token_is_whitespace: &mut Option<bool>,
                       line_count: &mut usize,
                       line_width: &mut usize| {
        let Some(is_whitespace) = *token_is_whitespace else {
            return;
        };

        if token.is_empty() {
            *token_is_whitespace = None;
            return;
        }

        if is_whitespace {
            for ch in token.chars() {
                let width = UnicodeWidthChar::width(ch).unwrap_or(0);
                if *line_width + width <= max_width {
                    *line_width += width;
                } else {
                    *line_count += 1;
                    *line_width = width;
                }
            }
        } else {
            let token_width: usize = token
                .chars()
                .map(|ch| UnicodeWidthChar::width(ch).unwrap_or(0))
                .sum();

            if token_width <= max_width {
                if *line_width > 0 && *line_width + token_width > max_width {
                    *line_count += 1;
                    *line_width = 0;
                }
                *line_width += token_width;
            } else {
                if *line_width > 0 {
                    *line_count += 1;
                    *line_width = 0;
                }
                for ch in token.chars() {
                    let width = UnicodeWidthChar::width(ch).unwrap_or(0);
                    if *line_width + width <= max_width {
                        *line_width += width;
                    } else {
                        *line_count += 1;
                        *line_width = width;
                    }
                }
            }
        }

        token.clear();
        *token_is_whitespace = None;
    };

    for ch in text.chars() {
        let is_whitespace = ch.is_whitespace();
        match token_is_whitespace {
            Some(current) if current != is_whitespace => {
                flush_token(
                    &mut token,
                    &mut token_is_whitespace,
                    &mut line_count,
                    &mut line_width,
                );
            }
            _ => {}
        }
        token_is_whitespace = Some(is_whitespace);
        token.push(ch);
    }

    flush_token(
        &mut token,
        &mut token_is_whitespace,
        &mut line_count,
        &mut line_width,
    );

    line_count
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::TuiArgs;
    use crate::exporter::TaskExportInfo;
    use crossterm::event::KeyModifiers;

    fn sample_args() -> TuiArgs {
        TuiArgs {
            tag: None,
            refresh_ms: 1000,
            limit: 100,
        }
    }

    fn sample_log(message: &str) -> QueryLogRow {
        QueryLogRow {
            id: 1,
            task_id: 42,
            tag: Some("fixture-api".into()),
            ts: "2026-03-21T12:00:00+08:00".into(),
            stream: "stderr".into(),
            level: "error".into(),
            message: message.into(),
            status: "failed".into(),
        }
    }

    #[test]
    fn scroll_logs_to_end_uses_wrapped_line_count() {
        let mut app = App::new(
            PathBuf::from("db.sqlite"),
            "db".into(),
            Config::default(),
            sample_args(),
        );
        app.logs = vec![sample_log(
            "connection timeout while syncing index and retrying against upstream service",
        )];
        app.log_wrap_width = 24;
        app.log_viewport_height = 3;

        app.scroll_logs_to_end();

        assert!(app.log_scroll > 0);
        assert_eq!(app.log_scroll, app.log_max_scroll());
    }

    #[test]
    fn down_key_scrolls_long_wrapped_log_even_when_only_one_row_exists() {
        let mut app = App::new(
            PathBuf::from("db.sqlite"),
            "db".into(),
            Config::default(),
            sample_args(),
        );
        app.logs = vec![sample_log(
            "connection timeout while syncing index and retrying against upstream service",
        )];
        app.log_wrap_width = 24;
        app.log_viewport_height = 3;
        app.focus = FocusPane::Logs;
        app.follow_logs = false;

        app.handle_key(KeyEvent::from(KeyCode::Down)).unwrap();

        assert_eq!(app.log_scroll, 1);
    }

    #[test]
    fn lineage_view_key_cycles_lineage_modes() {
        let mut app = App::new(
            PathBuf::from("db.sqlite"),
            "db".into(),
            Config::default(),
            sample_args(),
        );

        assert_eq!(app.lineage_filter.as_str(), "all");

        app.handle_key(KeyEvent::from(KeyCode::Char('v'))).unwrap();
        assert_eq!(app.lineage_filter.as_str(), "triggered");
        assert!(app.status_message.contains("triggered"));

        app.handle_key(KeyEvent::from(KeyCode::Char('v'))).unwrap();
        assert_eq!(app.lineage_filter.as_str(), "retry");
        assert!(app.status_message.contains("retry"));

        app.handle_key(KeyEvent::from(KeyCode::Char('v'))).unwrap();
        assert_eq!(app.lineage_filter.as_str(), "all");
        assert!(app.status_message.contains("all"));
    }

    #[test]
    fn ctrl_d_scrolls_detail_down_by_page() {
        let mut app = App::new(
            PathBuf::from("db.sqlite"),
            "db".into(),
            Config::default(),
            sample_args(),
        );
        app.detail = Some(TaskExportInfo {
            id: 7,
            tag: Some("demo".into()),
            command: "cargo test --workspace --all-features --locked".into(),
            command_json: Some("[\"cargo\",\"test\"]".into()),
            shell: Some("bash".into()),
            work_dir: "C:/very/long/work/dir/for/testing/detail/scroll".into(),
            started_at: "2026-03-21T12:00:00+08:00".into(),
            ended_at: Some("2026-03-21T12:01:00+08:00".into()),
            duration_ms: Some(60_000),
            pid: Some(1234),
            parent_task_id: Some(3),
            retry_of_task_id: Some(5),
            trigger_type: Some("retry".into()),
            exit_code: Some(1),
            status: "failed".into(),
            env_vars: Some("A=1 B=2 C=3 D=4 E=5 F=6 G=7".into()),
        });
        app.logs = vec![sample_log("compile failed")];
        app.update_viewport(Rect::new(0, 0, 120, 20));

        app.handle_key(KeyEvent::new(KeyCode::Char('d'), KeyModifiers::CONTROL))
            .unwrap();

        assert!(app.detail_scroll > 0);
    }

    #[test]
    fn ctrl_f_and_ctrl_b_jump_detail_to_bounds() {
        let mut app = App::new(
            PathBuf::from("db.sqlite"),
            "db".into(),
            Config::default(),
            sample_args(),
        );
        app.detail = Some(TaskExportInfo {
            id: 7,
            tag: Some("demo".into()),
            command: "cargo test --workspace --all-features --locked".into(),
            command_json: Some("[\"cargo\",\"test\"]".into()),
            shell: Some("bash".into()),
            work_dir: "C:/very/long/work/dir/for/testing/detail/scroll".into(),
            started_at: "2026-03-21T12:00:00+08:00".into(),
            ended_at: Some("2026-03-21T12:01:00+08:00".into()),
            duration_ms: Some(60_000),
            pid: Some(1234),
            parent_task_id: Some(3),
            retry_of_task_id: Some(5),
            trigger_type: Some("retry".into()),
            exit_code: Some(1),
            status: "failed".into(),
            env_vars: Some("A=1 B=2 C=3 D=4 E=5 F=6 G=7".into()),
        });
        app.logs = vec![sample_log("compile failed")];
        app.update_viewport(Rect::new(0, 0, 120, 20));

        app.handle_key(KeyEvent::new(KeyCode::Char('f'), KeyModifiers::CONTROL))
            .unwrap();
        let bottom_scroll = app.detail_scroll;
        assert!(bottom_scroll > 0);

        app.handle_key(KeyEvent::new(KeyCode::Char('b'), KeyModifiers::CONTROL))
            .unwrap();
        assert_eq!(app.detail_scroll, 0);
    }
}
