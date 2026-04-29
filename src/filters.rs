use crate::Result;
use crate::cli::{
    AnalyzeArgs, ClearArgs, ExportArgs, ListArgs, QueryArgs, QueryMatchMode, QuerySearchField,
    TagsArgs,
};
use crate::formatter::QueryLogRow;
use crate::utils::normalize_time_arg;

#[derive(Debug, Clone, Default)]
pub struct NormalizedTimeRange {
    pub from: Option<String>,
    pub to: Option<String>,
}

impl NormalizedTimeRange {
    pub fn new(from: Option<&str>, to: Option<&str>) -> Result<Self> {
        Ok(Self {
            from: normalize_time_arg(from, false)?,
            to: normalize_time_arg(to, true)?,
        })
    }

    pub fn from_query_args(args: &QueryArgs) -> Result<Self> {
        Self::new(args.from.as_deref(), args.to.as_deref())
    }

    pub fn from_export_args(args: &ExportArgs) -> Result<Self> {
        Self::new(args.from.as_deref(), args.to.as_deref())
    }

    pub fn from_list_args(args: &ListArgs) -> Result<Self> {
        Self::new(args.from.as_deref(), args.to.as_deref())
    }

    pub fn from_tags_args(args: &TagsArgs) -> Result<Self> {
        Self::new(args.from.as_deref(), args.to.as_deref())
    }

    pub fn from_analyze_args(args: &AnalyzeArgs) -> Result<Self> {
        Self::new(args.from.as_deref(), args.to.as_deref())
    }

    pub fn from_clear_args(args: &ClearArgs) -> Result<Self> {
        Self::new(args.from.as_deref(), args.to.as_deref())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Pagination {
    pub limit: i64,
    pub offset: i64,
}

impl Pagination {
    pub fn new(limit: i64, offset: i64) -> Self {
        Self { limit, offset }
    }
}

#[derive(Debug, Clone, Default)]
pub struct LogRowQuery {
    pub task_id: Option<i64>,
    pub tag: Option<String>,
    pub level: Option<String>,
    pub status: Option<String>,
    pub time_range: NormalizedTimeRange,
}

impl LogRowQuery {
    pub fn from_query_args(args: &QueryArgs) -> Result<Self> {
        Ok(Self {
            task_id: args.task_id,
            tag: args.tag.clone(),
            level: args.level.clone(),
            status: args.status.clone(),
            time_range: NormalizedTimeRange::from_query_args(args)?,
        })
    }

    pub fn from_export_args(args: &ExportArgs) -> Result<Self> {
        Ok(Self {
            task_id: args.task_id,
            tag: args.tag.clone(),
            level: args.level.clone(),
            status: args.status.clone(),
            time_range: NormalizedTimeRange::from_export_args(args)?,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct TaskListFilter {
    pub tag: Option<String>,
    pub time_range: NormalizedTimeRange,
    pub limit: i64,
    pub offset: i64,
}

impl TaskListFilter {
    pub fn from_list_args(args: &ListArgs) -> Result<Self> {
        Ok(Self {
            tag: args.tag.clone(),
            time_range: NormalizedTimeRange::from_list_args(args)?,
            limit: args.limit,
            offset: args.offset,
        })
    }
}

#[derive(Debug, Clone)]
pub struct TagListFilter {
    pub grep: Option<String>,
    pub time_range: NormalizedTimeRange,
    pub pagination: Pagination,
}

impl TagListFilter {
    pub fn from_tags_args(args: &TagsArgs) -> Result<Self> {
        Ok(Self {
            grep: args.grep.clone(),
            time_range: NormalizedTimeRange::from_tags_args(args)?,
            pagination: Pagination::new(args.limit, args.offset),
        })
    }
}

#[derive(Debug, Clone)]
pub struct ClearTaskFilter {
    pub task_id: Option<i64>,
    pub tag: Option<String>,
    pub time_range: NormalizedTimeRange,
}

impl ClearTaskFilter {
    pub fn from_clear_args(args: &ClearArgs) -> Result<Self> {
        Ok(Self {
            task_id: args.task_id,
            tag: args.tag.clone(),
            time_range: NormalizedTimeRange::from_clear_args(args)?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct AnalysisRequest {
    pub tag: Option<String>,
    pub time_range: NormalizedTimeRange,
    pub top_tags: usize,
}

impl AnalysisRequest {
    pub fn from_analyze_args(args: &AnalyzeArgs) -> Result<Self> {
        Ok(Self {
            tag: args.tag.clone(),
            time_range: NormalizedTimeRange::from_analyze_args(args)?,
            top_tags: args.top_tags,
        })
    }
}

#[derive(Debug, Clone)]
pub struct LogSearchFilter {
    patterns: Vec<String>,
    mode: QueryMatchMode,
    fields: Vec<QuerySearchField>,
    case_sensitive: bool,
    invert_match: bool,
}

impl LogSearchFilter {
    pub fn from_query_args(args: &QueryArgs) -> Self {
        Self::new(
            &args.grep,
            args.grep_mode,
            &args.grep_fields,
            args.case_sensitive,
            args.invert_match,
        )
    }

    pub fn from_export_args(args: &ExportArgs) -> Self {
        Self::new(
            &args.grep,
            args.grep_mode,
            &args.grep_fields,
            args.case_sensitive,
            args.invert_match,
        )
    }

    fn new(
        patterns: &[String],
        mode: QueryMatchMode,
        fields: &[QuerySearchField],
        case_sensitive: bool,
        invert_match: bool,
    ) -> Self {
        let normalized_patterns = patterns
            .iter()
            .map(|pattern| {
                if case_sensitive {
                    pattern.clone()
                } else {
                    pattern.to_lowercase()
                }
            })
            .collect();

        let fields = if fields.is_empty() {
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
            fields.to_vec()
        };

        Self {
            patterns: normalized_patterns,
            mode,
            fields,
            case_sensitive,
            invert_match,
        }
    }

    pub fn is_match(&self, row: &QueryLogRow) -> bool {
        if self.patterns.is_empty() {
            return !self.invert_match;
        }

        let matches_pattern = |pattern: &str| {
            self.fields
                .iter()
                .any(|field| field_matches(row, *field, pattern, self.case_sensitive))
        };

        let matched = match self.mode {
            QueryMatchMode::Any => self.patterns.iter().any(|pattern| matches_pattern(pattern)),
            QueryMatchMode::All => self.patterns.iter().all(|pattern| matches_pattern(pattern)),
        };

        if self.invert_match { !matched } else { matched }
    }

    pub fn first_pattern(&self) -> Option<&str> {
        self.patterns.first().map(String::as_str)
    }
}

pub fn matches_query_row(row: &QueryLogRow, filter: &LogSearchFilter) -> bool {
    filter.is_match(row)
}

fn contains_pattern(value: &str, pattern: &str, case_sensitive: bool) -> bool {
    if case_sensitive {
        value.contains(pattern)
    } else {
        value.to_lowercase().contains(pattern)
    }
}

fn field_matches(
    row: &QueryLogRow,
    field: QuerySearchField,
    pattern: &str,
    case_sensitive: bool,
) -> bool {
    match field {
        QuerySearchField::Message => contains_pattern(&row.message, pattern, case_sensitive),
        QuerySearchField::Level => contains_pattern(&row.level, pattern, case_sensitive),
        QuerySearchField::Stream => contains_pattern(&row.stream, pattern, case_sensitive),
        QuerySearchField::Status => contains_pattern(&row.status, pattern, case_sensitive),
        QuerySearchField::Tag => {
            contains_pattern(row.tag.as_deref().unwrap_or(""), pattern, case_sensitive)
        }
        QuerySearchField::TaskId => {
            contains_pattern(&row.task_id.to_string(), pattern, case_sensitive)
        }
        QuerySearchField::Timestamp => contains_pattern(&row.ts, pattern, case_sensitive),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_row() -> QueryLogRow {
        QueryLogRow {
            id: 1,
            task_id: 42,
            tag: Some("deploy-prod".into()),
            ts: "2026-03-21T12:00:00+08:00".into(),
            stream: "stderr".into(),
            level: "error".into(),
            message: "connection timeout before retry".into(),
            status: "failed".into(),
        }
    }

    #[test]
    fn selected_fields_limit_matching_scope() {
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
            grep_mode: QueryMatchMode::Any,
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

        let filter = LogSearchFilter::from_query_args(&args);
        assert!(filter.is_match(&sample_row()));

        let mut wrong_field_row = sample_row();
        wrong_field_row.tag = Some("prod".into());
        wrong_field_row.message = "deploy failed".into();
        assert!(!filter.is_match(&wrong_field_row));
    }

    #[test]
    fn all_mode_requires_every_pattern() {
        let args = QueryArgs {
            task_id: None,
            tag: None,
            from: None,
            to: None,
            level: None,
            status: None,
            view: crate::cli::QueryView::Detail,
            output: crate::cli::QueryOutput::Table,
            grep: vec!["timeout".into(), "retry".into()],
            grep_mode: QueryMatchMode::All,
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

        let filter = LogSearchFilter::from_query_args(&args);
        assert!(filter.is_match(&sample_row()));

        let mut partial = sample_row();
        partial.message = "connection timeout".into();
        assert!(!filter.is_match(&partial));
    }

    #[test]
    fn task_list_filter_normalizes_time_and_pagination() {
        let args = ListArgs {
            tag: Some("demo".into()),
            from: Some("2026-03-21".into()),
            to: Some("2026-03-22".into()),
            output: crate::cli::ListOutput::Table,
            limit: 25,
            offset: 10,
        };

        let filter = TaskListFilter::from_list_args(&args).unwrap();
        assert_eq!(filter.tag.as_deref(), Some("demo"));
        assert_eq!(filter.limit, 25);
        assert_eq!(filter.offset, 10);
        assert!(
            filter
                .time_range
                .from
                .as_deref()
                .unwrap()
                .contains("2026-03-21")
        );
        assert!(
            filter
                .time_range
                .to
                .as_deref()
                .unwrap()
                .contains("2026-03-22")
        );
    }

    #[test]
    fn clear_filter_preserves_task_identity_and_time_range() {
        let args = ClearArgs {
            task_id: Some(7),
            tag: Some("demo".into()),
            from: Some("2026-03-21 10:00".into()),
            to: Some("2026-03-21 11:00".into()),
            all: false,
            yes: true,
            vacuum: false,
        };

        let filter = ClearTaskFilter::from_clear_args(&args).unwrap();
        assert_eq!(filter.task_id, Some(7));
        assert_eq!(filter.tag.as_deref(), Some("demo"));
        assert!(
            filter
                .time_range
                .from
                .as_deref()
                .unwrap()
                .contains("2026-03-21T10:00:00")
        );
        assert!(
            filter
                .time_range
                .to
                .as_deref()
                .unwrap()
                .contains("2026-03-21T11:00:00")
        );
    }

    #[test]
    fn analysis_request_maps_tag_range_and_top_tags() {
        let args = AnalyzeArgs {
            tag: Some("ops".into()),
            from: Some("2026-03-20".into()),
            to: Some("2026-03-21".into()),
            json: true,
            top_tags: 3,
        };

        let request = AnalysisRequest::from_analyze_args(&args).unwrap();
        assert_eq!(request.tag.as_deref(), Some("ops"));
        assert_eq!(request.top_tags, 3);
        assert!(
            request
                .time_range
                .from
                .as_deref()
                .unwrap()
                .contains("2026-03-20")
        );
        assert!(
            request
                .time_range
                .to
                .as_deref()
                .unwrap()
                .contains("2026-03-21")
        );
    }
}
