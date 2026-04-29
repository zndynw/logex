use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

pub const DISPLAY_VERSION: &str = concat!(
    env!("CARGO_PKG_VERSION"),
    " (built ",
    env!("LOGEX_BUILD_DATE"),
    ")"
);

#[derive(Debug, Parser)]
#[command(name = "logex")]
#[command(
    version = DISPLAY_VERSION,
    about = "Run commands and manage task logs",
    long_about = None
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    #[command(about = "Run a command and record task logs")]
    Run(RunArgs),
    #[command(hide = true)]
    RunWorker(RunWorkerArgs),
    #[command(about = "Generate sample tasks and logs for testing")]
    Seed(SeedArgs),
    #[command(about = "Open the interactive terminal dashboard")]
    Tui(TuiArgs),
    #[command(about = "Query logs with filters, grep-like search, and follow mode")]
    Query(QueryArgs),
    #[command(about = "Export logs to txt, json, csv, or html")]
    Export(ExportArgs),
    #[command(about = "List task summaries")]
    List(ListArgs),
    #[command(about = "List existing tags")]
    Tags(TagsArgs),
    #[command(about = "Analyze log levels and task results")]
    Analyze(AnalyzeArgs),
    #[command(about = "Clear tasks and logs by filters")]
    Clear(ClearArgs),
    #[command(about = "Run SQLite VACUUM to reclaim database file space")]
    Vacuum,
    #[command(about = "Retry an existing task")]
    Retry(RetryArgs),
}

#[derive(Debug, clap::Args)]
pub struct RunArgs {
    #[arg(short, long, help = "Task tag")]
    pub tag: Option<String>,
    #[arg(short = 'C', long, help = "Working directory for the command")]
    pub cwd: Option<PathBuf>,
    #[arg(long, help = "Print logs live to the terminal")]
    pub live: bool,
    #[arg(
        long,
        help = "Submit the task and continue execution in a detached worker",
        conflicts_with = "live"
    )]
    pub background: bool,
    #[arg(
        short = 'w',
        long = "wait",
        help = "Wait for the given task ID to finish before running"
    )]
    pub wait_for: Option<i64>,
    #[arg(
        short = 'e',
        long = "env-file",
        help = "Source environment files before running"
    )]
    pub env_files: Vec<PathBuf>,
    #[arg(
        short = 'E',
        long = "env",
        help = "Set environment variables (KEY=VALUE)"
    )]
    pub env_vars: Vec<String>,
    #[arg(
        required = true,
        trailing_var_arg = true,
        help = "Command and arguments to execute"
    )]
    pub command: Vec<String>,
}

#[derive(Debug, clap::Args)]
pub struct RunWorkerArgs {
    #[arg(long)]
    pub task_id: i64,
    #[arg(short = 'w', long = "wait")]
    pub wait_for: Option<i64>,
    #[arg(short = 'C', long)]
    pub cwd: Option<PathBuf>,
    #[arg(long)]
    pub live: bool,
    #[arg(short = 'e', long = "env-file")]
    pub env_files: Vec<PathBuf>,
    #[arg(short = 'E', long = "env")]
    pub env_vars: Vec<String>,
    #[arg(required = true, trailing_var_arg = true)]
    pub command: Vec<String>,
}

#[derive(Debug, clap::Args)]
pub struct SeedArgs {
    #[arg(long, default_value_t = 12, help = "Number of tasks to generate")]
    pub tasks: usize,
    #[arg(long, default_value_t = 8, help = "Base number of logs per task")]
    pub logs_per_task: usize,
    #[arg(
        long,
        default_value = "sample",
        help = "Tag prefix for generated tasks"
    )]
    pub tag_prefix: String,
}

#[derive(Debug, clap::Args)]
pub struct TuiArgs {
    #[arg(short = 't', long, help = "Initial tag filter")]
    pub tag: Option<String>,
    #[arg(
        long,
        default_value_t = 1000,
        help = "Refresh interval in milliseconds"
    )]
    pub refresh_ms: u64,
    #[arg(long, default_value_t = 100, help = "Maximum tasks shown in the list")]
    pub limit: i64,
}

#[derive(Debug, clap::Args)]
pub struct QueryArgs {
    #[arg(short = 'i', long = "id", help = "Filter by task ID")]
    pub task_id: Option<i64>,
    #[arg(short = 't', long, help = "Filter by tag")]
    pub tag: Option<String>,
    #[arg(
        short = 'f',
        long,
        help = "Start time: RFC3339 or YYYY-MM-DD[ HH:MM[:SS]]"
    )]
    pub from: Option<String>,
    #[arg(
        short = 'T',
        long,
        help = "End time: RFC3339 or YYYY-MM-DD[ HH:MM[:SS]]"
    )]
    pub to: Option<String>,
    #[arg(short = 'l', long, help = "Filter by level: error|info|warn|unknown")]
    pub level: Option<String>,
    #[arg(
        short = 's',
        long,
        help = "Filter by task status: running|success|failed"
    )]
    pub status: Option<String>,
    #[arg(short = 'v', long, value_enum, default_value_t = QueryView::Detail, help = "View mode")]
    pub view: QueryView,
    #[arg(short = 'o', long, value_enum, default_value_t = QueryOutput::Table, help = "Output format")]
    pub output: QueryOutput,
    #[arg(
        short = 'g',
        long,
        help = "Search keyword. Can be provided multiple times"
    )]
    pub grep: Vec<String>,
    #[arg(long, value_enum, default_value_t = QueryMatchMode::Any, help = "How multiple grep terms should match")]
    pub grep_mode: QueryMatchMode,
    #[arg(
        long,
        value_enum,
        value_delimiter = ',',
        help = "Restrict grep to specific fields"
    )]
    pub grep_fields: Vec<QuerySearchField>,
    #[arg(long, help = "Use case-sensitive grep matching")]
    pub case_sensitive: bool,
    #[arg(long, help = "Invert grep matches")]
    pub invert_match: bool,
    #[arg(long, help = "Disable ANSI highlight in text output")]
    pub no_highlight: bool,
    #[arg(
        short = 'A',
        long,
        default_value_t = 0,
        help = "Lines of trailing context"
    )]
    pub after_context: usize,
    #[arg(
        short = 'B',
        long,
        default_value_t = 0,
        help = "Lines of leading context"
    )]
    pub before_context: usize,
    #[arg(short = 'C', long, help = "Set both before and after context to N")]
    pub context: Option<usize>,
    #[arg(short = 'F', long, help = "Follow and print new logs continuously")]
    pub follow: bool,
    #[arg(
        short = 'n',
        long,
        default_value_t = 10,
        help = "Show the last N lines before follow starts"
    )]
    pub tail: usize,
    #[arg(
        short = 'p',
        long,
        default_value_t = 500,
        help = "Follow poll interval in milliseconds"
    )]
    pub poll_ms: u64,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum QueryView {
    Detail,
    Summary,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum QueryOutput {
    Plain,
    Table,
    Json,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum QueryMatchMode {
    Any,
    All,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum QuerySearchField {
    Message,
    Level,
    Stream,
    Status,
    Tag,
    TaskId,
    Timestamp,
}

#[derive(Debug, clap::Args)]
pub struct ExportArgs {
    #[arg(
        short = 'i',
        long = "id",
        help = "Filter by task ID; exports the full task log when used alone"
    )]
    pub task_id: Option<i64>,
    #[arg(short = 't', long, help = "Filter by tag")]
    pub tag: Option<String>,
    #[arg(
        short = 'f',
        long,
        help = "Start time: RFC3339 or YYYY-MM-DD[ HH:MM[:SS]]"
    )]
    pub from: Option<String>,
    #[arg(
        short = 'T',
        long,
        help = "End time: RFC3339 or YYYY-MM-DD[ HH:MM[:SS]]"
    )]
    pub to: Option<String>,
    #[arg(short = 'l', long, help = "Filter by level: error|info|warn|unknown")]
    pub level: Option<String>,
    #[arg(
        short = 's',
        long,
        help = "Filter by task status: running|success|failed"
    )]
    pub status: Option<String>,
    #[arg(
        short = 'g',
        long,
        help = "Search keyword. Can be provided multiple times"
    )]
    pub grep: Vec<String>,
    #[arg(long, value_enum, default_value_t = QueryMatchMode::Any, help = "How multiple grep terms should match")]
    pub grep_mode: QueryMatchMode,
    #[arg(
        long,
        value_enum,
        value_delimiter = ',',
        help = "Restrict grep to specific fields"
    )]
    pub grep_fields: Vec<QuerySearchField>,
    #[arg(long, help = "Use case-sensitive grep matching")]
    pub case_sensitive: bool,
    #[arg(long, help = "Invert grep matches")]
    pub invert_match: bool,
    #[arg(long, value_enum, default_value_t = ExportFormat::Txt, help = "Export file format")]
    pub format: ExportFormat,
    #[arg(short = 'o', long, help = "Output file path")]
    pub output: PathBuf,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum ExportFormat {
    Txt,
    Json,
    Csv,
    Html,
}

#[derive(Debug, clap::Args)]
pub struct ListArgs {
    #[arg(short = 't', long, help = "Filter by tag")]
    pub tag: Option<String>,
    #[arg(
        short = 'f',
        long,
        help = "Start time: RFC3339 or YYYY-MM-DD[ HH:MM[:SS]]"
    )]
    pub from: Option<String>,
    #[arg(
        short = 'T',
        long,
        help = "End time: RFC3339 or YYYY-MM-DD[ HH:MM[:SS]]"
    )]
    pub to: Option<String>,
    #[arg(short = 'o', long, value_enum, default_value_t = ListOutput::Table, help = "Output format")]
    pub output: ListOutput,
    #[arg(short = 'l', long, default_value_t = 50, help = "Limit")]
    pub limit: i64,
    #[arg(short = 'O', long, default_value_t = 0, help = "Offset")]
    pub offset: i64,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum ListOutput {
    Plain,
    Table,
}

#[derive(Debug, clap::Args)]
pub struct TagsArgs {
    #[arg(
        short = 'f',
        long,
        help = "Start time: RFC3339 or YYYY-MM-DD[ HH:MM[:SS]]"
    )]
    pub from: Option<String>,
    #[arg(
        short = 'T',
        long,
        help = "End time: RFC3339 or YYYY-MM-DD[ HH:MM[:SS]]"
    )]
    pub to: Option<String>,
    #[arg(short = 'g', long, help = "Search tag keyword")]
    pub grep: Option<String>,
    #[arg(short = 'o', long, value_enum, default_value_t = TagsOutput::Table, help = "Output format")]
    pub output: TagsOutput,
    #[arg(short = 'l', long, default_value_t = 50, help = "Limit")]
    pub limit: i64,
    #[arg(short = 'O', long, default_value_t = 0, help = "Offset")]
    pub offset: i64,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum TagsOutput {
    Plain,
    Table,
    Json,
}

#[derive(Debug, clap::Args)]
pub struct AnalyzeArgs {
    #[arg(short = 't', long, help = "Filter by tag")]
    pub tag: Option<String>,
    #[arg(
        short = 'f',
        long,
        help = "Start time: RFC3339 or YYYY-MM-DD[ HH:MM[:SS]]"
    )]
    pub from: Option<String>,
    #[arg(
        short = 'T',
        long,
        help = "End time: RFC3339 or YYYY-MM-DD[ HH:MM[:SS]]"
    )]
    pub to: Option<String>,
    #[arg(short = 'j', long, help = "Output as JSON")]
    pub json: bool,
    #[arg(
        long,
        default_value_t = 5,
        help = "Number of top tags to include; 0 disables tag breakdown"
    )]
    pub top_tags: usize,
}

#[derive(Debug, clap::Args)]
pub struct ClearArgs {
    #[arg(short = 'i', long = "id", help = "Clear by task ID")]
    pub task_id: Option<i64>,
    #[arg(short = 't', long, help = "Clear by tag")]
    pub tag: Option<String>,
    #[arg(
        short = 'f',
        long,
        help = "Start time: RFC3339 or YYYY-MM-DD[ HH:MM[:SS]]"
    )]
    pub from: Option<String>,
    #[arg(
        short = 'T',
        long,
        help = "End time: RFC3339 or YYYY-MM-DD[ HH:MM[:SS]]"
    )]
    pub to: Option<String>,
    #[arg(short = 'a', long, help = "Clear everything, requires --yes")]
    pub all: bool,
    #[arg(short = 'y', long, help = "Confirm destructive clear")]
    pub yes: bool,
    #[arg(
        long,
        help = "Run SQLite VACUUM after clearing to return free pages to the OS"
    )]
    pub vacuum: bool,
}

#[derive(Debug, clap::Args)]
pub struct RetryArgs {
    #[arg(short = 'i', long = "id", help = "Task ID to retry")]
    pub task_id: i64,
    #[arg(short, long, help = "Optional new tag for the retried task")]
    pub tag: Option<String>,
    #[arg(long, help = "Print logs live to the terminal")]
    pub live: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_background_conflicts_with_live() {
        let parsed = Cli::try_parse_from([
            "logex",
            "run",
            "--background",
            "--live",
            "--",
            "echo",
            "hello",
        ]);

        let err = parsed.expect_err("expected clap conflict");
        assert!(err.to_string().contains("cannot be used with"));
    }

    #[test]
    fn display_version_includes_package_version_and_build_date() {
        assert!(DISPLAY_VERSION.contains(env!("CARGO_PKG_VERSION")));
        assert!(DISPLAY_VERSION.contains("(built "));
        assert!(DISPLAY_VERSION.ends_with(')'));
    }

    #[test]
    fn parses_clear_vacuum_flag() {
        let parsed = Cli::try_parse_from(["logex", "clear", "--all", "--yes", "--vacuum"])
            .expect("clear --vacuum should parse");

        let Command::Clear(args) = parsed.command else {
            panic!("expected clear command");
        };
        assert!(args.all);
        assert!(args.yes);
        assert!(args.vacuum);
    }

    #[test]
    fn parses_vacuum_command() {
        let parsed = Cli::try_parse_from(["logex", "vacuum"]).expect("vacuum should parse");

        assert!(matches!(parsed.command, Command::Vacuum));
    }
}
