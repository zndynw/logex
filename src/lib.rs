//! Public entry points for embedding or testing `logex`.
//!
//! Most modules are currently public so the binary target and existing integration
//! tests can keep using their established paths. Treat those module-level exports
//! as implementation detail unless they are also re-exported from [`prelude`] or
//! from the crate root.
//!
//! Supported library entry points are:
//! - crate-root error and status types: [`Result`], [`LogexError`], [`LogLevel`],
//!   and [`TaskStatus`]
//! - [`prelude`], a curated facade for core task, query, and export flows
//! - specific module APIs already used by integration tests, until a narrower
//!   facade replaces those direct paths
//!
//! This boundary is intentionally additive for now; module visibility has not
//! been narrowed because `src/main.rs` is a separate crate target and integration
//! tests import several module paths directly.

pub mod analyzer;
pub mod cli;
pub mod config;
pub mod db;
pub mod error;
pub mod executor;
pub mod exporter;
pub mod filters;
pub mod formatter;
pub mod handlers;
pub mod migrations;
pub mod seeder;
pub mod services;
pub mod store;
pub mod tui;
pub mod utils;

pub use error::{LogLevel, LogexError, Result, TaskStatus};

/// Curated facade for the library's supported core flows.
///
/// Prefer importing from this module in new library consumers and tests. The
/// lower-level modules remain public for compatibility but are not all stable
/// API surfaces.
pub mod prelude {
    pub use crate::Result;
    pub use crate::cli::{ExportFormat, RunArgs};
    pub use crate::config::Config;
    pub use crate::executor::{
        TaskOrigin, TriggerType, get_task_info, run_task, run_task_with_origin,
    };
    pub use crate::exporter::render_export;
    pub use crate::filters::{LogRowQuery, NormalizedTimeRange};
    pub use crate::migrations::migrate;
    pub use crate::store::{
        LineageFilter, TaskListFilter, fetch_log_rows, fetch_task_detail, fetch_task_list,
        fetch_task_logs,
    };
}

#[cfg(test)]
mod tests {
    #[test]
    fn prelude_exposes_supported_core_flow_types() {
        use crate::prelude::{
            Config, ExportFormat, LineageFilter, LogRowQuery, NormalizedTimeRange, Result, RunArgs,
            TaskListFilter,
        };

        let _: Result<()> = Ok(());
        let _ = Config::default();
        let _ = ExportFormat::Json;
        let _ = LineageFilter::All;
        let _ = LogRowQuery::default();
        let _ = NormalizedTimeRange::default();
        let _ = TaskListFilter::default();
        let _ = RunArgs {
            tag: None,
            cwd: None,
            live: false,
            background: false,
            wait_for: None,
            env_files: Vec::new(),
            env_vars: Vec::new(),
            command: vec!["true".to_string()],
        };
    }
}
