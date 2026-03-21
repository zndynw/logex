use crate::cli::SeedArgs;
use crate::error::{LogLevel, Result, TaskStatus};
use chrono::{Duration, Local};
use rusqlite::{Connection, params};

#[derive(Debug, Clone, Default)]
pub struct SeedSummary {
    pub tasks_inserted: usize,
    pub logs_inserted: usize,
    pub running_tasks: usize,
    pub success_tasks: usize,
    pub failed_tasks: usize,
}

pub fn seed_sample_data(conn: &mut Connection, args: &SeedArgs) -> Result<SeedSummary> {
    let tx = conn.transaction()?;
    let now = Local::now();
    let commands = [
        "cargo build",
        "cargo test",
        "cargo run -- analyze",
        "cargo run -- export --format json --output exports/demo.json",
        "powershell -Command Get-Process",
        "python -m pytest -q",
    ];
    let work_dirs = [
        "C:/workspace/service-a",
        "C:/workspace/service-b",
        "C:/workspace/cli",
        "C:/workspace/jobs",
    ];
    let tag_suffixes = ["api", "worker", "export", "db"];
    let info_messages = [
        "starting task",
        "loading config",
        "connecting to sqlite",
        "processing batch",
        "writing output artifact",
        "task completed",
    ];
    let warn_messages = [
        "retrying after transient failure",
        "slow query detected",
        "falling back to cached value",
        "high memory usage observed",
    ];
    let error_messages = [
        "connection timeout while syncing index",
        "permission denied when opening output",
        "unexpected panic in worker pool",
        "database locked during write",
    ];

    let mut summary = SeedSummary::default();

    for index in 0..args.tasks {
        let task_number = index + 1;
        let status = match index % 5 {
            0 => TaskStatus::Failed,
            1 | 2 => TaskStatus::Success,
            _ => TaskStatus::Running,
        };

        let started_at = now - Duration::minutes((task_number as i64) * 17);
        let ended_at = match status {
            TaskStatus::Running => None,
            TaskStatus::Success => {
                Some(started_at + Duration::seconds(40 + (index % 7) as i64 * 25))
            }
            TaskStatus::Failed => {
                Some(started_at + Duration::seconds(15 + (index % 5) as i64 * 18))
            }
        };
        let duration_ms = ended_at.map(|ended| (ended - started_at).num_milliseconds());
        let exit_code = match status {
            TaskStatus::Success => Some(0),
            TaskStatus::Failed => Some((index % 3 + 1) as i32),
            TaskStatus::Running => None,
        };
        let tag = format!(
            "{}-{}",
            args.tag_prefix,
            tag_suffixes[index % tag_suffixes.len()]
        );

        tx.execute(
            "INSERT INTO tasks(tag, command, work_dir, started_at, ended_at, duration_ms, exit_code, status) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                tag,
                commands[index % commands.len()],
                work_dirs[index % work_dirs.len()],
                started_at.to_rfc3339(),
                ended_at.map(|value| value.to_rfc3339()),
                duration_ms,
                exit_code,
                status.as_str()
            ],
        )?;

        let task_id = tx.last_insert_rowid();
        let log_count = args.logs_per_task + (index % 4);

        for log_index in 0..log_count {
            let offset_secs = (log_index as i64) * 4;
            let ts = started_at + Duration::seconds(offset_secs);

            let (stream, level, message) =
                if matches!(status, TaskStatus::Failed) && log_index + 1 == log_count {
                    (
                        "stderr",
                        LogLevel::Error,
                        error_messages[index % error_messages.len()],
                    )
                } else if log_index > 0 && log_index % 5 == 0 {
                    (
                        "stdout",
                        LogLevel::Warn,
                        warn_messages[(index + log_index) % warn_messages.len()],
                    )
                } else if matches!(status, TaskStatus::Running) && log_index + 1 == log_count {
                    ("stdout", LogLevel::Info, "task still running")
                } else {
                    let stream = if log_index % 6 == 0 {
                        "stderr"
                    } else {
                        "stdout"
                    };
                    let level = if stream == "stderr" {
                        LogLevel::Error
                    } else {
                        LogLevel::Info
                    };
                    let message = if stream == "stderr" {
                        error_messages[(index + log_index) % error_messages.len()]
                    } else {
                        info_messages[(index + log_index) % info_messages.len()]
                    };
                    (stream, level, message)
                };

            tx.execute(
                "INSERT INTO task_logs(task_id, ts, stream, level, message) VALUES(?1, ?2, ?3, ?4, ?5)",
                params![task_id, ts.to_rfc3339(), stream, level.as_str(), format!("{} [{}:{}]", message, tag, log_index + 1)],
            )?;
            summary.logs_inserted += 1;
        }

        summary.tasks_inserted += 1;
        match status {
            TaskStatus::Running => summary.running_tasks += 1,
            TaskStatus::Success => summary.success_tasks += 1,
            TaskStatus::Failed => summary.failed_tasks += 1,
        }
    }

    tx.commit()?;
    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;

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
        conn
    }

    #[test]
    fn seed_inserts_tasks_and_logs() {
        let mut conn = setup_conn();
        let summary = seed_sample_data(
            &mut conn,
            &SeedArgs {
                tasks: 6,
                logs_per_task: 4,
                tag_prefix: "fixture".into(),
            },
        )
        .unwrap();

        let task_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM tasks", [], |row| row.get(0))
            .unwrap();
        let log_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM task_logs", [], |row| row.get(0))
            .unwrap();
        let distinct_tags: i64 = conn
            .query_row(
                "SELECT COUNT(DISTINCT tag) FROM tasks WHERE tag LIKE 'fixture-%'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(task_count, 6);
        assert_eq!(log_count as usize, summary.logs_inserted);
        assert!(summary.success_tasks > 0);
        assert!(summary.failed_tasks > 0);
        assert!(summary.running_tasks > 0);
        assert!(distinct_tags >= 3);
    }
}
