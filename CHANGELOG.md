# Changelog

All notable changes to this project will be documented in this file.

## [0.1.9] - 2026-05-07

### Fixed

- Hardened concurrent task execution and SQLite access. Connections now use a shared setup with a busy timeout, foreign keys, WAL, and normal synchronous mode to reduce `database is locked` failures when background workers and readers overlap. Fixes #2.
- Avoided rebuilding the FTS index on every process startup; it now rebuilds only when the FTS table is first created.
- Mark stale `running` tasks as failed on startup after the configured stale window, with an explanatory task log entry.
- Installed the Ctrl-C handler once per process and routed interrupts through shared task state, avoiding stale per-task handler captures.

### Tests

- Added disk-backed concurrency coverage for configured SQLite connections, concurrent writers, and stale running task recovery.
