# logex Engineering Backlog

## Scope

This backlog is for engineering execution, not product ideation.

Project positioning is assumed to be fixed:

- `logex` is a local, single-user command execution tracking, log search, troubleshooting, and lightweight task tracing tool.
- The near-term goal is to improve maintainability, observability, and developer workflow quality.
- The near-term goal is not to turn `logex` into a distributed scheduler or a web platform.

## Priority Rules

- `P0`: unblock future development, reduce structural risk, or fix maintenance pain that will otherwise compound
- `P1`: materially improve daily usage and troubleshooting experience
- `P2`: important follow-up work, but not on the immediate critical path

## Current Status

- `P0-1`: substantially complete
- `P0-2`: substantially complete
- `P0-3`: substantially complete
- `P0-4`: substantially complete
- `P1-1`: substantially complete
- `P1-2`: first useful version complete
- `P1-3`: first useful version complete
- `P1-4`: partially complete
- `P2-1`: basic version complete, not yet full migration history
- `P2-3`: partially complete via rich `#[cfg(test)]` coverage, not yet moved into top-level `tests/`

## Recent Progress

- Extracted service modules from `handlers.rs` and kept `main.rs` focused on dispatch.
- Introduced shared internal filters and normalized time / grep handling.
- Added structured command metadata and retry replay based on stored command data.
- Added lineage fields and surfaced them in retry flows, export, CLI list, and TUI.
- Added a basic migration module with schema versioning and legacy upgrade handling.
- Fixed a real migration bug by rebuilding FTS after legacy upgrades so pre-existing logs remain searchable.
- Upgraded HTML export into a more useful troubleshooting report.
- Improved TUI with lineage views, retry-centric browsing, richer task detail, and log summaries.
- Reworked repository boundaries so task list, task detail, task replay source, task status, log queries, tag queries, and most analyzer reads now live in `store.rs`.
- Added higher-value behavior tests around retry, query FTS, export grouping, follow/tail initialization, and migration upgrade flows.

## P0

### P0-1 Split `handlers.rs` into focused service modules

**Goal**

Reduce coupling between CLI parsing, business logic, SQL assembly, and output behavior.

**Why now**

`src/handlers.rs` is already carrying too many responsibilities. If new commands or output modes keep landing there, future changes will get slower and riskier.

**Files**

- Modify: `src/handlers.rs`
- Create: `src/services/mod.rs`
- Create: `src/services/query_service.rs`
- Create: `src/services/export_service.rs`
- Create: `src/services/task_service.rs`
- Create: `src/services/tag_service.rs`
- Modify: `src/lib.rs`
- Modify: `src/main.rs`

**Execution notes**

- Keep `main.rs` focused on command dispatch.
- Move reusable business logic out of handlers first, then reduce handlers to thin adapters.
- Avoid mixing SQL building and rendering logic in the same function.

**Acceptance**

- Existing command behavior remains unchanged.
- `handlers.rs` becomes a thin CLI bridge or disappears entirely.
- New service boundaries are clear enough that future commands can reuse them.
- `cargo test --lib` passes.

### P0-2 Introduce a unified filter/query model

**Goal**

Stop duplicating filtering rules across `query`, `export`, `list`, `tags`, and `analyze`.

**Why now**

The project already has multiple command paths that interpret the same concepts: tag, time range, status, grep, match mode, and output mode. Repeating those rules will keep causing drift.

**Files**

- Create: `src/filters.rs` or `src/query_model.rs`
- Modify: `src/cli.rs`
- Modify: `src/store.rs`
- Modify: `src/services/query_service.rs`
- Modify: `src/services/export_service.rs`
- Modify: `src/services/tag_service.rs`
- Modify: `src/services/task_service.rs`

**Execution notes**

- Separate CLI args from internal filter structs.
- Normalize time handling and grep handling once.
- Keep command-specific rendering concerns outside the shared filter layer.

**Acceptance**

- `query` and `export` share the same filtering semantics.
- New searchable fields can be added in one place.
- Time normalization and grep interpretation are no longer reimplemented in multiple functions.

### P0-3 Store task metadata in structured form

**Goal**

Make task replay, export, analysis, and future lineage features depend on structured metadata instead of reconstructed strings.

**Why now**

Today the project still leans too heavily on stringified command state. That is workable for MVP use, but it will become fragile for retry, chain analysis, and richer export.

**Files**

- Modify: `src/db.rs`
- Modify: `src/executor.rs`
- Modify: `src/store.rs`
- Modify: `src/exporter.rs`
- Modify: `src/error.rs`

**Suggested fields**

- `command_text`
- `command_json`
- `shell`
- `pid`

**Execution notes**

- Preserve backward compatibility for existing databases.
- Prefer additive migration over schema rewrite.
- Keep `command_text` for display, but stop treating it as the source of truth.

**Acceptance**

- New tasks persist structured execution metadata.
- Existing rows remain readable.
- Retry path prefers structured command data when present.
- Export can surface the structured data without reparsing shell strings.

### P0-4 Fix README/config documentation drift and reduce encoding ambiguity

**Goal**

Align README/config docs with the actual CLI and config behavior, and avoid confusion caused by terminal/code-page display mismatches.

**Why now**

The current repository files are valid UTF-8, but some inspection paths can still display mojibake when UTF-8 text is rendered through the wrong code page. Separately, README and config comments have already drifted from the implemented command surface. That damages usability and slows future maintenance.

**Files**

- Modify: `ReadME.md`
- Modify: `src/config.rs`

**Execution notes**

- Keep repository text in clean UTF-8 and verify with byte-level reads when needed.
- Do not treat terminal mojibake alone as proof that file contents are broken.
- Make examples match current command flags, defaults, and current feature surface.
- Consider normalizing `ReadME.md` to `README.md`.
- Keep docs concise and operational.

**Acceptance**

- README content is readable when opened as UTF-8 text.
- Default config comments are readable and consistent with runtime behavior.
- CLI docs match current code behavior.
- Backlog/workflow no longer assumes mojibake implies broken source-file encoding.

## P1

### P1-1 Add task lineage fields

**Goal**

Track relationships between tasks so retries and dependency-driven execution become visible in data, exports, and UI.

**Files**

- Modify: `src/db.rs`
- Modify: `src/executor.rs`
- Modify: `src/store.rs`
- Modify: `src/services/task_service.rs`
- Modify: `src/exporter.rs`
- Modify: `src/tui/app.rs`
- Modify: `src/tui/draw.rs`

**Suggested fields**

- `parent_task_id`
- `retry_of_task_id`
- `trigger_type`

**Execution notes**

- `retry` should create an explicit relation to the original task.
- Dependency-triggered runs should record the upstream task when applicable.
- Keep the first version minimal and data-first.

**Acceptance**

- Lineage metadata is stored for new tasks.
- Retry relationships are visible from CLI and export.
- TUI can display at least the immediate relationship fields.

### P1-2 Upgrade HTML export into a troubleshooting report

**Goal**

Turn HTML export from a log dump into a useful single-task incident report.

**Files**

- Modify: `src/exporter.rs`
- Modify: `src/store.rs`
- Modify: `src/formatter.rs`

**Suggested sections**

- task summary
- environment summary
- execution result
- log statistics
- highlighted errors and warnings
- lineage summary when available

**Execution notes**

- Keep output self-contained.
- Do not depend on remote assets.
- Prefer readability over styling complexity.

**Acceptance**

- A single-task HTML export is useful without extra tooling.
- Key execution metadata is visible at a glance.
- Rendering remains covered by tests.

### P1-3 Increase TUI troubleshooting value

**Goal**

Make the TUI good enough that common investigation flow stays inside it.

**Files**

- Modify: `src/tui/app.rs`
- Modify: `src/tui/draw.rs`
- Modify: `src/tui/mod.rs`

**Suggested improvements**

- richer task detail panel
- visible exit code and env summary
- lineage hints
- quick help overlay
- preset filters for errors/running/failed

**Execution notes**

- Favor dense but readable information.
- Keep controls discoverable.
- Avoid adding visual complexity without real navigation benefit.

**Acceptance**

- Common troubleshooting flow needs fewer CLI round-trips.
- Help and shortcuts are visible in-app.
- Detail pane exposes enough task context to reduce export-only investigation.

### P1-4 Fully unify grep/search semantics across commands

**Goal**

Make search behavior consistent and testable across query surfaces.

**Files**

- Modify: `src/cli.rs`
- Modify: `src/services/query_service.rs`
- Modify: `src/services/export_service.rs`
- Modify: `src/formatter.rs`

**Execution notes**

- Align `any/all`, `case_sensitive`, `invert_match`, and field restriction logic.
- Keep text highlighting independent from matching semantics.
- Avoid hidden special cases except explicit FTS optimization.

**Acceptance**

- `query` and `export` produce matching filter results for the same criteria.
- Search rules are defined in one place.
- Tests cover the main combinations.

## P2

### P2-1 Add schema versioning and proper migrations

**Goal**

Replace ad hoc schema evolution with a repeatable migration path.

**Files**

- Modify: `src/db.rs`
- Create: `src/migrations.rs`

**Execution notes**

- Add explicit schema version tracking.
- Keep migration steps idempotent.
- Make future additive changes safe.

**Acceptance**

- Schema upgrades are versioned and repeatable.
- Existing databases upgrade cleanly.

### P2-2 Expand configurable defaults

**Goal**

Move more runtime behavior from hardcoded defaults into explicit configuration.

**Files**

- Modify: `src/config.rs`
- Modify: `src/cli.rs`
- Modify: relevant service modules

**Suggested config areas**

- default output modes
- default export directory
- TUI refresh and list size defaults
- cleanup policy

**Acceptance**

- Config precedence is clear.
- New config fields are documented.
- Runtime behavior remains predictable when config is absent.

### P2-3 Add integration tests for core user flows

**Goal**

Raise confidence from unit-level correctness to end-to-end workflow correctness.

**Files**

- Create: `tests/run_query_export.rs`
- Create: `tests/retry_flow.rs`
- Create: `tests/migrations.rs`

**Execution notes**

- Prefer realistic command flow over excessive mocking.
- Cover the paths most likely to regress during refactors.
- Keep tests deterministic and local-only.

**Acceptance**

- Core flows are covered by integration tests.
- Refactors in service and storage layers can be validated quickly.

### P2-4 Normalize build and release workflow

**Goal**

Turn the current build script into a stable release process.

**Files**

- Modify: `build.ps1`
- Modify: `build.bat`
- Modify: `ReadME.md`
- Optional: CI workflow files

**Execution notes**

- Define supported targets explicitly.
- Standardize output names and packaging expectations.
- Keep local and release build paths understandable.

**Acceptance**

- Release build steps are documented and repeatable.
- Supported targets are explicit.
- Local contributors can build without guessing the intended flow.

## Suggested Execution Order

1. `P0-1` Split handlers into service modules
2. `P0-2` Introduce unified filter/query model
3. `P0-3` Store task metadata in structured form
4. `P0-4` Fix README and config encoding/documentation drift
5. `P1-1` Add task lineage fields
6. `P1-2` Upgrade HTML export into a troubleshooting report
7. `P1-3` Increase TUI troubleshooting value
8. `P1-4` Fully unify grep/search semantics across commands
9. `P2-1` Add schema versioning and proper migrations
10. `P2-3` Add integration tests for core user flows
11. `P2-2` Expand configurable defaults
12. `P2-4` Normalize build and release workflow

## Immediate Next Slice

If only one engineering slice should start now, start here:

1. Finish `P1-4` by tightening any remaining search/grep semantic drift and documenting the final rules clearly.
2. Decide whether `P2-3` should stay as rich in-module tests or be promoted into top-level integration tests under `tests/`.
3. Upgrade `P2-1` from basic schema versioning to a true ordered migration history when the next schema change lands.
