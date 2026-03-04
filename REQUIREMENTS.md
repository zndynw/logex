# logex 需求文档（评估版）

## 1. 文档目的

本文档用于：

1. 评估 `logex` 项目（Rust 二进制 + SQLite 日志存储）的可行性。
2. 明确第一阶段可实现范围（MVP）与验收标准。
3. 作为后续开发与需求确认的基线。

---

## 2. 项目目标

`logex` 是一个命令行工具，用于执行脚本/命令并管理运行日志。核心能力：

- 执行任务并采集日志（`run`）
- 按条件查询日志（`query`）
- 列表浏览已存储任务（`list`）
- 日志统计分析（`analyze`）
- 清理日志数据（`clear`）

示例：

```bash
logex run -t test -- bash script.sh
logex run -t test2 -- ls -lh
```

---

## 3. 可行性评估结论

## 3.1 总体结论

**可行，且适合 Rust 实现。**

原因：

- Rust 标准库可稳定支持子进程执行、标准输出/错误读取、退出码获取。
- SQLite 适合单机、轻量、结构化日志检索场景。
- CLI 子命令模式可通过成熟库快速构建（如 `clap`）。

## 3.2 前提条件

1. 明确日志等级判定规则（先按 `stdout/stderr` 区分，再自动细分 `error/warn/info/unknown`）。
2. 明确 `run` 的 Linux 命令执行策略（直接执行、是否依赖 shell）。
3. 明确日志保留与清理策略（通过 `~/.logex/config.toml` 统一配置）。

## 3.3 主要风险与应对

1. **风险：日志等级语义不一致**
   - 应对：MVP 先记录原始日志；等级可采用规则匹配（`error/warn/info`）并允许后续扩展。
2. **风险：超长日志导致存储/查询性能下降**
   - 应对：将日志拆分为明细表；建立必要索引；支持分页查询。
3. **风险：Linux 发行版环境差异（shell、路径、权限）**
   - 应对：MVP 先限定 Linux；命令参数解析使用 `--` 后透传策略；执行目录不存在或无权限时快速失败并报错。

---

## 4. 功能需求

## 4.1 `run`：执行并记录任务

### 输入

- 标签：`-t, --tag <TAG>`（可选，单值）
- 执行目录：`-C, --cwd <DIR>`（可选，默认当前目录）
- 命令：`--` 后为实际执行命令与参数

### 行为

1. 生成任务记录（`task_id`）。
2. 记录启动时间、执行目录、命令文本。
3. 执行命令并持续采集 stdout/stderr。
4. 将日志按行落库（含时间戳、流类型、等级）。其中流类型固定为 `stdout/stderr`，等级在此基础上自动细分。
5. 任务结束后更新结束时间、耗时、退出码、成功/失败状态。

### 输出

- 标准输出展示任务 ID 与最终状态（便于后续 `query/list/analyze` 使用）。

## 4.2 `query`：日志查询

### 支持过滤条件（MVP）

- `task_id`
- `tag`
- 时间范围（开始/结束）
- 日志等级（`error/warn/info/debug/unknown`）
- 状态（`success/failed`）

### 输出形式

- 默认返回日志明细（文本表格/行式输出）。
- 建议预留 `--json` 输出模式（便于脚本集成）。

### 时间语义

- `--from/--to` 使用 RFC3339 格式。
- 时区按服务器本地时区解释与展示。

## 4.3 `list`：任务列表

### 功能

- 展示已记录任务摘要：`task_id`、标签、命令、开始时间、结束时间、状态。
- 支持按标签/时间范围筛选。
- 支持分页（`--limit`、`--offset`）。

## 4.4 `analyze`：日志分析

### MVP 指标

- 指定标签或时间范围内：
  - `info/error/warn` 日志条数
  - 各等级占比
  - 成功/失败任务数量

### 输出

- 文本统计摘要（必要时可扩展 JSON）。

## 4.5 `clear`：数据清理

### 清理维度

- 按 `task_id`
- 按 `tag`
- 按时间范围
- 全量清理（必须二次确认或 `--yes`）

### 安全要求

- 默认拒绝高风险清理（例如无条件全量删除）。
- 清理后输出删除的任务数与日志数。

---

## 5. 数据模型（建议）

## 5.1 表结构（MVP）

1. `tasks`
   - `id`（主键，自增）
   - `tag`（文本，可空）
   - `command`（文本，原始命令）
   - `work_dir`（文本）
   - `started_at`（时间戳）
   - `ended_at`（时间戳，可空）
   - `duration_ms`（整数，可空）
   - `exit_code`（整数，可空）
   - `status`（文本：`running/success/failed`）

2. `task_logs`
   - `id`（主键，自增）
   - `task_id`（外键 -> `tasks.id`）
   - `ts`（时间戳）
   - `stream`（文本：`stdout/stderr`）
   - `level`（文本：`error/warn/info/debug/unknown`）
   - `message`（文本）

3. `app_config`（运行时配置来源，文件不入库，仅文档说明）
   - 路径：`~/.logex/config.toml`
   - 核心项：
     - `retention_days`（日志保留天数）
     - `cleanup_on_start`（启动时是否自动清理）
     - `max_tasks`（可选，按任务数上限清理）

4. SQLite 文件位置
   - 默认：`~/.logex/logex.db`
   - 目录不存在时自动创建 `~/.logex/`

## 5.2 索引建议

- `tasks(tag, started_at)`
- `tasks(status, started_at)`
- `task_logs(task_id, ts)`
- `task_logs(level, ts)`

---

## 6. 日志等级规则（MVP 建议）

1. 分层策略（MVP）：
   - 第一层（必选）：按来源流记录 `stdout/stderr`
   - 第二层（自动）：
     - 含关键字 `error` -> `error`
     - 含关键字 `warn` -> `warn`
     - 含关键字 `info` -> `info`
     - 其余 -> `unknown`
2. 后续可扩展：
   - 用户自定义正则规则
   - JSON 日志自动解析等级字段

---

## 7. 非功能需求

1. **可靠性**：任务异常退出时也要保证任务记录状态可追踪。
2. **性能**：单任务万级日志行可写入并可查询。
3. **可维护性**：子命令、存储层、分析层分层实现。
4. **可观测性**：关键操作（建库、清理、查询）应有明确错误信息。
5. **运行环境**：MVP 明确仅支持 Linux。
6. **时间一致性**：所有时间字段与查询参数统一使用 RFC3339，时区采用服务器本地时区。

---

## 8. CLI 交互草案

```bash
logex run -t <tag> [-C <dir>] -- <cmd> [args...]
logex query [--id <task_id>] [--tag <tag>] [--from <ts>] [--to <ts>] [--level <level>] [--status <status>] [--json]
logex list [--tag <tag>] [--from <ts>] [--to <ts>] [--limit <n>] [--offset <n>]
logex analyze [--tag <tag>] [--from <ts>] [--to <ts>] [--json]
logex clear [--id <task_id> | --tag <tag> | --from <ts> --to <ts> | --all] [--yes]
```

时间参数约定：`<ts>` 为 RFC3339 格式，按服务器本地时区处理。

配置与存储约定：

- 配置文件：`~/.logex/config.toml`
- SQLite：`~/.logex/logex.db`
- 配置来源：仅配置文件，不支持环境变量覆盖

---

## 9. MVP 范围与验收标准

## 9.1 MVP 范围

- 完成 5 个子命令基本能力。
- SQLite 本地存储（默认 `~/.logex/logex.db`）。
- 支持标签过滤、时间过滤、状态过滤。
- 支持分析中等级占比与成功失败统计。
- `run` 支持执行目录参数 `-C/--cwd`。
- 支持通过 `~/.logex/config.toml` 配置日志保留与清理策略。

## 9.2 验收标准

1. 能成功运行 `run` 执行命令并写入任务 + 日志（包含 `stdout/stderr` 流标记与自动细分等级）。
2. `query/list` 可按标签与时间筛选并返回正确记录。
3. `analyze` 能输出指定范围内等级占比与任务成功率。
4. `clear` 在安全确认后按条件删除，且结果可验证。
5. 程序首次运行可自动创建 `~/.logex/`，并使用其中配置与数据库文件。

---

## 10. 后续开发建议（分阶段）

1. **阶段 A：骨架搭建**
   - CLI 子命令定义
   - SQLite schema 初始化与迁移
2. **阶段 B：核心链路**
   - `run` 子进程执行与日志实时入库
   - `list/query` 查询能力
3. **阶段 C：分析与清理**
   - `analyze` 统计逻辑
   - `clear` 安全删除逻辑
4. **阶段 D：增强**
   - JSON 输出
   - 规则化等级识别
   - 更完整测试与 Linux 环境兼容性验证

---

## 11. 需求冻结结论（已确认）

1. 标签不支持多值（单任务单标签）。
2. `query` 默认返回日志明细。
3. 时间格式使用 RFC3339，时区使用服务器本地时区。
4. 暂不考虑 Linux 之外的平台。
5. 配置仅来自 `~/.logex/config.toml`，不支持环境变量覆盖。
