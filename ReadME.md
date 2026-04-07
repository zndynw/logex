# logex 使用文档

`logex` 是一个基于 Rust 的命令行日志执行与管理工具，支持执行命令、记录日志、查询任务、统计分析和清理数据。

## 1. 功能概览

- `run`：执行命令并实时采集 stdout/stderr 日志写入 SQLite
- `query`：按任务/标签/时间/等级/状态查询日志，支持 follow、grep、上下文
- `list`：查看任务摘要列表
- `tags`：列出已有 tag（去重）
- `analyze`：统计日志等级占比与任务成功/失败数量
- `clear`：按条件清理任务及日志，支持安全确认

## 2. 环境与依赖

- Rust（建议稳定版）
- SQLite（项目使用 `rusqlite` + `bundled`，默认可直接构建）
- MVP 目标运行环境：Linux

## 3. 构建与运行

在项目根目录执行：

```bash
cargo build
cargo run -- --help
```

发布构建：

```bash
cargo build --release
./target/release/logex --help
```

## 4. 数据与存储位置

- 数据目录：`~/.logex/`
- 数据库文件：`~/.logex/logex.db`

首次运行会自动创建目录与表结构。

## 5. 命令使用

### 5.1 run：执行命令并记录日志

```bash
logex run -t <tag> [-C <dir>] -- <cmd> [args...]
```

常用参数：

- `-t, --tag`：任务标签（可选）
- `-C, --cwd`：执行目录（可选，默认当前目录）
- `--` 后为实际命令与参数

示例：

```bash
logex run -t test -- bash script.sh
logex run -t ls-demo -C /tmp -- ls -lah
```

执行完成后会输出：`task_id=<id> status=<success|failed>`。

### 5.2 query：查询日志

```bash
logex query [options]
```

常用参数：

- `-i, --id`：按任务 ID 过滤
- `-t, --tag`：按标签过滤
- `-f, --from`：起始时间
- `-T, --to`：结束时间
- `-l, --level`：日志等级过滤（`error|info|warn|unknown`）
- `-s, --status`：任务状态过滤（`success|failed`）
- `-v, --view`：视图（`detail|summary`，默认 `detail`）
- `-o, --output`：输出格式（`plain|table|json`，默认 `table`）
- `-g, --grep`：关键词匹配（匹配 message/level/stream/status/tag）
- `-A, --after-context`：匹配后上下文行数
- `-B, --before-context`：匹配前上下文行数
- `-C, --context`：同时设置前后上下文
- `-F, --follow`：持续轮询输出新增日志
- `-n, --tail`：follow 启动时先显示最后 N 行（默认 10）
- `-p, --poll-ms`：follow 轮询间隔毫秒（默认 500）

示例：

```bash
logex query -t test
logex query -i 12 -o table
logex query -g error -C 2
logex query -t test -F -n 20
logex query -v summary -o json
```

### 5.3 list：查看任务摘要

```bash
logex list [options]
```

常用参数：

- `-t, --tag`：按标签过滤
- `-f, --from`：起始时间
- `-T, --to`：结束时间
- `-o, --output`：输出格式（`plain|table`，默认 `table`）
- `-l, --limit`：返回条数（默认 50）
- `-O, --offset`：分页偏移（默认 0）

示例：

```bash
logex list
logex list -t test -l 20 -O 0
```

### 5.4 analyze：日志统计

```bash
logex analyze [options]
```

常用参数：

- `-t, --tag`：按标签过滤
- `-f, --from`：起始时间
- `-T, --to`：结束时间
- `-j, --json`：JSON 输出（当前为占位实现）

示例：

```bash
logex analyze
logex analyze -t test -f "2026-03-01" -T "2026-03-02"
```

### 5.5 tags：列出已有 tag（去重）

```bash
logex tags [options]
```

常用参数：

- `-f, --from`：起始时间
- `-T, --to`：结束时间
- `-g, --grep`：关键词匹配（匹配 tag）
- `-o, --output`：输出格式（`plain|table|json`，默认 `table`）
- `-l, --limit`：返回条数（默认 50）
- `-O, --offset`：分页偏移（默认 0）

示例：

```bash
logex tags
logex tags -g demo
logex tags -o json
```

### 5.6 clear：清理任务与日志

```bash
logex clear [options]
```

常用参数：

- `-i, --id`：按任务 ID 清理
- `-t, --tag`：按标签清理
- `-f, --from`：起始时间
- `-T, --to`：结束时间
- `-a, --all`：全量清理（必须搭配 `--yes`）
- `-y, --yes`：高风险操作确认

示例：

```bash
logex clear -t test
logex clear -f "2026-03-01" -T "2026-03-02"
logex clear --all --yes
```

安全策略：

- 未提供任何过滤条件时会拒绝清理
- `--all` 未带 `--yes` 会拒绝执行

## 6. 时间格式说明

支持以下输入格式：

- RFC3339（例如 `2026-03-01T12:30:00+08:00`）
- `YYYY-MM-DD`（日期）
- `YYYY-MM-DD HH:MM[:SS]`

当输入仅日期时：

- `--from` 会自动补为当天 `00:00:00`
- `--to` 会自动补为当天 `23:59:59`

## 7. 快速上手示例

```bash
# 1) 执行一个命令并记录日志
logex run -t demo -- bash -c "echo hello && echo oops 1>&2"

# 2) 查看任务列表
logex list -t demo

# 2.1) 列出已有 tag（去重）
logex tags -o table

# 3) 查询该标签日志
logex query -t demo -o table

# 4) 统计分析
logex analyze -t demo

# 5) 清理 demo 数据
logex clear -t demo
```

## 8. 常见问题

- 命令找不到：请确认通过 `cargo run -- ...` 或已将可执行文件加入 `PATH`
- 执行目录报错：`-C` 指定目录必须存在且为目录
- 看不到日志：可先用 `logex list` 确认任务存在，再用 `query` 过滤对应 `id/tag`
