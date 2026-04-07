# logex

`logex` 是一个本地单用户的命令执行记录与日志排查工具。

它适合这些场景：

- 运行命令时持续采集 `stdout` / `stderr`
- 把任务与日志落到本地 SQLite，方便后续查询
- 用标签、时间、状态、grep 条件做排障
- 重试已有任务，并保留 retry / dependency 等 lineage 信息
- 导出单任务日志，尤其是 HTML 排障报告
- 在 TUI 中查看任务列表、详情、日志和基础统计

## 构建与运行

要求：

- Rust 稳定版
- 本地可执行命令运行环境

构建：

```bash
cargo build
```

查看帮助：

```bash
cargo run -- --help
```

发布构建：

```bash
cargo build --release
```

## 数据位置

程序默认会在用户目录下创建：

- 数据目录：`~/.logex/`
- 数据库：`~/.logex/logex.db`
- 配置文件：`~/.logex/config.toml`

首次运行会自动初始化数据库和默认配置。

## 命令概览

- `run`：执行命令并记录任务与日志
- `retry`：按已有任务重试，复用结构化命令元数据
- `query`：查询日志，支持 grep、context、follow
- `export`：导出日志为 `txt/json/csv/html`
- `list`：查看任务摘要
- `tags`：查看已有标签
- `analyze`：查看任务与日志统计
- `clear`：按条件清理数据
- `tui`：打开终端排障界面
- `seed`：生成测试数据

## 常用用法

### 运行任务

```bash
logex run -t demo -- cargo test --lib
```

指定工作目录：

```bash
logex run -t api -C ./examples/api -- cargo run
```

等待另一个任务完成后再执行，并记录 dependency lineage：

```bash
logex run -t deploy --wait 12 -- ./deploy.sh
```

加载环境文件和环境变量：

```bash
logex run -t web -e .env.dev -E RUST_LOG=debug -- cargo run
```

实时输出日志：

```bash
logex run -t demo --live -- cargo test
```

### 重试任务

按任务 ID 重试：

```bash
logex retry -i 42
```

重试并换一个新标签：

```bash
logex retry -i 42 -t retry-demo
```

### 查询日志

按标签查询：

```bash
logex query -t demo
```

按任务 ID 查询：

```bash
logex query -i 42 -o table
```

grep 错误，并带上下文：

```bash
logex query -g error -C 2
```

多个 grep 词都必须命中：

```bash
logex query -g timeout -g retry --grep-mode all
```

只在指定字段里 grep：

```bash
logex query -g failed --grep-fields message,status
```

持续 follow 新日志：

```bash
logex query -t demo -F -n 20 -p 500
```

### 导出日志

导出某个任务为 HTML 排障报告：

```bash
logex export -i 42 --format html -o exports/task-42.html
```

导出筛选后的 JSON：

```bash
logex export -t demo -g error --format json -o exports/demo-errors.json
```

### 查看任务和标签

```bash
logex list -t demo -l 20
logex tags -g demo -o table
```

### 统计分析

```bash
logex analyze -t demo
logex analyze -t demo --json --top-tags 10
```

### 清理数据

按标签清理：

```bash
logex clear -t demo
```

全量清理：

```bash
logex clear --all --yes
```

## TUI

打开终端界面：

```bash
logex tui
```

常用参数：

- `-t, --tag`：初始标签过滤
- `--refresh-ms`：刷新间隔
- `--limit`：任务列表上限

当前 TUI 支持：

- 任务列表与日志浏览
- 任务详情面板
- retry 确认与后台执行
- 导出当前任务
- 状态过滤
- 标签过滤
- lineage 视图切换：`all / triggered / retry`

进入 TUI 后可按 `?` 查看快捷键帮助。

## 时间格式

以下命令中的 `--from` / `--to` 支持：

- RFC3339，例如 `2026-03-01T12:30:00+08:00`
- `YYYY-MM-DD`
- `YYYY-MM-DD HH:MM[:SS]`

如果只传日期：

- `--from` 会自动补成当天 `00:00:00`
- `--to` 会自动补成当天 `23:59:59`

## 配置文件

默认配置文件位于 `~/.logex/config.toml`。

当前支持的默认项：

- `poll_ms`：follow 轮询间隔
- `tail`：follow 启动时先显示多少历史日志
- `batch_size`：日志批量写入条数
- `batch_timeout_secs`：日志批量写入超时秒数
- `auto_cleanup_days`：自动清理多少天前的数据，默认关闭

## 排障提示

- 如果命令执行失败，先用 `logex list` 找到任务 ID，再用 `query` / `export` 深入看
- 如果需要复盘单个失败任务，优先使用 HTML 导出
- 如果需要查看 retry / dependency 关系，优先使用 TUI 的 lineage 视图和任务详情
