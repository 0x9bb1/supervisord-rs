# Rust 重写设计文档（第一版）

## 目标与范围
- 重写 `supervisord`（同一二进制内包含控制与服务管理子命令），仅提供 CLI（不做 Web UI 与 XML-RPC）。
- 支持平台：Linux 与 macOS。
- 配置格式采用 TOML，不要求兼容现有 `supervisord.conf`。
- 单进程守护足够：`supervisord` 作为唯一后台进程，运行期控制通过本地 IPC 通信。

## 核心架构
- `supervisord` 负责启动/停止/重启子进程、维护进程状态、采集输出、处理信号。
- `supervisord` 暴露本地 IPC 控制端点（Unix Domain Socket；macOS 同样支持）。
- 同一二进制的 CLI 子命令通过 IPC 与守护进程通信，命令尽量兼容现有 `supervisorctl`。
- 进程管理模型为 `Program`（配置项） -> `Process`（实际进程实例，支持多副本）。

## 模块划分（Rust crate 内部）
- `config`：TOML 解析与校验，加载默认值。
- `ipc`：UDS 监听与请求/响应协议（JSON/Msgpack 均可）。
- `supervisor`：核心调度与状态机。
- `process`：spawn、signal、pid 管理。
- `logging`：守护日志、子进程 stdout/stderr 处理。
- `cli`：`supervisord` 的子命令与参数解析。

## 配置（TOML 草案）
- 配置文件为 `supervisord.toml`。
- `supervisord` 节包含 `logfile`, `pidfile`, `sock_path`, `umask`, `minfds`。
- `programs` 为数组，每项含 `name`, `command`, `cwd`, `autostart`, `autorestart`, `numprocs`, `environment`, `stdout_log`, `stderr_log`。
- 行为偏好：配置变更需显式 `reread`/`update`。

## CLI 设计
- 目标：尽量兼容现有 `supervisorctl` 习惯，降低迁移成本。
- 顶层用法：`supervisord [OPTIONS] <command>`。
- 全局选项：`-c/--configuration`，`-d/--daemon`，`--env-file`。
- 默认前台运行：`supervisord -c /path/to/supervisord.toml`。
- 后台运行：`supervisord -c /path/to/supervisord.toml -d` 或 `--daemon`。
- 可用子命令：`ctl`、`init`、`service`、`version`。
- `ctl` 子命令尽量对齐 `supervisorctl`：`status`、`start`、`stop`、`restart`、`reload`、`shutdown`、`signal`、`pid`、`logtail`。
- `ctl` 可选参数：`-s/--serverurl`，`-u/--user`，`-P/--password`，`-v/--verbose`（保留兼容，即便默认本地 UDS）。
- `init` 用于输出配置模板，参数：`-o/--output` 指定输出文件。
- `service` 提供服务管理：`install`、`uninstall`、`start`、`stop`（目标为本机服务管理器，如 `systemd`/`launchd`）。
- 输出格式默认人类可读；可加 `--json` 便于脚本解析。

## 状态与恢复
- 状态机：`STOPPED` → `STARTING` → `RUNNING` → `BACKOFF` → `FATAL`
- 守护进程重启后可从 `pidfile` 与历史日志恢复最小状态，避免重复启动。

## 可观测性
- 守护日志文件：事件、命令、崩溃堆栈。
- 子进程日志：支持文件轮转（按大小/数量）。
- `supervisord status --json` 用于集成监控。

## 测试策略
- 单元测试：配置解析、状态机、命令处理。
- 集成测试：真实 spawn 子进程、信号处理、IPC 通讯。
- 平台测试：Linux 与 macOS CI 两套。

## 里程碑
1. 里程碑 A：配置解析 + 进程 spawn + 基础 IPC + `status/start/stop`
2. 里程碑 B：`restart/reread/update/tail` + 日志轮转
3. 里程碑 C：兼容更多 supervisor 行为（autorestart/backoff 细节）

## 实施优先级（最小闭环）
1. `config`：最小 TOML（`supervisord` + `programs`，字段先覆盖 `name`、`command`、`autostart`）。
2. `cli`：`supervisord run` + `supervisord ctl status/start/stop`。
3. `ipc`：UDS 请求/响应，先只支持 `status/start/stop`。
4. `process`：spawn/stop + 基础状态机。
5. `logging`：stdout/stderr 重定向到文件（先不做轮转）。
6. `test`：最小集成测试（启动 `sleep` 或 `cat`，验证 `status` 变化）。

## 依赖与协议选择
- 异步运行时：`tokio`。
- CLI：`clap`。
- 配置解析：`serde` + `toml`。
- IPC：UDS + JSON，使用长度前缀协议（`tokio-util` + `bytes`）。
- 错误处理：`anyhow`。
- 日志：`tracing` + `tracing-subscriber`。
- 进程/信号：`nix`（Linux/macOS 统一）。
