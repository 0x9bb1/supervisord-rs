# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
cargo build                  # debug build
cargo build --release        # release build
cargo test                   # run all tests
cargo clippy                 # lint
cargo fmt                    # format
```

Run a single test:
```bash
cargo test <test_name>
```

## Architecture

supervisord-rs is a Unix process supervisor daemon written in Rust. It is a single binary that acts as both the daemon (`supervisord run`) and the control client (`supervisord ctl <command>`).

### IPC Model

The daemon exposes a **Unix Domain Socket** (default `/tmp/supervisord.sock`). The protocol is **JSON over a length-delimited framing** (via `tokio-util` `LengthDelimitedCodec`). The CLI subcommands in `main.rs` connect to this socket and send/receive JSON.

### Key Modules

| Module | Role |
|--------|------|
| `main.rs` | CLI entry point (clap), daemonization (double-fork), signal handling, async runtime setup |
| `supervisor.rs` | Core state machine — holds all `ProgramState` instances, drives STOPPED→STARTING→RUNNING→BACKOFF→FATAL transitions, handles commands from IPC |
| `ipc.rs` | Unix socket server (daemon side) and client helpers (CLI side); handles streaming for log-tail and event-feed commands; config file watching via `notify` |
| `process.rs` | Spawns child processes via Tokio, captures stdout/stderr, size-based log rotation |
| `config.rs` | TOML parsing into `SupervisordConfig` / `ProgramConfig`; config search path logic |
| `service.rs` | systemd service install/uninstall/start/stop (Linux only) |
| `logging.rs` | Initializes `tracing-subscriber` with `RUST_LOG` env filter (default: `info`) |

### Data Flow

```
CLI subcommand (main.rs)
  └─ connects to Unix socket (ipc.rs client helpers)
       └─ sends JSON command
            └─ supervisor.rs handles command
                 └─ process.rs spawns / kills processes
```

### Configuration Format

TOML (not INI like the original supervisord). Search order:
1. `SUPERVISORD_RS_CONFIG` env var
2. `./supervisord.toml`, `./etc/supervisord.toml`
3. `/etc/supervisord.toml`, `/etc/supervisord-rs/supervisord.toml`, `/etc/supervisor/supervisord.toml`
4. `../etc/supervisord.toml`, `../supervisord.toml` (relative to executable)

### Daemonization

`supervisord -d run` performs the classic double-fork before starting the Tokio runtime. The child writes its PID to `pidfile` and redirects stdio. This logic lives at the top of `main.rs`.

### IPC Protocol

`ipc::Request` fields: `command`, `program`, `lines`, `stream` (`stdout`/`stderr`), `follow`, `signal`, `offset`, `bytes`, `since`.
`ipc::Response` fields: `ok`, `message`, `data` (JSON value).

Available `ctl` commands: `start`, `stop`, `restart`, `status`, `signal`, `reread`, `update`, `reload`, `shutdown`, `pid`, `logtail`, `tail`, `maintail`, `events`, `avail`, `clear`, `fg`.

### Config File Watching

`ipc.rs` uses the `notify` crate to watch the config file for changes. On modification, it triggers an automatic `reread`+`update` cycle inside the daemon.

### Tests

Integration tests are in `tests/milestones.rs`. They start a real daemon process and communicate over the socket. A `tempfile`-backed config and socket path are used so tests are self-contained.

Run integration tests (they require a Unix environment):
```bash
cargo test --test milestones
```
