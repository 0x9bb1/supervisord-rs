# rvisor

Rust-based process supervisor with a local IPC control plane and a `rvisor ctl` CLI.

It is aimed at the same operational space as `supervisord`, but uses TOML config and a local Unix socket control API instead of XML-RPC and INI.

## Features

- Run a foreground or daemonized supervisor from the same binary.
- Start, stop, restart, reread, update, and reload managed programs through `rvisor ctl`.
- Stream process events and tail program logs over local IPC.
- Support `autostart`, `autorestart`, `startsecs`, `startretries`, `stopwaitsecs`, `numprocs`, environment injection, and log rotation.
- Install and manage a user service through `systemd --user` on Linux or `launchd` on macOS.

## Install

Build from source:

```bash
cargo build
```

Install the binary:

```bash
cargo install --git https://github.com/0x9bb1/rvisor.git
```

Run the test suite:

```bash
cargo test
```

## Quick Start

Generate or copy a config:

```bash
rvisor init -o supervisord.toml
# or
cp examples/supervisord.toml ./supervisord.toml
```

Start the supervisor in the foreground:

```bash
rvisor -c ./supervisord.toml run
```

Control it from another shell:

```bash
rvisor -c ./supervisord.toml ctl status
rvisor -c ./supervisord.toml ctl start example
rvisor -c ./supervisord.toml ctl tail example --follow
```

Run it as a daemon instead:

```bash
rvisor -c ./supervisord.toml --daemon run
```

## Example Config

Minimal example:

```toml
[supervisord]
sock_path = "/tmp/rvisor.sock"

[[programs]]
name = "example"
command = "sleep 60"
autostart = true
autorestart = "unexpected"
stdout_log = "/tmp/example.out.log"
stderr_log = "/tmp/example.err.log"
```

See [`examples/supervisord.toml`](/mnt/e/code/rvisor/examples/supervisord.toml) for a fuller example.

## Common Commands

Show process state:

```bash
rvisor ctl status
rvisor ctl status example
```

Control processes:

```bash
rvisor ctl start example
rvisor ctl stop example
rvisor ctl restart example
rvisor ctl signal TERM example
```

Apply config changes:

```bash
rvisor ctl reread
rvisor ctl update
rvisor ctl reload
```

Inspect logs and events:

```bash
rvisor ctl tail example
rvisor ctl tail example --follow
rvisor ctl logtail example --stderr --lines 100
rvisor ctl maintail --follow
rvisor ctl events
```

Useful output modes:

```bash
rvisor ctl status --json
rvisor ctl shell
rvisor version
```

## Config Resolution

If `-c` is omitted, `rvisor` searches for a config in this order:

1. `RVISOR_CONFIG` when set
2. `./supervisord.toml`
3. `./etc/supervisord.toml`
4. `/etc/supervisord.toml`
5. `/etc/rvisor/supervisord.toml`
6. `/etc/supervisor/supervisord.toml`
7. `../etc/supervisord.toml` relative to the executable
8. `../supervisord.toml` relative to the executable

## Service Management

Install a user service:

```bash
rvisor service install
rvisor service start
rvisor service status
```

The service subcommands use `systemd --user` on Linux and `launchd` on macOS.
