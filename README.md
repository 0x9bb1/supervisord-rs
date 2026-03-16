# supervisord-rs

Rust-based process supervisor with a local IPC control plane and supervisorctl-like CLI (minus XML-RPC/Web UI and INI config).

## Build

```bash
cargo build
```

## Install

```bash
cargo install --git https://github.com/0x9bb1/supervisord-rs.git
```

## Test

```bash
cargo test
```

## Run

```bash
# copy the example config and edit as needed
cp examples/supervisord.toml ./supervisord.toml

# foreground
supervisord-rs -c /path/to/supervisord.toml run

# control
supervisord-rs -c /path/to/supervisord.toml ctl status
```

If `-c` is omitted, the CLI searches for a config in this order:
1. `SUPERVISORD_RS_CONFIG` (when set)
2. `./supervisord.toml`
3. `./etc/supervisord.toml`
4. `/etc/supervisord.toml`
5. `/etc/supervisord-rs/supervisord.toml`
6. `/etc/supervisor/supervisord.toml`
7. `../etc/supervisord.toml` (relative to the executable)
8. `../supervisord.toml` (relative to the executable)

## Release

```bash
# create a tag like v0.1.0 and push it
git tag v0.1.0
git push origin v0.1.0
```
