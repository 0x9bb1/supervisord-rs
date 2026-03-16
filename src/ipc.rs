use crate::supervisor;
use anyhow::Context;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Mutex;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use futures::{SinkExt, StreamExt};
use notify::{Config as NotifyConfig, EventKind, RecommendedWatcher, RecursiveMode, Watcher};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Request {
    pub command: String,
    pub program: Option<String>,
    #[serde(default)]
    pub lines: Option<usize>,
    #[serde(default)]
    pub stream: Option<String>,
    #[serde(default)]
    pub follow: Option<bool>,
    #[serde(default)]
    pub signal: Option<String>,
    #[serde(default)]
    pub offset: Option<u64>,
    #[serde(default)]
    pub bytes: Option<u64>,
    #[serde(default)]
    pub since: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub ok: bool,
    pub message: String,
    pub data: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LogTailReply {
    pub lines: Vec<String>,
    pub offset: u64,
}

pub async fn run_server(
    sock_path: &Path,
    supervisor: Arc<Mutex<supervisor::Supervisor>>,
    allowed_uids: Vec<u32>,
) -> anyhow::Result<()> {
    if let Some(parent) = sock_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create socket dir {}", parent.display()))?;
    }
    let listener = match UnixListener::bind(sock_path) {
        Ok(l) => l,
        Err(_) if sock_path.exists() => {
            // If we can connect, a live instance is already running.
            if UnixStream::connect(sock_path).await.is_ok() {
                anyhow::bail!(
                    "another instance is already running (socket {})",
                    sock_path.display()
                );
            }
            // Stale socket file — remove and rebind.
            std::fs::remove_file(sock_path)
                .with_context(|| format!("remove stale socket {}", sock_path.display()))?;
            UnixListener::bind(sock_path)
                .with_context(|| format!("bind socket {}", sock_path.display()))?
        }
        Err(e) => return Err(e).with_context(|| format!("bind socket {}", sock_path.display())),
    };
    tracing::info!("listening on {}", sock_path.display());
    let allowed_uids = Arc::new(allowed_uids);

    loop {
        let (stream, _) = listener.accept().await?;
        let supervisor = supervisor.clone();
        let allowed = allowed_uids.clone();
        tokio::spawn(async move {
            // Check peer credentials when an allow-list is configured.
            #[cfg(unix)]
            if !allowed.is_empty() {
                let authorized = stream
                    .peer_cred()
                    .map(|cred| allowed.contains(&cred.uid()))
                    .unwrap_or(false);
                if !authorized {
                    // Send a structured error so the client gets ok=false
                    // rather than an abrupt connection close.
                    let _ = send_error_response(stream, "connection refused: unauthorized uid").await;
                    return;
                }
            }
            if let Err(err) = handle_stream(stream, supervisor).await {
                tracing::warn!("ipc error: {err}");
            }
        });
    }
}

async fn send_error_response(stream: tokio::net::UnixStream, message: &str) -> anyhow::Result<()> {
    let response = Response {
        ok: false,
        message: message.to_string(),
        data: None,
    };
    let payload = serde_json::to_vec(&response)?;
    let mut framed = Framed::new(stream, LengthDelimitedCodec::new());
    framed.send(Bytes::from(payload)).await?;
    Ok(())
}

pub async fn send_request(sock_path: &Path, request: Request) -> anyhow::Result<Response> {
    let stream = UnixStream::connect(sock_path)
        .await
        .with_context(|| format!("connect to {}", sock_path.display()))?;
    let mut framed = Framed::new(stream, LengthDelimitedCodec::new());
    let payload = serde_json::to_vec(&request)?;
    framed.send(Bytes::from(payload)).await?;
    let response = framed
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("no response"))??;
    let response: Response = serde_json::from_slice(&response)?;
    Ok(response)
}

pub async fn send_stream_request(
    sock_path: &Path,
    request: Request,
) -> anyhow::Result<Framed<UnixStream, LengthDelimitedCodec>> {
    let stream = UnixStream::connect(sock_path)
        .await
        .with_context(|| format!("connect to {}", sock_path.display()))?;
    let mut framed = Framed::new(stream, LengthDelimitedCodec::new());
    let payload = serde_json::to_vec(&request)?;
    framed.send(Bytes::from(payload)).await?;
    Ok(framed)
}

async fn handle_stream(
    stream: UnixStream,
    supervisor: Arc<Mutex<supervisor::Supervisor>>,
) -> anyhow::Result<()> {
    let mut framed = Framed::new(stream, LengthDelimitedCodec::new());
    while let Some(frame) = framed.next().await {
        let frame = frame?;
        let request: Request = serde_json::from_slice(&frame)?;
        if request.command == "logtail" && request.follow == Some(true) {
            handle_logtail_stream(&mut framed, request, supervisor.clone()).await?;
            break;
        } else if request.command == "events" {
            handle_events_stream(&mut framed, supervisor.clone()).await?;
            break;
        } else {
            let response = handle_request(request, supervisor.clone()).await;
            let payload = serde_json::to_vec(&response)?;
            framed.send(Bytes::from(payload)).await?;
        }
    }
    Ok(())
}

async fn handle_request(
    request: Request,
    supervisor: Arc<Mutex<supervisor::Supervisor>>,
) -> Response {
    match request.command.as_str() {
        "status" => {
            let guard = supervisor.lock().await;
            let statuses = guard.status(request.program.as_deref());
            Response {
                ok: true,
                message: "ok".to_string(),
                data: serde_json::to_value(statuses).ok(),
            }
        }
        "start" => {
            let result = if let Some(name) = request.program {
                supervisor::start_program(supervisor, &name).await
            } else {
                supervisor::start_all(supervisor).await
            };
            result_to_response(result)
        }
        "restart" => {
            let result = if let Some(name) = request.program {
                supervisor::restart_program(supervisor, &name).await
            } else {
                match supervisor::stop_all(supervisor.clone()).await {
                    Ok(_) => supervisor::start_all(supervisor).await,
                    Err(err) => Err(err),
                }
            };
            result_to_response(result)
        }
        "stop" => {
            let result = if let Some(name) = request.program {
                supervisor::stop_program(supervisor, &name).await
            } else {
                supervisor::stop_all(supervisor).await
            };
            result_to_response(result)
        }
        "reread" => match supervisor::reread(supervisor).await {
            Ok(summary) => Response {
                ok: true,
                message: "ok".to_string(),
                data: serde_json::to_value(summary).ok(),
            },
            Err(err) => Response {
                ok: false,
                message: err.to_string(),
                data: None,
            },
        },
        "update" => match supervisor::update(supervisor).await {
            Ok(summary) => Response {
                ok: true,
                message: "ok".to_string(),
                data: serde_json::to_value(summary).ok(),
            },
            Err(err) => Response {
                ok: false,
                message: err.to_string(),
                data: None,
            },
        },
        "reload" => match supervisor::reload(supervisor).await {
            Ok(summary) => Response {
                ok: true,
                message: "ok".to_string(),
                data: serde_json::to_value(summary).ok(),
            },
            Err(err) => Response {
                ok: false,
                message: err.to_string(),
                data: None,
            },
        },
        "pid" => Response {
            ok: true,
            message: "ok".to_string(),
            data: serde_json::to_value(std::process::id()).ok(),
        },
        "signal" => {
            let Some(signal) = request.signal else {
                return Response {
                    ok: false,
                    message: "signal is required".to_string(),
                    data: None,
                };
            };
            let result = if let Some(name) = request.program {
                supervisor::signal_program(supervisor, &name, &signal).await
            } else {
                supervisor::signal_all(supervisor, &signal).await
            };
            result_to_response(result)
        }
        "shutdown" => {
            let supervisor_clone = supervisor.clone();
            tokio::spawn(async move {
                let _ = supervisor::shutdown(supervisor_clone).await;
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                std::process::exit(0);
            });
            Response {
                ok: true,
                message: "shutdown".to_string(),
                data: None,
            }
        }
        "logtail" => {
            let Some(name) = request.program else {
                return Response {
                    ok: false,
                    message: "program is required".to_string(),
                    data: None,
                };
            };
            let lines = request.lines.unwrap_or(10);
            let stream = request.stream.unwrap_or_else(|| "stdout".to_string());
            let offset = request.offset.unwrap_or(0);
            let bytes = request.bytes;
            let since = request.since;
            let log_path = {
                let guard = supervisor.lock().await;
                let Some(config) = guard.program_config(&name) else {
                    return Response {
                        ok: false,
                        message: format!("unknown program {name}"),
                        data: None,
                    };
                };
                match stream.as_str() {
                    "stderr" => config.stderr_log.clone(),
                    _ => config.stdout_log.clone(),
                }
            };
            match log_path {
                Some(path) => match read_tail_lines(&path, lines, offset, bytes, since).await {
                    Ok(reply) => Response {
                        ok: true,
                        message: "ok".to_string(),
                        data: serde_json::to_value(reply).ok(),
                    },
                    Err(err) => Response {
                        ok: false,
                        message: err.to_string(),
                        data: None,
                    },
                },
                None => Response {
                    ok: false,
                    message: "log file is not configured".to_string(),
                    data: None,
                },
            }
        }
        "maintail" => {
            let lines = request.lines.unwrap_or(10);
            let offset = request.offset.unwrap_or(0);
            let bytes = request.bytes;
            let since = request.since;
            let log_path = {
                let guard = supervisor.lock().await;
                guard.logfile().cloned()
            };
            match log_path {
                Some(path) => match read_tail_lines(&path, lines, offset, bytes, since).await {
                    Ok(reply) => Response {
                        ok: true,
                        message: "ok".to_string(),
                        data: serde_json::to_value(reply).ok(),
                    },
                    Err(err) => Response {
                        ok: false,
                        message: err.to_string(),
                        data: None,
                    },
                },
                None => Response {
                    ok: false,
                    message: "supervisord logfile is not configured".to_string(),
                    data: None,
                },
            }
        }
        "avail" => {
            let names = supervisor::avail(supervisor).await;
            Response {
                ok: true,
                message: "ok".to_string(),
                data: serde_json::to_value(names).ok(),
            }
        }
        "clear" => {
            let result = supervisor::clear_logs(supervisor, request.program.as_deref()).await;
            result_to_response(result)
        }
        "add" => {
            let Some(name) = request.program else {
                return Response {
                    ok: false,
                    message: "program is required".to_string(),
                    data: None,
                };
            };
            let result = supervisor::add_program(supervisor, &name).await;
            result_to_response(result)
        }
        "remove" => {
            let Some(name) = request.program else {
                return Response {
                    ok: false,
                    message: "program is required".to_string(),
                    data: None,
                };
            };
            let result = supervisor::remove_program(supervisor, &name).await;
            result_to_response(result)
        }
        _ => Response {
            ok: false,
            message: "unsupported command".to_string(),
            data: None,
        },
    }
}

fn result_to_response(result: anyhow::Result<String>) -> Response {
    match result {
        Ok(message) => Response {
            ok: true,
            message,
            data: None,
        },
        Err(err) => Response {
            ok: false,
            message: err.to_string(),
            data: None,
        },
    }
}

async fn read_tail_lines(
    path: &Path,
    lines: usize,
    offset: u64,
    bytes: Option<u64>,
    since: Option<u64>,
) -> anyhow::Result<LogTailReply> {
    if !tokio::fs::try_exists(path).await.unwrap_or(false) {
        return Ok(LogTailReply {
            lines: Vec::new(),
            offset: 0,
        });
    }
    let metadata = tokio::fs::metadata(path).await?;
    if let Some(since_ts) = since {
        if let Ok(modified) = metadata.modified() {
            if let Ok(duration) = modified.duration_since(std::time::UNIX_EPOCH) {
                if duration.as_secs() < since_ts {
                    return Ok(LogTailReply {
                        lines: Vec::new(),
                        offset: metadata.len(),
                    });
                }
            }
        }
    }
    let mut offset = offset;
    if offset > metadata.len() {
        offset = 0;
    }
    if offset > 0 {
        let mut file = tokio::fs::File::open(path).await?;
        use tokio::io::AsyncSeekExt;
        file.seek(std::io::SeekFrom::Start(offset)).await?;
        let mut content = String::new();
        use tokio::io::AsyncReadExt;
        file.read_to_string(&mut content).await?;
        let new_offset = offset + content.as_bytes().len() as u64;
        let lines_vec = content
            .lines()
            .map(|line| line.to_string())
            .collect::<Vec<_>>();
        return Ok(LogTailReply {
            lines: lines_vec,
            offset: new_offset,
        });
    }
    let content = if let Some(bytes) = bytes {
        let mut file = tokio::fs::File::open(path).await?;
        use tokio::io::AsyncSeekExt;
        let len = metadata.len();
        let start = len.saturating_sub(bytes);
        file.seek(std::io::SeekFrom::Start(start)).await?;
        let mut buffer = String::new();
        use tokio::io::AsyncReadExt;
        file.read_to_string(&mut buffer).await?;
        buffer
    } else {
        tokio::fs::read_to_string(path).await?
    };
    let mut all: Vec<String> = content.lines().map(|line| line.to_string()).collect();
    if all.len() > lines {
        all = all.split_off(all.len() - lines);
    }
    let offset = metadata.len();
    Ok(LogTailReply { lines: all, offset })
}

async fn handle_logtail_stream(
    framed: &mut Framed<UnixStream, LengthDelimitedCodec>,
    request: Request,
    supervisor: Arc<Mutex<supervisor::Supervisor>>,
) -> anyhow::Result<()> {
    let Some(name) = request.program else {
        let response = Response {
            ok: false,
            message: "program is required".to_string(),
            data: None,
        };
        let payload = serde_json::to_vec(&response)?;
        let _ = framed.send(Bytes::from(payload)).await;
        return Ok(());
    };
    let lines = request.lines.unwrap_or(10);
    let stream = request.stream.unwrap_or_else(|| "stdout".to_string());
    let mut offset = request.offset.unwrap_or(0);
    let bytes = request.bytes;
    let since = request.since;
    let log_path = {
        let guard = supervisor.lock().await;
        let Some(config) = guard.program_config(&name) else {
            let response = Response {
                ok: false,
                message: format!("unknown program {name}"),
                data: None,
            };
            let payload = serde_json::to_vec(&response)?;
            let _ = framed.send(Bytes::from(payload)).await;
            return Ok(());
        };
        match stream.as_str() {
            "stderr" => config.stderr_log.clone(),
            _ => config.stdout_log.clone(),
        }
    };
    let Some(path) = log_path else {
        let response = Response {
            ok: false,
            message: "log file is not configured".to_string(),
            data: None,
        };
        let payload = serde_json::to_vec(&response)?;
        let _ = framed.send(Bytes::from(payload)).await;
        return Ok(());
    };

    if let Ok(reply) = read_tail_lines(&path, lines, offset, bytes, since).await {
        offset = reply.offset;
        let response = Response {
            ok: true,
            message: "ok".to_string(),
            data: serde_json::to_value(reply).ok(),
        };
        let payload = serde_json::to_vec(&response)?;
        if framed.send(Bytes::from(payload)).await.is_err() {
            return Ok(());
        }
    }

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let mut watcher = RecommendedWatcher::new(
        move |res: Result<notify::Event, notify::Error>| {
            if let Ok(event) = res {
                if matches!(event.kind, EventKind::Modify(_)|EventKind::Create(_)|EventKind::Remove(_)) {
                    let _ = tx.send(());
                }
            }
        },
        NotifyConfig::default(),
    )?;
    let watch_path = path.parent().unwrap_or_else(|| Path::new("/"));
    watcher.watch(watch_path, RecursiveMode::NonRecursive)?;

    while let Some(_) = rx.recv().await {
        match read_tail_lines(&path, lines, offset, None, None).await {
            Ok(reply) => {
                offset = reply.offset;
                if reply.lines.is_empty() {
                    continue;
                }
                let response = Response {
                    ok: true,
                    message: "ok".to_string(),
                    data: serde_json::to_value(reply).ok(),
                };
                let payload = serde_json::to_vec(&response)?;
                if framed.send(Bytes::from(payload)).await.is_err() {
                    break;
                }
            }
            Err(_) => {
                break;
            }
        }
    }
    Ok(())
}

async fn handle_events_stream(
    framed: &mut Framed<UnixStream, LengthDelimitedCodec>,
    supervisor: Arc<Mutex<supervisor::Supervisor>>,
) -> anyhow::Result<()> {
    let mut rx = {
        let guard = supervisor.lock().await;
        guard.events()
    };
    while let Ok(event) = rx.recv().await {
        let response = Response {
            ok: true,
            message: "ok".to_string(),
            data: serde_json::to_value(event).ok(),
        };
        let payload = serde_json::to_vec(&response)?;
        if framed.send(Bytes::from(payload)).await.is_err() {
            break;
        }
    }
    Ok(())
}
