use crate::config::{Autorestart, Config, ProgramConfig};
use crate::process;
use crate::supervisor::expand_programs;
use crate::supervisor::{
    Event, ProgramHandle, ProgramState, ProgramStatus, ReloadSummary, RereadSummary, Rvisor,
    UpdateSummary,
};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::time::Instant;

const QUEUE_SIZE: usize = 256;
const SEND_TIMEOUT: Duration = Duration::from_secs(5);
const REPLY_TIMEOUT: Duration = Duration::from_secs(30);

pub enum Command {
    // External
    Status {
        program: Option<String>,
        reply: oneshot::Sender<Vec<ProgramStatus>>,
    },
    Start {
        program: Option<String>,
        reply: oneshot::Sender<anyhow::Result<String>>,
    },
    Stop {
        program: Option<String>,
        reply: oneshot::Sender<anyhow::Result<String>>,
    },
    Restart {
        program: Option<String>,
        reply: oneshot::Sender<anyhow::Result<String>>,
    },
    Signal {
        program: Option<String>,
        signal: String,
        reply: oneshot::Sender<anyhow::Result<String>>,
    },
    Reread {
        config: Config,
        reply: oneshot::Sender<anyhow::Result<RereadSummary>>,
    },
    Update {
        config: Config,
        reply: oneshot::Sender<anyhow::Result<UpdateSummary>>,
    },
    Reload {
        config: Config,
        reply: oneshot::Sender<anyhow::Result<ReloadSummary>>,
    },
    Avail {
        reply: oneshot::Sender<Vec<String>>,
    },
    Pid {
        reply: oneshot::Sender<u32>,
    },
    Clear {
        program: Option<String>,
        reply: oneshot::Sender<anyhow::Result<String>>,
    },
    Add {
        name: String,
        config: Config,
        reply: oneshot::Sender<anyhow::Result<String>>,
    },
    Remove {
        name: String,
        reply: oneshot::Sender<anyhow::Result<String>>,
    },
    EventsSubscribe {
        reply: oneshot::Sender<broadcast::Receiver<Event>>,
    },
    LogPath {
        name: String,
        stream: String,
        reply: oneshot::Sender<anyhow::Result<PathBuf>>,
    },
    MainLogPath {
        reply: oneshot::Sender<Option<PathBuf>>,
    },
    Shutdown {
        reply: oneshot::Sender<anyhow::Result<()>>,
    },
    // Internal (from worker tasks)
    InternalReady {
        name: String,
        pid: i32,
    },
    InternalExit {
        name: String,
        pid: i32,
        exit_code: i32,
    },
    InternalStopped {
        name: String,
        pid: i32,
        result: anyhow::Result<()>,
    },
    InternalAutostart {
        name: String,
    },
}

#[derive(Clone)]
pub struct RvisorHandle {
    tx: mpsc::Sender<Command>,
    pub config_path: PathBuf,
}

impl RvisorHandle {
    async fn send_infallible<R: Send + 'static>(
        &self,
        make_cmd: impl FnOnce(oneshot::Sender<R>) -> Command,
    ) -> anyhow::Result<R> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send_timeout(make_cmd(tx), SEND_TIMEOUT)
            .await
            .map_err(|e| match e {
                mpsc::error::SendTimeoutError::Timeout(_) => {
                    anyhow::anyhow!("supervisor queue full (backpressure)")
                }
                mpsc::error::SendTimeoutError::Closed(_) => {
                    anyhow::anyhow!("supervisor actor is not running")
                }
            })?;
        tokio::time::timeout(REPLY_TIMEOUT, rx)
            .await
            .map_err(|_| anyhow::anyhow!("supervisor actor timed out"))?
            .map_err(|_| anyhow::anyhow!("supervisor actor dropped reply"))
    }

    async fn send_fallible<R: Send + 'static>(
        &self,
        make_cmd: impl FnOnce(oneshot::Sender<anyhow::Result<R>>) -> Command,
    ) -> anyhow::Result<R> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send_timeout(make_cmd(tx), SEND_TIMEOUT)
            .await
            .map_err(|e| match e {
                mpsc::error::SendTimeoutError::Timeout(_) => {
                    anyhow::anyhow!("supervisor queue full (backpressure)")
                }
                mpsc::error::SendTimeoutError::Closed(_) => {
                    anyhow::anyhow!("supervisor actor is not running")
                }
            })?;
        tokio::time::timeout(REPLY_TIMEOUT, rx)
            .await
            .map_err(|_| anyhow::anyhow!("supervisor actor timed out"))?
            .map_err(|_| anyhow::anyhow!("supervisor actor dropped reply"))?
    }

    pub async fn status(&self, program: Option<String>) -> anyhow::Result<Vec<ProgramStatus>> {
        self.send_infallible(|reply| Command::Status { program, reply })
            .await
    }
    pub async fn start(&self, program: Option<String>) -> anyhow::Result<String> {
        self.send_fallible(|reply| Command::Start { program, reply })
            .await
    }
    pub async fn stop(&self, program: Option<String>) -> anyhow::Result<String> {
        self.send_fallible(|reply| Command::Stop { program, reply })
            .await
    }
    pub async fn restart(&self, program: Option<String>) -> anyhow::Result<String> {
        self.send_fallible(|reply| Command::Restart { program, reply })
            .await
    }
    pub async fn signal(&self, program: Option<String>, signal: String) -> anyhow::Result<String> {
        self.send_fallible(|reply| Command::Signal {
            program,
            signal,
            reply,
        })
        .await
    }
    pub async fn reread(&self, config: Config) -> anyhow::Result<RereadSummary> {
        self.send_fallible(|reply| Command::Reread { config, reply })
            .await
    }
    pub async fn update(&self, config: Config) -> anyhow::Result<UpdateSummary> {
        self.send_fallible(|reply| Command::Update { config, reply })
            .await
    }
    pub async fn reload(&self, config: Config) -> anyhow::Result<ReloadSummary> {
        self.send_fallible(|reply| Command::Reload { config, reply })
            .await
    }
    pub async fn avail(&self) -> anyhow::Result<Vec<String>> {
        self.send_infallible(|reply| Command::Avail { reply }).await
    }
    pub async fn pid(&self) -> anyhow::Result<u32> {
        self.send_infallible(|reply| Command::Pid { reply }).await
    }
    pub async fn clear(&self, program: Option<String>) -> anyhow::Result<String> {
        self.send_fallible(|reply| Command::Clear { program, reply })
            .await
    }
    pub async fn add(&self, name: String, config: Config) -> anyhow::Result<String> {
        self.send_fallible(|reply| Command::Add {
            name,
            config,
            reply,
        })
        .await
    }
    pub async fn remove(&self, name: String) -> anyhow::Result<String> {
        self.send_fallible(|reply| Command::Remove { name, reply })
            .await
    }
    pub async fn events_subscribe(&self) -> anyhow::Result<broadcast::Receiver<Event>> {
        self.send_infallible(|reply| Command::EventsSubscribe { reply })
            .await
    }
    pub async fn log_path(&self, name: String, stream: String) -> anyhow::Result<PathBuf> {
        self.send_fallible(|reply| Command::LogPath {
            name,
            stream,
            reply,
        })
        .await
    }
    pub async fn main_log_path(&self) -> anyhow::Result<Option<PathBuf>> {
        self.send_infallible(|reply| Command::MainLogPath { reply })
            .await
    }
    pub async fn shutdown(&self) -> anyhow::Result<()> {
        self.send_fallible(|reply| Command::Shutdown { reply })
            .await
    }
}

pub fn spawn_actor(
    config_path: PathBuf,
    programs: Vec<ProgramConfig>,
    global_env: HashMap<String, String>,
    pidfile: Option<PathBuf>,
    sock_path: PathBuf,
    logfile: Option<PathBuf>,
) -> RvisorHandle {
    let supervisor = Rvisor::new_from_config(
        config_path.clone(),
        programs,
        global_env,
        pidfile,
        sock_path,
        logfile,
    );
    // Get expanded autostart names before handing supervisor to the actor loop.
    let autostart_names = supervisor.autostart_programs();
    let (tx, rx) = mpsc::channel(QUEUE_SIZE);
    let tx_clone = tx.clone();
    tokio::spawn(actor_loop(supervisor, rx, tx_clone));
    let handle = RvisorHandle { tx, config_path };
    // Queue up start commands for all autostart programs (using expanded names).
    // We do this after spawning the actor so it can process them.
    let handle_clone = handle.clone();
    tokio::spawn(async move {
        for name in autostart_names {
            let _ = handle_clone.start(Some(name)).await;
        }
    });
    handle
}

struct PendingUpdate {
    reply: oneshot::Sender<anyhow::Result<UpdateSummary>>,
    summary: UpdateSummary,
    waiting: HashSet<String>,
    to_start: Vec<String>,
    /// Programs that must be removed from sup.programs once their stop completes.
    to_remove: HashSet<String>,
}

struct PendingStopAll {
    reply: oneshot::Sender<anyhow::Result<String>>,
    waiting: HashSet<String>,
    then_start_all: bool,
}

/// Tracks an in-progress shutdown: reply already sent, waiting for all workers
/// to send InternalStopped before doing cleanup and exit(0).
struct PendingShutdown {
    waiting: HashSet<String>,
    pidfile: Option<PathBuf>,
    sock_path: PathBuf,
}

struct PendingOps<'a> {
    stops: &'a mut HashMap<String, oneshot::Sender<anyhow::Result<String>>>,
    restarts: &'a mut HashMap<String, oneshot::Sender<anyhow::Result<String>>>,
    update: &'a mut Option<PendingUpdate>,
    stop_all: &'a mut Option<PendingStopAll>,
    shutdown: &'a mut Option<PendingShutdown>,
}

async fn actor_loop(mut sup: Rvisor, mut rx: mpsc::Receiver<Command>, tx: mpsc::Sender<Command>) {
    let mut pending_stops: HashMap<String, oneshot::Sender<anyhow::Result<String>>> =
        HashMap::new();
    let mut pending_restarts: HashMap<String, oneshot::Sender<anyhow::Result<String>>> =
        HashMap::new();
    let mut pending_update: Option<PendingUpdate> = None;
    let mut pending_stop_all: Option<PendingStopAll> = None;
    let mut pending_shutdown: Option<PendingShutdown> = None;

    while let Some(cmd) = rx.recv().await {
        match cmd {
            Command::Status { program, reply } => {
                let _ = reply.send(sup.status(program.as_deref()));
            }
            Command::Avail { reply } => {
                let mut names: Vec<String> = sup.programs.keys().cloned().collect();
                names.sort();
                let _ = reply.send(names);
            }
            Command::Pid { reply } => {
                let _ = reply.send(std::process::id());
            }
            Command::EventsSubscribe { reply } => {
                let _ = reply.send(sup.events());
            }
            Command::LogPath {
                name,
                stream,
                reply,
            } => match sup.programs.get(&name) {
                None => {
                    let _ = reply.send(Err(anyhow::anyhow!("unknown program {}", name)));
                }
                Some(h) => {
                    let path = if stream == "stderr" {
                        h.config.stderr_log.clone()
                    } else {
                        h.config.stdout_log.clone()
                    };
                    match path {
                        Some(p) => {
                            let _ = reply.send(Ok(p));
                        }
                        None => {
                            let _ =
                                reply.send(Err(anyhow::anyhow!("log not configured for {}", name)));
                        }
                    }
                }
            },
            Command::MainLogPath { reply } => {
                let _ = reply.send(sup.logfile.clone());
            }
            Command::Start { program, reply } => {
                handle_start(&mut sup, &tx, program, reply);
            }
            Command::Stop { program, reply } => {
                handle_stop(
                    &mut sup,
                    &tx,
                    program,
                    reply,
                    &mut pending_stops,
                    &mut pending_stop_all,
                );
            }
            Command::Restart { program, reply } => {
                handle_restart(
                    &mut sup,
                    &tx,
                    program,
                    reply,
                    &mut pending_restarts,
                    &mut pending_stop_all,
                );
            }
            Command::Signal {
                program,
                signal,
                reply,
            } => {
                let result = handle_signal(&mut sup, program, &signal);
                let _ = reply.send(result);
            }
            Command::Reread { config, reply } => {
                let result = compute_reread(&sup, &config);
                let _ = reply.send(Ok(result));
            }
            Command::Update { config, reply } => {
                handle_update(
                    &mut sup,
                    &tx,
                    config,
                    reply,
                    &mut pending_update,
                    &mut pending_stop_all,
                );
            }
            Command::Reload { config, reply } => {
                handle_reload(
                    &mut sup,
                    &tx,
                    config,
                    reply,
                    &mut pending_update,
                    &mut pending_stop_all,
                );
            }
            Command::Clear { program, reply } => {
                let result = handle_clear(&sup, program).await;
                let _ = reply.send(result);
            }
            Command::Add {
                name,
                config,
                reply,
            } => {
                let result = handle_add(&mut sup, &name, &config);
                let _ = reply.send(result);
            }
            Command::Remove { name, reply } => {
                let result = handle_remove(&mut sup, &name);
                let _ = reply.send(result);
            }
            Command::Shutdown { reply } => {
                handle_shutdown(&mut sup, &tx, reply, &mut pending_shutdown);
            }
            Command::InternalReady { name, pid } => {
                handle_internal_ready(&mut sup, &name, pid);
            }
            Command::InternalExit {
                name,
                pid,
                exit_code,
            } => {
                handle_internal_exit(&mut sup, &tx, name, pid, exit_code);
            }
            Command::InternalStopped { name, pid, result } => handle_internal_stopped(
                &mut sup,
                &tx,
                name,
                pid,
                result,
                PendingOps {
                    stops: &mut pending_stops,
                    restarts: &mut pending_restarts,
                    update: &mut pending_update,
                    stop_all: &mut pending_stop_all,
                    shutdown: &mut pending_shutdown,
                },
            ),
            Command::InternalAutostart { name } => {
                handle_internal_autostart(&mut sup, &tx, &name);
            }
        }
    }
}

fn do_start_one(
    sup: &mut Rvisor,
    tx: &mpsc::Sender<Command>,
    name: &str,
    reset_retries: bool,
) -> anyhow::Result<String> {
    // Extract all needed info while the borrow is live, then drop it
    let (already_running, config) = {
        let Some(handle) = sup.programs.get_mut(name) else {
            return Err(anyhow::anyhow!("unknown program {}", name));
        };
        if matches!(
            handle.state,
            ProgramState::Running | ProgramState::Starting | ProgramState::Stopping
        ) {
            return Ok(format!("{} already running", name));
        }
        if reset_retries {
            handle.retries = 0;
        }
        handle.state = ProgramState::Starting;
        let config = handle.config.clone();
        (false, config)
    };
    let _ = already_running; // suppress warning
    sup.emit_event(name, ProgramState::Starting, None);

    let global_env = sup.global_env.clone();

    let spawned = match process::spawn_program(&config, &global_env) {
        Ok(s) => s,
        Err(e) => {
            if let Some(h) = sup.programs.get_mut(name) {
                h.state = ProgramState::Fatal;
            }
            sup.emit_event(name, ProgramState::Fatal, None);
            return Err(e);
        }
    };

    let pid = spawned.pid;
    let process::SpawnedProcess {
        pid: _,
        child,
        log_handles,
    } = spawned;

    if let Some(h) = sup.programs.get_mut(name) {
        h.pid = Some(pid);
        h.start_time = Some(Instant::now());
    }

    let startsecs = config.startsecs;
    let name_str = name.to_string();

    // Readiness timer
    let tx1 = tx.clone();
    let n1 = name_str.clone();
    tokio::spawn(async move {
        if startsecs > 0 {
            tokio::time::sleep(Duration::from_secs(startsecs)).await;
        }
        let _ = tx1.send(Command::InternalReady { name: n1, pid }).await;
    });

    // Exit monitor — owns child AND log_handles
    let tx2 = tx.clone();
    let n2 = name_str;
    tokio::spawn(async move {
        let mut child = child;
        let status = child.wait().await.ok();
        // drain log handles
        for h in log_handles {
            match h.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => tracing::warn!("log write error for {n2}: {e}"),
                Err(e) => tracing::warn!("log task for {n2} panicked: {e}"),
            }
        }
        let exit_code = status.and_then(|s| s.code()).unwrap_or(-1);
        let _ = tx2
            .send(Command::InternalExit {
                name: n2,
                pid,
                exit_code,
            })
            .await;
    });

    Ok(format!("{} started", name))
}

fn handle_start(
    sup: &mut Rvisor,
    tx: &mpsc::Sender<Command>,
    program: Option<String>,
    reply: oneshot::Sender<anyhow::Result<String>>,
) {
    match program {
        Some(name) => {
            let result = do_start_one(sup, tx, &name, true);
            let _ = reply.send(result);
        }
        None => {
            let names: Vec<String> = sup.programs.keys().cloned().collect();
            let mut last_err: Option<anyhow::Error> = None;
            for name in names {
                if let Err(e) = do_start_one(sup, tx, &name, true) {
                    last_err = Some(e);
                }
            }
            let _ = reply.send(
                last_err
                    .map(Err)
                    .unwrap_or_else(|| Ok("started all programs".to_string())),
            );
        }
    }
}

fn spawn_stop_worker(
    tx: &mpsc::Sender<Command>,
    name: String,
    pid: i32,
    stop_signal: String,
    wait_secs: u64,
    killasgroup: bool,
) {
    let tx = tx.clone();
    tokio::spawn(async move {
        let result = do_stop_process(pid, &stop_signal, wait_secs, killasgroup).await;
        let _ = tx
            .send(Command::InternalStopped { name, pid, result })
            .await;
    });
}

async fn do_stop_process(
    pid: i32,
    signal: &str,
    wait_secs: u64,
    killasgroup: bool,
) -> anyhow::Result<()> {
    crate::supervisor::kill_process_tree(pid, signal, killasgroup);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(wait_secs);
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if !crate::supervisor::process_alive(pid) {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            break;
        }
    }
    crate::supervisor::kill_process_tree(pid, "KILL", killasgroup);
    Ok(())
}

fn handle_stop(
    sup: &mut Rvisor,
    tx: &mpsc::Sender<Command>,
    program: Option<String>,
    reply: oneshot::Sender<anyhow::Result<String>>,
    pending_stops: &mut HashMap<String, oneshot::Sender<anyhow::Result<String>>>,
    pending_stop_all: &mut Option<PendingStopAll>,
) {
    match program {
        Some(name) => {
            // Extract state info before emitting events (borrow checker)
            let stop_info = sup.programs.get_mut(&name).map(|h| {
                let pid = h.pid;
                let sig = h.config.stopsignal.clone();
                let wait = h.config.stopwaitsecs;
                let kg = h.config.killasgroup;
                if pid.is_some() {
                    h.state = ProgramState::Stopping;
                } else {
                    h.state = ProgramState::Stopped;
                }
                (pid, sig, wait, kg)
            });
            let Some((pid_opt, sig, wait, kg)) = stop_info else {
                let _ = reply.send(Err(anyhow::anyhow!("unknown program {}", name)));
                return;
            };
            if let Some(pid) = pid_opt {
                sup.emit_event(&name, ProgramState::Stopping, Some(pid));
                pending_stops.insert(name.clone(), reply);
                spawn_stop_worker(tx, name, pid, sig, wait, kg);
            } else {
                sup.emit_event(&name, ProgramState::Stopped, None);
                let _ = reply.send(Ok(format!("{} stopped", name)));
            }
        }
        None => {
            // Stop all programs
            let running: Vec<String> = sup
                .programs
                .iter()
                .filter(|(_, h)| h.pid.is_some())
                .map(|(n, _)| n.clone())
                .collect();

            if running.is_empty() {
                let _ = reply.send(Ok("stopped all programs".to_string()));
                return;
            }

            let waiting: HashSet<String> = running.iter().cloned().collect();
            *pending_stop_all = Some(PendingStopAll {
                reply,
                waiting,
                then_start_all: false,
            });

            for name in running {
                // Extract values, set state, drop borrow, then emit event
                let info = sup.programs.get_mut(&name).and_then(|h| {
                    h.pid.map(|pid| {
                        h.state = ProgramState::Stopping;
                        let sig = h.config.stopsignal.clone();
                        let wait = h.config.stopwaitsecs;
                        let kg = h.config.killasgroup;
                        (pid, sig, wait, kg)
                    })
                });
                if let Some((pid, sig, wait, kg)) = info {
                    sup.emit_event(&name, ProgramState::Stopping, Some(pid));
                    spawn_stop_worker(tx, name, pid, sig, wait, kg);
                }
            }
        }
    }
}

fn handle_restart(
    sup: &mut Rvisor,
    tx: &mpsc::Sender<Command>,
    program: Option<String>,
    reply: oneshot::Sender<anyhow::Result<String>>,
    pending_restarts: &mut HashMap<String, oneshot::Sender<anyhow::Result<String>>>,
    pending_stop_all: &mut Option<PendingStopAll>,
) {
    match program {
        Some(name) => {
            let stop_info = sup.programs.get_mut(&name).map(|h| {
                let pid = h.pid;
                let sig = h.config.stopsignal.clone();
                let wait = h.config.stopwaitsecs;
                let kg = h.config.killasgroup;
                if pid.is_some() {
                    h.state = ProgramState::Stopping;
                }
                (pid, sig, wait, kg)
            });
            let Some((pid_opt, sig, wait, kg)) = stop_info else {
                let _ = reply.send(Err(anyhow::anyhow!("unknown program {}", name)));
                return;
            };
            if let Some(pid) = pid_opt {
                sup.emit_event(&name, ProgramState::Stopping, Some(pid));
                pending_restarts.insert(name.clone(), reply);
                spawn_stop_worker(tx, name, pid, sig, wait, kg);
            } else {
                // Not running, just start it
                let result = do_start_one(sup, tx, &name, true);
                let _ = reply.send(result);
            }
        }
        None => {
            // Restart all: stop all, then start all
            let running: Vec<String> = sup
                .programs
                .iter()
                .filter(|(_, h)| h.pid.is_some())
                .map(|(n, _)| n.clone())
                .collect();

            if running.is_empty() {
                // No running programs, just start all
                let names: Vec<String> = sup.programs.keys().cloned().collect();
                for name in names {
                    let _ = do_start_one(sup, tx, &name, true);
                }
                let _ = reply.send(Ok("restarted all programs".to_string()));
                return;
            }

            let waiting: HashSet<String> = running.iter().cloned().collect();
            *pending_stop_all = Some(PendingStopAll {
                reply,
                waiting,
                then_start_all: true,
            });

            for name in running {
                let info = sup.programs.get_mut(&name).and_then(|h| {
                    h.pid.map(|pid| {
                        h.state = ProgramState::Stopping;
                        let sig = h.config.stopsignal.clone();
                        let wait = h.config.stopwaitsecs;
                        let kg = h.config.killasgroup;
                        (pid, sig, wait, kg)
                    })
                });
                if let Some((pid, sig, wait, kg)) = info {
                    sup.emit_event(&name, ProgramState::Stopping, Some(pid));
                    spawn_stop_worker(tx, name, pid, sig, wait, kg);
                }
            }
        }
    }
}

fn handle_signal(
    sup: &mut Rvisor,
    program: Option<String>,
    signal: &str,
) -> anyhow::Result<String> {
    match program {
        Some(name) => {
            let Some(h) = sup.programs.get(&name) else {
                return Err(anyhow::anyhow!("unknown program {}", name));
            };
            let Some(pid) = h.pid else {
                return Err(anyhow::anyhow!("program {} not running", name));
            };
            crate::supervisor::send_signal(pid, signal)?;
            Ok(format!("{} signaled {}", name, signal))
        }
        None => {
            let pids: Vec<(String, i32)> = sup
                .programs
                .iter()
                .filter_map(|(n, h)| h.pid.map(|p| (n.clone(), p)))
                .collect();
            for (_name, pid) in &pids {
                crate::supervisor::send_signal(*pid, signal)?;
            }
            Ok(format!("signaled all with {}", signal))
        }
    }
}

fn compute_reread(sup: &Rvisor, config: &Config) -> RereadSummary {
    let new_map: HashMap<String, ProgramConfig> = expand_programs(config.programs.clone())
        .into_iter()
        .map(|p| (p.name.clone(), p))
        .collect();

    let current_names: HashSet<_> = sup.programs.keys().cloned().collect();
    let new_names: HashSet<_> = new_map.keys().cloned().collect();

    let mut added: Vec<String> = new_names.difference(&current_names).cloned().collect();
    let mut removed: Vec<String> = current_names.difference(&new_names).cloned().collect();
    let mut changed: Vec<String> = new_names
        .intersection(&current_names)
        .filter_map(|name| {
            let existing = sup.programs.get(name)?;
            let updated = new_map.get(name)?;
            if existing.config != *updated {
                Some(name.clone())
            } else {
                None
            }
        })
        .collect();

    added.sort();
    removed.sort();
    changed.sort();

    RereadSummary {
        added,
        changed,
        removed,
    }
}

fn handle_update(
    sup: &mut Rvisor,
    tx: &mpsc::Sender<Command>,
    config: Config,
    reply: oneshot::Sender<anyhow::Result<UpdateSummary>>,
    pending_update: &mut Option<PendingUpdate>,
    _pending_stop_all: &mut Option<PendingStopAll>,
) {
    let mut summary = UpdateSummary {
        started: Vec::new(),
        stopped: Vec::new(),
        restarted: Vec::new(),
        removed: Vec::new(),
        added: Vec::new(),
        changed: Vec::new(),
    };

    let mut new_map: HashMap<String, ProgramConfig> = expand_programs(config.programs.clone())
        .into_iter()
        .map(|p| (p.name.clone(), p))
        .collect();

    let current_names: HashSet<String> = sup.programs.keys().cloned().collect();
    let new_names: HashSet<String> = new_map.keys().cloned().collect();

    let mut to_stop_removed: Vec<String> = Vec::new();
    let mut to_stop_changed: Vec<String> = Vec::new();
    let mut to_start_added: Vec<String> = Vec::new();

    // Removed programs
    for name in current_names.difference(&new_names) {
        to_stop_removed.push(name.clone());
        summary.removed.push(name.clone());
    }

    // Added programs
    for name in new_names.difference(&current_names) {
        if let Some(program) = new_map.remove(name) {
            let autostart = program.autostart;
            summary.added.push(name.clone());
            sup.programs.insert(
                name.clone(),
                ProgramHandle {
                    config: program,
                    state: ProgramState::Stopped,
                    pid: None,
                    retries: 0,
                    start_time: None,
                },
            );
            if autostart {
                to_start_added.push(name.clone());
            }
        }
    }

    // Changed programs
    for name in new_names.intersection(&current_names) {
        if let Some(new_cfg) = new_map.remove(name) {
            if let Some(handle) = sup.programs.get_mut(name) {
                if handle.config != new_cfg {
                    summary.changed.push(name.clone());
                    handle.config = new_cfg;
                    if matches!(handle.state, ProgramState::Running | ProgramState::Starting) {
                        to_stop_changed.push(name.clone());
                        summary.restarted.push(name.clone());
                    }
                }
            }
        }
    }

    // Collect all names that need stopping
    let mut waiting: HashSet<String> = HashSet::new();
    // Programs that must be removed once their stop completes.
    let mut to_remove: HashSet<String> = HashSet::new();
    for name in &to_stop_removed {
        let info = sup.programs.get_mut(name).and_then(|h| {
            h.pid.map(|pid| {
                h.state = ProgramState::Stopping;
                let sig = h.config.stopsignal.clone();
                let wait_secs = h.config.stopwaitsecs;
                let kg = h.config.killasgroup;
                (pid, sig, wait_secs, kg)
            })
        });
        match info {
            Some((pid, sig, wait_secs, kg)) => {
                sup.emit_event(name, ProgramState::Stopping, Some(pid));
                waiting.insert(name.clone());
                to_remove.insert(name.clone());
                spawn_stop_worker(tx, name.clone(), pid, sig, wait_secs, kg);
            }
            None => {
                // No pid: remove immediately
                sup.programs.remove(name);
            }
        }
    }
    for name in &to_stop_changed {
        let info = sup.programs.get_mut(name).and_then(|h| {
            h.pid.map(|pid| {
                h.state = ProgramState::Stopping;
                let sig = h.config.stopsignal.clone();
                let wait_secs = h.config.stopwaitsecs;
                let kg = h.config.killasgroup;
                (pid, sig, wait_secs, kg)
            })
        });
        if let Some((pid, sig, wait_secs, kg)) = info {
            sup.emit_event(name, ProgramState::Stopping, Some(pid));
            waiting.insert(name.clone());
            spawn_stop_worker(tx, name.clone(), pid, sig, wait_secs, kg);
        }
    }

    // Build to_start list: added autostart + changed (will restart after stop)
    let mut to_start = to_start_added;
    to_start.extend(to_stop_changed.iter().cloned());

    if waiting.is_empty() {
        // No stops needed: immediately start to_start programs and reply
        for name in &to_start {
            if let Ok(msg) = do_start_one(sup, tx, name, true) {
                summary.started.push(name.clone());
                tracing::debug!("update started {}: {}", name, msg);
            }
        }
        let _ = reply.send(Ok(summary));
    } else {
        *pending_update = Some(PendingUpdate {
            reply,
            summary,
            waiting,
            to_start,
            to_remove,
        });
    }
}

fn handle_reload(
    sup: &mut Rvisor,
    tx: &mpsc::Sender<Command>,
    config: Config,
    reply: oneshot::Sender<anyhow::Result<ReloadSummary>>,
    pending_update: &mut Option<PendingUpdate>,
    pending_stop_all: &mut Option<PendingStopAll>,
) {
    let reread = compute_reread(sup, &config);
    // We need to wrap the update reply to also include the reread summary
    let (update_tx, update_rx) = oneshot::channel::<anyhow::Result<UpdateSummary>>();
    handle_update(sup, tx, config, update_tx, pending_update, pending_stop_all);

    // Spawn a task to combine reread + update result
    tokio::spawn(async move {
        match update_rx.await {
            Ok(Ok(update)) => {
                let _ = reply.send(Ok(ReloadSummary { reread, update }));
            }
            Ok(Err(e)) => {
                let _ = reply.send(Err(e));
            }
            Err(_) => {
                let _ = reply.send(Err(anyhow::anyhow!("update reply dropped")));
            }
        }
    });
}

async fn handle_clear(sup: &Rvisor, program: Option<String>) -> anyhow::Result<String> {
    let paths: Vec<Option<PathBuf>> = match &program {
        Some(name) => {
            let Some(h) = sup.programs.get(name) else {
                return Err(anyhow::anyhow!("unknown program {}", name));
            };
            vec![h.config.stdout_log.clone(), h.config.stderr_log.clone()]
        }
        None => sup
            .programs
            .values()
            .flat_map(|h| [h.config.stdout_log.clone(), h.config.stderr_log.clone()])
            .collect(),
    };
    let mut errors: Vec<String> = Vec::new();
    for path in paths.into_iter().flatten() {
        if let Err(e) = truncate_file(&path).await {
            errors.push(format!("{}: {}", path.display(), e));
        }
    }
    if errors.is_empty() {
        Ok("cleared".to_string())
    } else {
        Err(anyhow::anyhow!("clear errors: {}", errors.join("; ")))
    }
}

async fn truncate_file(path: &Path) -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt;
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .await?;
    file.flush().await?;
    Ok(())
}

fn handle_add(sup: &mut Rvisor, name: &str, config: &Config) -> anyhow::Result<String> {
    if sup.programs.contains_key(name) {
        return Ok(format!("{} already exists", name));
    }
    let program = config
        .programs
        .iter()
        .find(|p| p.name == name)
        .ok_or_else(|| anyhow::anyhow!("program {} not found in config", name))?
        .clone();
    sup.programs.insert(
        name.to_string(),
        ProgramHandle {
            config: program,
            state: ProgramState::Stopped,
            pid: None,
            retries: 0,
            start_time: None,
        },
    );
    Ok(format!("{} added", name))
}

fn handle_remove(sup: &mut Rvisor, name: &str) -> anyhow::Result<String> {
    if sup.programs.remove(name).is_some() {
        sup.emit_event(name, ProgramState::Stopped, None);
        Ok(format!("{} removed", name))
    } else {
        Err(anyhow::anyhow!("unknown program {}", name))
    }
}

fn handle_shutdown(
    sup: &mut Rvisor,
    tx: &mpsc::Sender<Command>,
    reply: oneshot::Sender<anyhow::Result<()>>,
    pending_shutdown: &mut Option<PendingShutdown>,
) {
    // Reply immediately so the caller doesn't block.
    let _ = reply.send(Ok(()));

    // Collect stop info for all running programs before emitting events.
    let names: Vec<String> = sup.programs.keys().cloned().collect();
    let mut stop_infos: Vec<(String, i32, String, u64, bool)> = Vec::new();
    for name in &names {
        if let Some(h) = sup.programs.get_mut(name) {
            if let Some(pid) = h.pid {
                let sig = h.config.stopsignal.clone();
                let wait = h.config.stopwaitsecs;
                let kg = h.config.killasgroup;
                h.state = ProgramState::Stopping;
                stop_infos.push((name.clone(), pid, sig, wait, kg));
            }
        }
    }

    if stop_infos.is_empty() {
        // No running processes — clean up and exit immediately.
        let pidfile = sup.pidfile.clone();
        let sock_path = sup.sock_path.clone();
        tokio::spawn(async move {
            if let Some(p) = pidfile {
                let _ = tokio::fs::remove_file(p).await;
            }
            let _ = tokio::fs::remove_file(sock_path).await;
            std::process::exit(0);
        });
        return;
    }

    let mut waiting = HashSet::new();
    for (name, pid, sig, wait, kg) in stop_infos {
        sup.emit_event(&name, ProgramState::Stopping, Some(pid));
        waiting.insert(name.clone());
        spawn_stop_worker(tx, name, pid, sig, wait, kg);
    }

    *pending_shutdown = Some(PendingShutdown {
        waiting,
        pidfile: sup.pidfile.clone(),
        sock_path: sup.sock_path.clone(),
    });
}

fn handle_internal_ready(sup: &mut Rvisor, name: &str, pid: i32) {
    let should_emit = if let Some(h) = sup.programs.get_mut(name) {
        if h.pid == Some(pid) && matches!(h.state, ProgramState::Starting) {
            h.state = ProgramState::Running;
            true
        } else {
            false
        }
    } else {
        false
    };
    if should_emit {
        sup.emit_event(name, ProgramState::Running, Some(pid));
    }
}

fn handle_internal_exit(
    sup: &mut Rvisor,
    tx: &mpsc::Sender<Command>,
    name: String,
    pid: i32,
    exit_code: i32,
) {
    // Phase 1: extract all info while holding the borrow, compute new state
    enum ExitDecision {
        Exited,
        Fatal,
        Backoff { retries: u32 },
    }

    let decision = {
        let Some(handle) = sup.programs.get_mut(&name) else {
            return;
        };
        if handle.pid != Some(pid) {
            return;
        }
        // STOPPING is handled by InternalStopped
        if matches!(handle.state, ProgramState::Stopping) {
            return;
        }

        let autorestart = handle.config.autorestart;
        let exitcodes = handle.config.exitcodes.clone();
        let startsecs = handle.config.startsecs;
        let startretries = handle.config.startretries;
        let expected = exitcodes.contains(&exit_code);

        let elapsed = handle
            .start_time
            .map(|t| t.elapsed())
            .unwrap_or_else(|| Duration::from_secs(0));
        handle.pid = None;
        handle.start_time = None;

        if autorestart == Autorestart::Never || (autorestart == Autorestart::Unexpected && expected)
        {
            handle.state = ProgramState::Exited;
            ExitDecision::Exited
        } else {
            let early_exit = elapsed < Duration::from_secs(startsecs);
            if early_exit {
                handle.retries += 1;
            } else {
                handle.retries = 0;
            }

            if handle.retries > startretries {
                handle.state = ProgramState::Fatal;
                ExitDecision::Fatal
            } else {
                let retries = handle.retries;
                handle.state = ProgramState::Backoff;
                ExitDecision::Backoff { retries }
            }
        }
    };

    // Phase 2: emit events (borrow dropped)
    match decision {
        ExitDecision::Exited => {
            sup.emit_event(&name, ProgramState::Exited, None);
        }
        ExitDecision::Fatal => {
            sup.emit_event(&name, ProgramState::Fatal, None);
        }
        ExitDecision::Backoff { retries } => {
            sup.emit_event(&name, ProgramState::Backoff, None);
            let delay_secs = if retries == 0 {
                1u64
            } else {
                (1u64 << (retries as u64).saturating_sub(1).min(5)).min(32)
            };
            let tx_clone = tx.clone();
            let name_clone = name.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(delay_secs)).await;
                let _ = tx_clone
                    .send(Command::InternalAutostart { name: name_clone })
                    .await;
            });
        }
    }
}

fn handle_internal_stopped(
    sup: &mut Rvisor,
    tx: &mpsc::Sender<Command>,
    name: String,
    pid: i32,
    result: anyhow::Result<()>,
    pending: PendingOps<'_>,
) {
    // Whether this program should be removed from the map (update removed it).
    let in_to_remove = pending
        .update
        .as_ref()
        .map(|pu| pu.to_remove.contains(&name))
        .unwrap_or(false);

    // Update program state (drop borrow before calling emit_event).
    let should_emit = if let Some(h) = sup.programs.get_mut(&name) {
        if h.pid == Some(pid) || h.pid.is_none() {
            h.pid = None;
            h.state = ProgramState::Stopped;
            h.start_time = None;
            true
        } else {
            false
        }
    } else {
        false
    };
    if should_emit {
        sup.emit_event(&name, ProgramState::Stopped, None);
    }

    // Remove program from the map if it was deleted by an update.
    if in_to_remove {
        sup.programs.remove(&name);
        if let Some(pu) = pending.update.as_mut() {
            pu.to_remove.remove(&name);
        }
    }

    // Check pending_stops — propagate stop errors to the caller.
    if let Some(reply) = pending.stops.remove(&name) {
        match result {
            Ok(()) => {
                let _ = reply.send(Ok(format!("{} stopped", name)));
            }
            Err(e) => {
                let _ = reply.send(Err(e));
            }
        }
    }

    // Check pending_restarts — propagate start errors.
    if let Some(reply) = pending.restarts.remove(&name) {
        let _ = reply.send(do_start_one(sup, tx, &name, true));
    }

    // Check pending_stop_all
    let stop_all_done = if let Some(psa) = pending.stop_all.as_mut() {
        psa.waiting.remove(&name);
        psa.waiting.is_empty()
    } else {
        false
    };
    if stop_all_done {
        if let Some(psa) = pending.stop_all.take() {
            if psa.then_start_all {
                let names: Vec<String> = sup.programs.keys().cloned().collect();
                for n in names {
                    let _ = do_start_one(sup, tx, &n, true);
                }
                let _ = psa.reply.send(Ok("restarted all programs".to_string()));
            } else {
                let _ = psa.reply.send(Ok("stopped all programs".to_string()));
            }
        }
    }

    // Check pending_update
    let update_done = if let Some(pu) = pending.update.as_mut() {
        pu.waiting.remove(&name);
        pu.waiting.is_empty()
    } else {
        false
    };
    if update_done {
        if let Some(mut pu) = pending.update.take() {
            for start_name in &pu.to_start {
                if do_start_one(sup, tx, start_name, true).is_ok() {
                    pu.summary.started.push(start_name.clone());
                }
            }
            let _ = pu.reply.send(Ok(pu.summary));
        }
    }

    // Check pending_shutdown
    let shutdown_done = if let Some(ps) = pending.shutdown.as_mut() {
        ps.waiting.remove(&name);
        ps.waiting.is_empty()
    } else {
        false
    };
    if shutdown_done {
        if let Some(ps) = pending.shutdown.take() {
            tokio::spawn(async move {
                if let Some(p) = ps.pidfile {
                    let _ = tokio::fs::remove_file(p).await;
                }
                let _ = tokio::fs::remove_file(ps.sock_path).await;
                std::process::exit(0);
            });
        }
    }
}

fn handle_internal_autostart(sup: &mut Rvisor, tx: &mpsc::Sender<Command>, name: &str) {
    let Some(h) = sup.programs.get(name) else {
        return;
    };
    if matches!(h.state, ProgramState::Stopping | ProgramState::Stopped) {
        return;
    }
    let _ = do_start_one(sup, tx, name, false);
}
