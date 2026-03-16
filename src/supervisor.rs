use crate::config::{self, Autorestart, ProgramConfig};
use crate::process;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::Instant;
use tokio::sync::broadcast;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc as StdArc;
use futures::future::BoxFuture;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgramStatus {
    pub name: String,
    pub state: String,
    pub pid: Option<i32>,
    pub uptime: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub event_type: String,
    pub serial: u64,
    pub name: String,
    pub group: String,
    pub state: String,
    pub pid: Option<i32>,
    pub timestamp: u64,
    pub payload: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RereadSummary {
    pub added: Vec<String>,
    pub changed: Vec<String>,
    pub removed: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateSummary {
    pub started: Vec<String>,
    pub stopped: Vec<String>,
    pub restarted: Vec<String>,
    pub removed: Vec<String>,
    pub added: Vec<String>,
    pub changed: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReloadSummary {
    pub reread: RereadSummary,
    pub update: UpdateSummary,
}

#[derive(Debug, Clone, Copy)]
enum ProgramState {
    Stopped,
    Starting,
    Running,
    Backoff,
    Stopping,
    Exited,
    Fatal,
}

impl ProgramState {
    fn as_str(self) -> &'static str {
        match self {
            ProgramState::Stopped => "STOPPED",
            ProgramState::Starting => "STARTING",
            ProgramState::Running => "RUNNING",
            ProgramState::Backoff => "BACKOFF",
            ProgramState::Stopping => "STOPPING",
            ProgramState::Exited => "EXITED",
            ProgramState::Fatal => "FATAL",
        }
    }
}

#[derive(Debug)]
struct ProgramHandle {
    config: ProgramConfig,
    state: ProgramState,
    pid: Option<i32>,
    retries: u32,
    start_time: Option<Instant>,
    /// Handles for the stdout/stderr log-writer tasks spawned alongside this
    /// process instance.  Stored here so that `stop_program` and the exit
    /// monitoring task can `await` them, ensuring all buffered log data is
    /// flushed to disk and any write errors are reported before we consider
    /// the process fully stopped.
    log_handles: Vec<tokio::task::JoinHandle<anyhow::Result<()>>>,
}

#[derive(Debug)]
pub struct Supervisor {
    config_path: PathBuf,
    programs: HashMap<String, ProgramHandle>,
    global_env: HashMap<String, String>,
    pidfile: Option<PathBuf>,
    sock_path: PathBuf,
    logfile: Option<PathBuf>,
    event_tx: broadcast::Sender<Event>,
    event_seq: StdArc<AtomicU64>,
}

/// Expand a list of `ProgramConfig` entries so that every entry with
/// `numprocs > 1` becomes `numprocs` individual entries named
/// `group:group_00`, `group:group_01`, …  Entries with `numprocs == 1`
/// keep their original name unchanged.
fn expand_programs(programs: Vec<ProgramConfig>) -> Vec<ProgramConfig> {
    let mut out = Vec::new();
    for program in programs {
        if program.numprocs <= 1 {
            out.push(program);
        } else {
            for i in 0..program.numprocs {
                let instance_name = format!("{}:{}_{:02}", program.name, program.name, i);
                let mut cfg = program.clone();
                cfg.name = instance_name;
                cfg.numprocs = 1;
                out.push(cfg);
            }
        }
    }
    out
}

impl Supervisor {
    pub fn new_from_config(
        config_path: PathBuf,
        programs: Vec<ProgramConfig>,
        global_env: HashMap<String, String>,
        pidfile: Option<PathBuf>,
        sock_path: PathBuf,
        logfile: Option<PathBuf>,
    ) -> Self {
        let (event_tx, _) = broadcast::channel(1024);
        let mut map = HashMap::new();
        for program in expand_programs(programs) {
            map.insert(
                program.name.clone(),
                ProgramHandle {
                    config: program,
                    state: ProgramState::Stopped,
                    pid: None,
                    retries: 0,
                    start_time: None,
                    log_handles: Vec::new(),
                },
            );
        }
        Self {
            config_path,
            programs: map,
            global_env,
            pidfile,
            sock_path,
            logfile,
            event_tx,
            event_seq: StdArc::new(AtomicU64::new(0)),
        }
    }

    pub fn autostart_programs(&self) -> Vec<String> {
        self.programs
            .values()
            .filter(|h| h.config.autostart)
            .map(|h| h.config.name.clone())
            .collect()
    }

    pub fn status(&self, name: Option<&str>) -> Vec<ProgramStatus> {
        match name {
            Some(target) => self
                .programs
                .get(target)
                .map(|program| vec![program_to_status(program)])
                .unwrap_or_default(),
            None => self
                .programs
                .values()
                .map(program_to_status)
                .collect(),
        }
    }

    pub fn program_config(&self, name: &str) -> Option<&ProgramConfig> {
        self.programs.get(name).map(|handle| &handle.config)
    }

    pub fn pidfile(&self) -> Option<&PathBuf> {
        self.pidfile.as_ref()
    }

    pub fn sock_path(&self) -> &PathBuf {
        &self.sock_path
    }

    pub fn logfile(&self) -> Option<&PathBuf> {
        self.logfile.as_ref()
    }

    pub fn events(&self) -> broadcast::Receiver<Event> {
        self.event_tx.subscribe()
    }
}

pub async fn start_program(
    supervisor: Arc<Mutex<Supervisor>>,
    name: &str,
) -> anyhow::Result<String> {
    start_program_inner(supervisor, name.to_string(), true).await
}

pub async fn restart_program(
    supervisor: Arc<Mutex<Supervisor>>,
    name: &str,
) -> anyhow::Result<String> {
    stop_program(supervisor.clone(), name).await?;
    start_program_inner(supervisor, name.to_string(), true).await
}

fn start_program_inner(
    supervisor: Arc<Mutex<Supervisor>>,
    name: String,
    reset_retries: bool,
) -> BoxFuture<'static, anyhow::Result<String>> {
    Box::pin(async move {
        // Phase 1: acquire lock, read config, set STARTING state, then release lock
        // before spawning so IPC commands (e.g. `ctl status`) are not blocked by fork/exec.
        let (config, global_env, event_tx, event_seq, autorestart, exitcodes) = {
            let mut guard = supervisor.lock().await;
            let global_env = guard.global_env.clone();
            let event_tx = guard.event_tx.clone();
            let event_seq = guard.event_seq.clone();
            let Some(program) = guard.programs.get_mut(&name) else {
                anyhow::bail!("unknown program {}", name);
            };
            if matches!(program.state, ProgramState::Running | ProgramState::Starting | ProgramState::Stopping) {
                return Ok(format!("{} already running", name));
            }
            // When called from the autorestart path (reset_retries=false), respect a
            // manual stop: if the program is already STOPPED don't bring it back.
            if !reset_retries && matches!(program.state, ProgramState::Stopped) {
                return Ok(format!("{} is stopped", name));
            }
            if reset_retries {
                program.retries = 0;
            }
            program.state = ProgramState::Starting;
            emit_event(&event_seq, &event_tx, &program.config.name, program.state, program.pid);
            let config = program.config.clone();
            let autorestart = program.config.autorestart;
            let exitcodes = program.config.exitcodes.clone();
            (config, global_env, event_tx, event_seq, autorestart, exitcodes)
            // guard dropped here — lock released before fork/exec
        };

        // Phase 2: spawn the process without holding the lock
        let spawn = process::spawn_program(&config, &global_env);

        // Phase 3: re-acquire lock to record pid and start time; keep state as
        // STARTING until startsecs have elapsed (see the readiness task below).
        let spawn = match spawn {
            Ok(spawn) => spawn,
            Err(err) => {
                let mut guard = supervisor.lock().await;
                if let Some(program) = guard.programs.get_mut(&name) {
                    program.state = ProgramState::Fatal;
                    emit_event(&event_seq, &event_tx, &program.config.name, program.state, program.pid);
                }
                return Err(err);
            }
        };
        let program_pid = spawn.pid;
        let startsecs = config.startsecs;
        // Destructure so log_handles can be stored in ProgramHandle while
        // child stays local for the monitoring task below.
        let crate::process::SpawnedProcess { pid: _, child: mut spawn_child, log_handles } = spawn;
        {
            let mut guard = supervisor.lock().await;
            if let Some(program) = guard.programs.get_mut(&name) {
                program.pid = Some(program_pid);
                // State stays STARTING; the readiness task below will flip it to
                // RUNNING once startsecs have elapsed and the process is still alive.
                program.start_time = Some(Instant::now());
                // Store log handles so stop_program and the monitoring task
                // can await them and surface any write errors.
                program.log_handles = log_handles;
                // No event emitted here — STARTING was already emitted in Phase 1.
            }
        }

        // Readiness task: transition STARTING → RUNNING after startsecs.
        // If the process dies first, handle_exit clears the pid/state before
        // this task wakes up, and the `pid == program_pid` guard prevents a
        // stale transition.
        {
            let sup = supervisor.clone();
            let n = name.clone();
            let etx = event_tx.clone();
            let eseq = event_seq.clone();
            tokio::spawn(async move {
                if startsecs > 0 {
                    tokio::time::sleep(Duration::from_secs(startsecs)).await;
                }
                let mut guard = sup.lock().await;
                if let Some(program) = guard.programs.get_mut(&n) {
                    if program.pid == Some(program_pid)
                        && matches!(program.state, ProgramState::Starting)
                    {
                        program.state = ProgramState::Running;
                        emit_event(&eseq, &etx, &program.config.name, program.state, program.pid);
                    }
                }
            });
        }

        let program_name = config.name.clone();
        let supervisor_clone = supervisor.clone();
        tokio::spawn(async move {
            let status = spawn_child.wait().await.ok();
            // Take log handles out of ProgramHandle and await them so that
            // all buffered output is flushed and any I/O errors are surfaced
            // before we decide whether to restart.
            let handles = {
                let mut guard = supervisor_clone.lock().await;
                guard.programs.get_mut(&program_name)
                    .map(|p| std::mem::take(&mut p.log_handles))
                    .unwrap_or_default()
            };
            await_log_handles(handles, &program_name).await;
            let exit_status = status
                .as_ref()
                .and_then(|s| s.code())
                .unwrap_or(-1);
            let expected = exitcodes.contains(&exit_status);
            let restart_delay = handle_exit(
                supervisor_clone.clone(),
                program_name.clone(),
                program_pid,
                exit_status,
                expected,
                autorestart,
            )
            .await;
            if let Some(delay) = restart_delay {
                tokio::time::sleep(delay).await;
                let _ = start_program_inner(supervisor_clone, program_name, false).await;
            }
        });
        Ok(format!("{} started", name))
    })
}

pub async fn stop_program(
    supervisor: Arc<Mutex<Supervisor>>,
    name: &str,
) -> anyhow::Result<String> {
    // Transition to STOPPING (keep pid visible) and extract kill parameters.
    let (pid, stop_signal, wait_secs, killasgroup) = {
        let mut guard = supervisor.lock().await;
        let event_tx = guard.event_tx.clone();
        let event_seq = guard.event_seq.clone();
        let Some(program) = guard.programs.get_mut(name) else {
            anyhow::bail!("unknown program {}", name);
        };
        let pid = program.pid;
        let stop_signal = program.config.stopsignal.clone();
        let wait_secs = program.config.stopwaitsecs;
        let killasgroup = program.config.killasgroup;
        program.retries = 0;
        program.start_time = None;
        if pid.is_some() {
            // Keep pid visible while we wait for the process to die.
            program.state = ProgramState::Stopping;
            emit_event(&event_seq, &event_tx, &program.config.name, program.state, pid);
        } else {
            program.state = ProgramState::Stopped;
            emit_event(&event_seq, &event_tx, &program.config.name, program.state, None);
        }
        (pid, stop_signal, wait_secs, killasgroup)
    };
    if let Some(pid) = pid {
        kill_process_tree(pid, &stop_signal, killasgroup);
        // Poll for process death instead of always sleeping the full stopwaitsecs.
        // This lets shutdown complete in milliseconds when the process exits
        // promptly, while still sending SIGKILL if it refuses to die.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(wait_secs);
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if !process_alive(pid) {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                break;
            }
        }
        kill_process_tree(pid, "KILL", killasgroup);
        // Process is confirmed dead (or SIGKILL just sent). Clear pid, mark STOPPED,
        // and take log handles — all in one lock to avoid a second acquisition.
        let handles = {
            let mut guard = supervisor.lock().await;
            let event_tx = guard.event_tx.clone();
            let event_seq = guard.event_seq.clone();
            let handles = guard.programs.get_mut(name).map(|program| {
                program.pid = None;
                program.state = ProgramState::Stopped;
                emit_event(&event_seq, &event_tx, &program.config.name, program.state, None);
                std::mem::take(&mut program.log_handles)
            }).unwrap_or_default();
            handles
        };
        await_log_handles(handles, name).await;
    }
    Ok(format!("{} stopped", name))
}

pub async fn start_all(supervisor: Arc<Mutex<Supervisor>>) -> anyhow::Result<String> {
    let names: Vec<String> = {
        let guard = supervisor.lock().await;
        guard.programs.keys().cloned().collect()
    };
    for name in names {
        start_program(supervisor.clone(), &name).await?;
    }
    Ok("started all programs".to_string())
}

pub async fn stop_all(supervisor: Arc<Mutex<Supervisor>>) -> anyhow::Result<String> {
    let names: Vec<String> = {
        let guard = supervisor.lock().await;
        guard.programs.keys().cloned().collect()
    };
    let mut tasks = Vec::with_capacity(names.len());
    for name in names {
        let sup = supervisor.clone();
        tasks.push(tokio::spawn(
            async move { stop_program(sup, &name).await },
        ));
    }
    for task in tasks {
        task.await.map_err(|e| anyhow::anyhow!("stop task panicked: {e}"))??;
    }
    Ok("stopped all programs".to_string())
}

pub async fn shutdown(supervisor: Arc<Mutex<Supervisor>>) -> anyhow::Result<()> {
    let (pidfile, sock_path) = {
        let guard = supervisor.lock().await;
        (guard.pidfile.clone(), guard.sock_path.clone())
    };
    let _ = stop_all(supervisor).await;
    if let Some(pidfile) = pidfile {
        let _ = tokio::fs::remove_file(pidfile).await;
    }
    let _ = tokio::fs::remove_file(sock_path).await;
    Ok(())
}

pub async fn avail(supervisor: Arc<Mutex<Supervisor>>) -> Vec<String> {
    let guard = supervisor.lock().await;
    let mut names = guard.programs.keys().cloned().collect::<Vec<_>>();
    names.sort();
    names
}

pub async fn clear_logs(
    supervisor: Arc<Mutex<Supervisor>>,
    name: Option<&str>,
) -> anyhow::Result<String> {
    let logs = {
        let guard = supervisor.lock().await;
        let programs = match name {
            Some(target) => guard
                .programs
                .get(target)
                .map(|p| vec![p.config.clone()])
                .unwrap_or_default(),
            None => guard.programs.values().map(|p| p.config.clone()).collect(),
        };
        programs
    };
    for program in logs {
        if let Some(path) = program.stdout_log {
            truncate_file(&path).await?;
        }
        if let Some(path) = program.stderr_log {
            truncate_file(&path).await?;
        }
    }
    Ok("cleared".to_string())
}

pub async fn add_program(supervisor: Arc<Mutex<Supervisor>>, name: &str) -> anyhow::Result<String> {
    let config_path = {
        let guard = supervisor.lock().await;
        guard.config_path.clone()
    };
    let config = config::load(Some(&config_path))?;
    let program = config
        .programs
        .into_iter()
        .find(|p| p.name == name)
        .ok_or_else(|| anyhow::anyhow!("program {} not found in config", name))?;
    let mut guard = supervisor.lock().await;
    if guard.programs.contains_key(name) {
        return Ok(format!("{} already exists", name));
    }
    guard.programs.insert(
        name.to_string(),
        ProgramHandle {
            config: program,
            state: ProgramState::Stopped,
            pid: None,
            retries: 0,
            start_time: None,
            log_handles: Vec::new(),
        },
    );
    Ok(format!("{} added", name))
}

pub async fn remove_program(
    supervisor: Arc<Mutex<Supervisor>>,
    name: &str,
) -> anyhow::Result<String> {
    let mut guard = supervisor.lock().await;
    let event_tx = guard.event_tx.clone();
    let event_seq = guard.event_seq.clone();
    if let Some(program) = guard.programs.remove(name) {
        emit_event(&event_seq, &event_tx, &program.config.name, ProgramState::Stopped, None);
        return Ok(format!("{} removed", name));
    }
    anyhow::bail!("unknown program {}", name);
}

pub async fn signal_program(
    supervisor: Arc<Mutex<Supervisor>>,
    name: &str,
    signal: &str,
) -> anyhow::Result<String> {
    let mut guard = supervisor.lock().await;
    let Some(program) = guard.programs.get_mut(name) else {
        anyhow::bail!("unknown program {}", name);
    };
    let Some(pid) = program.pid else {
        anyhow::bail!("program {} not running", name);
    };
    send_signal(pid, signal)?;
    Ok(format!("{} signaled {}", name, signal))
}

pub async fn signal_all(
    supervisor: Arc<Mutex<Supervisor>>,
    signal: &str,
) -> anyhow::Result<String> {
    let mut guard = supervisor.lock().await;
    for program in guard.programs.values_mut() {
        if let Some(pid) = program.pid {
            send_signal(pid, signal)?;
        }
    }
    Ok(format!("signaled all with {}", signal))
}

pub async fn reread(supervisor: Arc<Mutex<Supervisor>>) -> anyhow::Result<RereadSummary> {
    let config_path = {
        let guard = supervisor.lock().await;
        guard.config_path.clone()
    };
    let config = config::load(Some(&config_path))?;
    let new_map = expand_programs(config.programs)
        .into_iter()
        .map(|p| (p.name.clone(), p))
        .collect::<HashMap<_, _>>();
    let guard = supervisor.lock().await;
    let current_names: HashSet<_> = guard.programs.keys().cloned().collect();
    let new_names: HashSet<_> = new_map.keys().cloned().collect();

    let added = new_names
        .difference(&current_names)
        .cloned()
        .collect::<Vec<_>>();
    let removed = current_names
        .difference(&new_names)
        .cloned()
        .collect::<Vec<_>>();
    let changed = new_names
        .intersection(&current_names)
        .filter_map(|name| {
            let existing = guard.programs.get(name)?;
            let updated = new_map.get(name)?;
            if existing.config != *updated {
                Some(name.clone())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    Ok(RereadSummary {
        added,
        changed,
        removed,
    })
}

pub async fn update(supervisor: Arc<Mutex<Supervisor>>) -> anyhow::Result<UpdateSummary> {
    let config_path = {
        let guard = supervisor.lock().await;
        guard.config_path.clone()
    };
    let config = config::load(Some(&config_path))?;
    let mut summary = UpdateSummary {
        started: Vec::new(),
        stopped: Vec::new(),
        restarted: Vec::new(),
        removed: Vec::new(),
        added: Vec::new(),
        changed: Vec::new(),
    };

    let mut to_restart = Vec::new();
    let mut to_start = Vec::new();
    let mut to_stop = Vec::new();
    {
        let mut guard = supervisor.lock().await;
        let mut new_map = HashMap::new();
        for program in expand_programs(config.programs) {
            new_map.insert(program.name.clone(), program);
        }

        let current_names: HashSet<_> = guard.programs.keys().cloned().collect();
        let new_names: HashSet<_> = new_map.keys().cloned().collect();

        for name in current_names.difference(&new_names) {
            to_stop.push(name.clone());
            summary.removed.push(name.clone());
        }
        for name in new_names.difference(&current_names) {
            if let Some(program) = new_map.remove(name) {
                summary.added.push(name.clone());
                let autostart = program.autostart;
                guard.programs.insert(
                    name.clone(),
                    ProgramHandle {
                        config: program,
                        state: ProgramState::Stopped,
                        pid: None,
                        retries: 0,
                        start_time: None,
                        log_handles: Vec::new(),
                    },
                );
                if autostart {
                    to_start.push(name.clone());
                }
            }
        }
        for name in new_names.intersection(&current_names) {
            if let Some(program) = new_map.remove(name) {
                let handle = guard.programs.get_mut(name).expect("handle exists");
                if handle.config != program {
                    summary.changed.push(name.clone());
                    handle.config = program;
                    if matches!(handle.state, ProgramState::Running | ProgramState::Starting) {
                        to_restart.push(name.clone());
                    }
                }
            }
        }
    }

    for name in to_stop {
        stop_program(supervisor.clone(), &name).await?;
        summary.stopped.push(name);
    }
    for name in to_start {
        start_program(supervisor.clone(), &name).await?;
        summary.started.push(name);
    }
    for name in to_restart {
        restart_program(supervisor.clone(), &name).await?;
        summary.restarted.push(name);
    }

    Ok(summary)
}

pub async fn reload(supervisor: Arc<Mutex<Supervisor>>) -> anyhow::Result<ReloadSummary> {
    let reread = reread(supervisor.clone()).await?;
    let update = update(supervisor).await?;
    Ok(ReloadSummary { reread, update })
}

async fn handle_exit(
    supervisor: Arc<Mutex<Supervisor>>,
    name: String,
    pid: i32,
    _exit_status: i32,
    expected: bool,
    autorestart: Autorestart,
) -> Option<Duration> {
    let retries = {
        let mut guard = supervisor.lock().await;
        let event_tx = guard.event_tx.clone();
        let event_seq = guard.event_seq.clone();
        let Some(handle) = guard.programs.get_mut(&name) else {
            return None;
        };
        if handle.pid != Some(pid) {
            return None;
        }
        // stop_program handles the STOPPING → STOPPED transition itself.
        if matches!(handle.state, ProgramState::Stopping) {
            return None;
        }
        handle.pid = None;
        let elapsed = handle
            .start_time
            .map(|t| t.elapsed())
            .unwrap_or_else(|| Duration::from_secs(0));
        handle.start_time = None;
        if autorestart == Autorestart::Never {
            handle.state = ProgramState::Exited;
            emit_event(&event_seq, &event_tx, &handle.config.name, handle.state, handle.pid);
            return None;
        }
        if autorestart == Autorestart::Unexpected && expected {
            handle.state = ProgramState::Exited;
            emit_event(&event_seq, &event_tx, &handle.config.name, handle.state, handle.pid);
            return None;
        }
        let early_exit = elapsed < Duration::from_secs(handle.config.startsecs);
        if early_exit {
            handle.retries += 1;
        } else {
            handle.retries = 0;
        }
        if handle.retries > handle.config.startretries {
            handle.state = ProgramState::Fatal;
            emit_event(&event_seq, &event_tx, &handle.config.name, handle.state, handle.pid);
            return None;
        }
        handle.state = ProgramState::Backoff;
        emit_event(&event_seq, &event_tx, &handle.config.name, handle.state, handle.pid);
        handle.retries
    };
    // Exponential backoff: 1s × 2^(retries-1), capped at 32s.
    // retries=1 → 1s, retries=2 → 2s, retries=3 → 4s, …, retries≥6 → 32s.
    // retries=0 means a healthy run that exited unexpectedly → 1s.
    let delay_secs = if retries == 0 {
        1u64
    } else {
        (1u64 << (retries as u64).saturating_sub(1).min(5)).min(32)
    };
    Some(Duration::from_secs(delay_secs))
}

fn program_to_status(program: &ProgramHandle) -> ProgramStatus {
    let uptime = if matches!(program.state, ProgramState::Running | ProgramState::Starting) {
        program
            .start_time
            .map(|t| t.elapsed().as_secs())
    } else {
        None
    };
    ProgramStatus {
        name: program.config.name.clone(),
        state: program.state.as_str().to_string(),
        pid: program.pid,
        uptime,
    }
}

fn emit_event(
    seq: &AtomicU64,
    tx: &broadcast::Sender<Event>,
    name: &str,
    state: ProgramState,
    pid: Option<i32>,
) {
    let event_type = match state {
        ProgramState::Starting => "PROCESS_STATE_STARTING",
        ProgramState::Running => "PROCESS_STATE_RUNNING",
        ProgramState::Backoff => "PROCESS_STATE_BACKOFF",
        ProgramState::Stopping => "PROCESS_STATE_STOPPING",
        ProgramState::Stopped => "PROCESS_STATE_STOPPED",
        ProgramState::Exited => "PROCESS_STATE_EXITED",
        ProgramState::Fatal => "PROCESS_STATE_FATAL",
    };
    let serial = seq.fetch_add(1, Ordering::Relaxed) + 1;
    let payload = format!(
        "processname:{} groupname:{} pid:{} state:{}",
        name,
        name,
        pid.map(|p| p.to_string()).unwrap_or_else(|| "0".to_string()),
        state.as_str()
    );
    let event = Event {
        event_type: event_type.to_string(),
        serial,
        name: name.to_string(),
        group: name.to_string(),
        state: state.as_str().to_string(),
        pid,
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        payload,
    };
    let _ = tx.send(event);
}

fn process_alive(pid: i32) -> bool {
    #[cfg(unix)]
    {
        use nix::sys::signal::kill;
        use nix::unistd::Pid;
        kill(Pid::from_raw(pid), None).is_ok()
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        false
    }
}

async fn truncate_file(path: &PathBuf) -> anyhow::Result<()> {
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

#[cfg(unix)]
fn parse_signal(signal: &str) -> anyhow::Result<nix::sys::signal::Signal> {
    use nix::sys::signal::Signal;
    match signal.to_uppercase().as_str() {
        "HUP" | "SIGHUP" => Ok(Signal::SIGHUP),
        "INT" | "SIGINT" => Ok(Signal::SIGINT),
        "QUIT" | "SIGQUIT" => Ok(Signal::SIGQUIT),
        "KILL" | "SIGKILL" => Ok(Signal::SIGKILL),
        "TERM" | "SIGTERM" => Ok(Signal::SIGTERM),
        "USR1" | "SIGUSR1" => Ok(Signal::SIGUSR1),
        "USR2" | "SIGUSR2" => Ok(Signal::SIGUSR2),
        "STOP" | "SIGSTOP" => Ok(Signal::SIGSTOP),
        "CONT" | "SIGCONT" => Ok(Signal::SIGCONT),
        other => anyhow::bail!("unsupported signal {other}"),
    }
}

async fn await_log_handles(
    handles: Vec<tokio::task::JoinHandle<anyhow::Result<()>>>,
    name: &str,
) {
    for h in handles {
        match h.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::warn!("log write error for {name}: {e}"),
            Err(e) => tracing::warn!("log task for {name} panicked: {e}"),
        }
    }
}

fn send_signal(pid: i32, signal: &str) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use nix::unistd::Pid;
        nix::sys::signal::kill(Pid::from_raw(pid), parse_signal(signal)?)?;
    }
    Ok(())
}

/// Kill a process and all of its descendants, regardless of which process
/// group they belong to.  This handles programs (like dockerd) that call
/// setpgid/setsid internally and therefore escape a simple killpg().
///
/// Strategy:
///   1. If `group` is true, also send to the whole process group (-pid) so
///      that processes still in the original group are caught in one shot.
///   2. Walk /proc to find every process whose ancestor chain leads back to
///      `root_pid`, and send the signal to each of them.
fn kill_process_tree(root_pid: i32, signal: &str, group: bool) {
    #[cfg(unix)]
    {
        use nix::sys::signal::kill;
        use nix::unistd::Pid;

        let Ok(signal) = parse_signal(signal) else { return };

        // Optional: kill the entire original process group.
        if group {
            let _ = kill(Pid::from_raw(-root_pid), signal);
        }

        // Build a pid→children map from /proc so we can do a depth-first
        // traversal without repeated /proc scans.
        let mut children: std::collections::HashMap<i32, Vec<i32>> =
            std::collections::HashMap::new();
        if let Ok(entries) = std::fs::read_dir("/proc") {
            for entry in entries.flatten() {
                let Ok(pid) = entry.file_name().to_str().unwrap_or("").parse::<i32>() else {
                    continue;
                };
                let Ok(status) = std::fs::read_to_string(format!("/proc/{pid}/status")) else {
                    continue;
                };
                for line in status.lines() {
                    if let Some(rest) = line.strip_prefix("PPid:") {
                        if let Ok(ppid) = rest.trim().parse::<i32>() {
                            children.entry(ppid).or_default().push(pid);
                        }
                        break;
                    }
                }
            }
        }

        // Depth-first: kill deepest descendants first so they cannot spawn
        // new children before we reach their parents.
        fn kill_subtree(
            pid: i32,
            sig: nix::sys::signal::Signal,
            children: &std::collections::HashMap<i32, Vec<i32>>,
        ) {
            if let Some(kids) = children.get(&pid) {
                for &child in kids {
                    kill_subtree(child, sig, children);
                }
            }
            let _ = nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), sig);
        }

        // Kill descendants of root_pid (but not root_pid itself — that was
        // already covered by the killpg above, or we'll send it below).
        if let Some(kids) = children.get(&root_pid) {
            for &child in kids {
                kill_subtree(child, signal, &children);
            }
        }

        // Finally kill the root itself (in case killpg didn't reach it).
        let _ = kill(Pid::from_raw(root_pid), signal);
    }
}
