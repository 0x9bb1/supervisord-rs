use crate::config::ProgramConfig;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::sync::broadcast;
use tokio::time::Instant;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgramState {
    Stopped,
    Starting,
    Running,
    Backoff,
    Stopping,
    Exited,
    Fatal,
}

impl ProgramState {
    pub fn as_str(self) -> &'static str {
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
pub struct ProgramHandle {
    pub config: ProgramConfig,
    pub state: ProgramState,
    pub pid: Option<i32>,
    pub retries: u32,
    pub start_time: Option<Instant>,
}

#[derive(Debug)]
pub struct Rvisor {
    pub config_path: PathBuf,
    pub programs: HashMap<String, ProgramHandle>,
    pub global_env: HashMap<String, String>,
    pub pidfile: Option<PathBuf>,
    pub sock_path: PathBuf,
    pub logfile: Option<PathBuf>,
    pub event_tx: broadcast::Sender<Event>,
    pub event_seq: u64,
}

/// Expand a list of `ProgramConfig` entries so that every entry with
/// `numprocs > 1` becomes `numprocs` individual entries named
/// `group:group_00`, `group:group_01`, …  Entries with `numprocs == 1`
/// keep their original name unchanged.
pub(crate) fn expand_programs(programs: Vec<ProgramConfig>) -> Vec<ProgramConfig> {
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

impl Rvisor {
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
            event_seq: 0,
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
            None => self.programs.values().map(program_to_status).collect(),
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

    pub fn emit_event(&mut self, name: &str, state: ProgramState, pid: Option<i32>) {
        self.event_seq += 1;
        let serial = self.event_seq;
        let event_type = match state {
            ProgramState::Starting => "PROCESS_STATE_STARTING",
            ProgramState::Running => "PROCESS_STATE_RUNNING",
            ProgramState::Backoff => "PROCESS_STATE_BACKOFF",
            ProgramState::Stopping => "PROCESS_STATE_STOPPING",
            ProgramState::Stopped => "PROCESS_STATE_STOPPED",
            ProgramState::Exited => "PROCESS_STATE_EXITED",
            ProgramState::Fatal => "PROCESS_STATE_FATAL",
        };
        let payload = format!(
            "processname:{} groupname:{} pid:{} state:{}",
            name,
            name,
            pid.map(|p| p.to_string())
                .unwrap_or_else(|| "0".to_string()),
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
        let _ = self.event_tx.send(event);
    }
}

fn program_to_status(program: &ProgramHandle) -> ProgramStatus {
    let uptime = if matches!(
        program.state,
        ProgramState::Running | ProgramState::Starting
    ) {
        program.start_time.map(|t| t.elapsed().as_secs())
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

pub fn process_alive(pid: i32) -> bool {
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

#[cfg(unix)]
pub(crate) fn parse_signal(signal: &str) -> anyhow::Result<nix::sys::signal::Signal> {
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

pub fn send_signal(pid: i32, signal: &str) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use nix::unistd::Pid;
        nix::sys::signal::kill(Pid::from_raw(pid), parse_signal(signal)?)?;
    }
    #[cfg(not(unix))]
    {
        let _ = (pid, signal);
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
pub fn kill_process_tree(root_pid: i32, signal: &str, group: bool) {
    #[cfg(unix)]
    {
        use nix::sys::signal::kill;
        use nix::unistd::Pid;

        let Ok(signal) = parse_signal(signal) else {
            return;
        };

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
    #[cfg(not(unix))]
    {
        let _ = (root_pid, signal, group);
    }
}
