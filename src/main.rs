use clap::{Parser, Subcommand};
use std::path::{Path, PathBuf};
use std::io::Write;
use std::sync::Arc;
use tokio::sync::Mutex;

use rvisor::{config, ipc, logging, service, supervisor};

#[derive(Parser, Debug)]
#[command(name = "supervisord", version, about = "Rust-based supervisor daemon")]
struct Args {
    #[arg(short = 'c', long = "configuration")]
    configuration: Option<PathBuf>,

    #[arg(short = 'd', long = "daemon")]
    daemon: bool,

    #[arg(long = "env-file")]
    env_file: Option<PathBuf>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run the daemon (foreground by default)
    Run,
    /// Control a running daemon
    Ctl {
        #[command(subcommand)]
        command: Option<CtlCommand>,

        #[arg(short = 's', long = "serverurl")]
        serverurl: Option<String>,

        #[arg(short = 'u', long = "user")]
        user: Option<String>,

        #[arg(short = 'P', long = "password")]
        password: Option<String>,

        #[arg(short = 'v', long = "verbose")]
        verbose: bool,

        #[arg(long = "json")]
        json: bool,
    },
    /// Initialize a configuration template
    Init {
        #[arg(short = 'o', long = "output")]
        output: Option<PathBuf>,
    },
    /// Install/uninstall/start/stop service
    Service {
        #[command(subcommand)]
        command: Option<ServiceCommand>,
    },
    /// Show version
    Version,
}

#[derive(Subcommand, Debug)]
enum ServiceCommand {
    Install,
    Uninstall,
    Start,
    Stop,
    Status,
    Enable,
    Disable,
    Restart,
    Reload,
}

#[derive(Subcommand, Debug)]
#[command(disable_help_subcommand = true)]
enum CtlCommand {
    Start {
        #[arg(value_name = "PROGRAM")]
        program: Option<String>,
    },
    Restart {
        #[arg(value_name = "PROGRAM")]
        program: Option<String>,
    },
    Status {
        #[arg(value_name = "PROGRAM")]
        program: Option<String>,
    },
    Stop {
        #[arg(value_name = "PROGRAM")]
        program: Option<String>,
    },
    Reread,
    Update,
    Reload,
    Shutdown,
    Pid,
    Signal {
        #[arg(value_name = "SIGNAL")]
        signal: String,
        #[arg(value_name = "PROGRAM")]
        program: Option<String>,
    },
    Logtail {
        #[arg(value_name = "PROGRAM")]
        program: String,
        #[arg(long = "lines", default_value_t = 10)]
        lines: usize,
        #[arg(long = "bytes")]
        bytes: Option<u64>,
        #[arg(long = "since")]
        since: Option<u64>,
        #[arg(long = "stderr")]
        stderr: bool,
        #[arg(long = "follow")]
        follow: bool,
    },
    Tail {
        #[arg(value_name = "PROGRAM")]
        program: String,
        #[arg(long = "lines", default_value_t = 10)]
        lines: usize,
        #[arg(long = "stderr")]
        stderr: bool,
        #[arg(long = "follow")]
        follow: bool,
    },
    Maintail {
        #[arg(long = "lines", default_value_t = 10)]
        lines: usize,
        #[arg(long = "follow")]
        follow: bool,
    },
    Events,
    Avail,
    Clear {
        #[arg(value_name = "PROGRAM")]
        program: Option<String>,
    },
    Add {
        #[arg(value_name = "PROGRAM")]
        program: String,
    },
    Remove {
        #[arg(value_name = "PROGRAM")]
        program: String,
    },
    Fg {
        #[arg(value_name = "PROGRAM")]
        program: String,
    },
    Shell,
    Help {
        #[arg(value_name = "COMMAND")]
        command: Option<String>,
    },
}

fn main() -> anyhow::Result<()> {
    logging::init();
    let args = Args::parse();
    let config_path = resolve_config_path(args.configuration.as_deref());
    let will_daemonize = matches!(args.command, Some(Command::Run) | None) && args.daemon;

    if will_daemonize {
        daemonize()?;
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(run_async(args, config_path))
}

async fn run_async(args: Args, config_path: PathBuf) -> anyhow::Result<()> {
    match args.command {
        Some(Command::Init { output }) => {
            let template = config::template();
            if let Some(path) = output {
                std::fs::write(path, template)?;
            } else {
                println!("{template}");
            }
            return Ok(());
        }
        Some(Command::Version) => {
            println!("{}", env!("CARGO_PKG_VERSION"));
            return Ok(());
        }
        Some(Command::Service { command }) => {
            let command = command.unwrap_or(ServiceCommand::Install);
            let command = match command {
                ServiceCommand::Install => service::ServiceCommand::Install,
                ServiceCommand::Uninstall => service::ServiceCommand::Uninstall,
                ServiceCommand::Start => service::ServiceCommand::Start,
                ServiceCommand::Stop => service::ServiceCommand::Stop,
                ServiceCommand::Status => service::ServiceCommand::Status,
                ServiceCommand::Enable => service::ServiceCommand::Enable,
                ServiceCommand::Disable => service::ServiceCommand::Disable,
                ServiceCommand::Restart => service::ServiceCommand::Restart,
                ServiceCommand::Reload => service::ServiceCommand::Reload,
            };
            let result = service::run(command, Some(&config_path))?;
            println!("{result}");
            return Ok(());
        }
        Some(Command::Ctl {
            command,
            serverurl,
            verbose: _,
            json,
            ..
        }) => {
            let sock_path = resolve_sock_path(&config_path, serverurl.as_deref()).await?;
            let command = command.unwrap_or(CtlCommand::Status { program: None });
            run_ctl(&sock_path, command, json).await?;
            return Ok(());
        }
        Some(Command::Run) | None => {}
    }

    let config = config::load(Some(&config_path))?;
    apply_umask(config.supervisord.umask)?;
    ensure_minfds(config.supervisord.minfds)?;
    if let Some(pidfile) = &config.supervisord.pidfile {
        check_single_instance(pidfile)?;
        write_pidfile(pidfile)?;
    }
    let global_env = load_env_file(args.env_file.as_deref())?;
    let supervisor = Arc::new(Mutex::new(supervisor::Supervisor::new_from_config(
        config_path,
        config.programs.clone(),
        global_env,
        config.supervisord.pidfile.clone(),
        config.supervisord.sock_path.clone(),
        config.supervisord.logfile.clone(),
    )));
    let autostart = {
        let guard = supervisor.lock().await;
        guard.autostart_programs()
    };
    for name in autostart {
        supervisor::start_program(supervisor.clone(), &name).await?;
    }

    let sock_path = config.supervisord.sock_path.clone();
    let allowed_uids = config.supervisord.allowed_uids.clone();
    setup_signal_handlers(supervisor.clone());
    ipc::run_server(&sock_path, supervisor, allowed_uids).await?;
    Ok(())
}

async fn resolve_sock_path(
    config_path: &Path,
    serverurl: Option<&str>,
) -> anyhow::Result<PathBuf> {
    if let Some(url) = serverurl {
        if let Some(path) = url.strip_prefix("unix://") {
            return Ok(PathBuf::from(path));
        }
        return Ok(PathBuf::from(url));
    }
    let config = config::load(Some(config_path))?;
    Ok(config.supervisord.sock_path)
}

fn resolve_config_path(explicit: Option<&Path>) -> PathBuf {
    if let Some(path) = explicit {
        return path.to_path_buf();
    }
    if let Ok(path) = std::env::var("RVISOR_CONFIG") {
        if !path.trim().is_empty() {
            return PathBuf::from(path);
        }
    }
    if let Some(path) = find_default_config_path() {
        return path;
    }
    PathBuf::from("supervisord.toml")
}

fn find_default_config_path() -> Option<PathBuf> {
    let cwd = std::env::current_dir().ok();
    let candidates = [
        cwd.as_ref().map(|dir| dir.join("supervisord.toml")),
        cwd.as_ref().map(|dir| dir.join("etc").join("supervisord.toml")),
        Some(PathBuf::from("/etc/supervisord.toml")),
        Some(PathBuf::from("/etc/rvisor/supervisord.toml")),
        Some(PathBuf::from("/etc/supervisor/supervisord.toml")),
        current_exe_relative("../etc/supervisord.toml"),
        current_exe_relative("../supervisord.toml"),
    ];
    for candidate in candidates.into_iter().flatten() {
        if candidate.exists() {
            return Some(candidate);
        }
    }
    None
}

fn current_exe_relative(path: &str) -> Option<PathBuf> {
    let exe = std::env::current_exe().ok()?;
    let dir = exe.parent()?;
    Some(dir.join(path))
}

async fn run_ctl(
    sock_path: &Path,
    command: CtlCommand,
    json: bool,
) -> anyhow::Result<()> {
    match command {
        CtlCommand::Shell => {
            run_shell(sock_path).await?;
            return Ok(());
        }
        CtlCommand::Help { command } => {
            print_help(command.as_deref());
            return Ok(());
        }
        other => run_ctl_inner(sock_path, other, json).await,
    }
}

async fn run_ctl_inner(
    sock_path: &Path,
    command: CtlCommand,
    json: bool,
) -> anyhow::Result<()> {
    let (command_name, program, lines, stream, follow, signal, offset, bytes, since, help_cmd) = match command {
        CtlCommand::Start { program } => ("start", program, None, None, None, None, None, None, None, None),
        CtlCommand::Restart { program } => ("restart", program, None, None, None, None, None, None, None, None),
        CtlCommand::Status { program } => ("status", program, None, None, None, None, None, None, None, None),
        CtlCommand::Stop { program } => ("stop", program, None, None, None, None, None, None, None, None),
        CtlCommand::Reread => ("reread", None, None, None, None, None, None, None, None, None),
        CtlCommand::Update => ("update", None, None, None, None, None, None, None, None, None),
        CtlCommand::Reload => ("reload", None, None, None, None, None, None, None, None, None),
        CtlCommand::Shutdown => ("shutdown", None, None, None, None, None, None, None, None, None),
        CtlCommand::Pid => ("pid", None, None, None, None, None, None, None, None, None),
        CtlCommand::Signal { signal, program } => {
            ("signal", program, None, None, None, Some(signal), None, None, None, None)
        }
        CtlCommand::Logtail {
            program,
            lines,
            bytes,
            since,
            stderr,
            follow,
        } => (
            "logtail",
            Some(program),
            Some(lines),
            Some(if stderr { "stderr" } else { "stdout" }.to_string()),
            Some(follow),
            None,
            Some(0),
            bytes,
            since,
            None,
        ),
        CtlCommand::Tail {
            program,
            lines,
            stderr,
            follow,
        } => (
            "logtail",
            Some(program),
            Some(lines),
            Some(if stderr { "stderr" } else { "stdout" }.to_string()),
            Some(follow),
            None,
            Some(0),
            None,
            None,
            None,
        ),
        CtlCommand::Maintail { lines, follow } => (
            "maintail",
            None,
            Some(lines),
            None,
            Some(follow),
            None,
            Some(0),
            None,
            None,
            None,
        ),
        CtlCommand::Events => ("events", None, None, None, None, None, None, None, None, None),
        CtlCommand::Avail => ("avail", None, None, None, None, None, None, None, None, None),
        CtlCommand::Clear { program } => ("clear", program, None, None, None, None, None, None, None, None),
        CtlCommand::Add { program } => ("add", Some(program), None, None, None, None, None, None, None, None),
        CtlCommand::Remove { program } => ("remove", Some(program), None, None, None, None, None, None, None, None),
        CtlCommand::Fg { program } => ("fg", Some(program), None, None, None, None, None, None, None, None),
        CtlCommand::Help { command } => ("help", None, None, None, None, None, None, None, None, command),
        CtlCommand::Shell => ("shell", None, None, None, None, None, None, None, None, None),
    };
    let request = ipc::Request {
        command: command_name.to_string(),
        program,
        lines,
        stream,
        follow,
        signal,
        offset,
        bytes,
        since,
    };
    if command_name == "help" {
        print_help(help_cmd.as_deref());
        return Ok(());
    }
    if command_name == "fg" {
        let program = request.program.clone().unwrap();
        let start = ipc::Request {
            command: "start".to_string(),
            program: Some(program.clone()),
            lines: None,
            stream: None,
            follow: None,
            signal: None,
            offset: None,
            bytes: None,
            since: None,
        };
        let response = ipc::send_request(sock_path, start).await?;
        if !response.ok {
            anyhow::bail!("{}", response.message);
        }
        let tail = ipc::Request {
            command: "logtail".to_string(),
            program: Some(program),
            lines: Some(10),
            stream: Some("stdout".to_string()),
            follow: Some(true),
            signal: None,
            offset: Some(0),
            bytes: None,
            since: None,
        };
        let framed = ipc::send_stream_request(sock_path, tail).await?;
        run_logtail_stream(framed).await?;
        return Ok(());
    }
    if (command_name == "logtail" || command_name == "maintail") && follow == Some(true) {
        let framed = ipc::send_stream_request(sock_path, request).await?;
        run_logtail_stream(framed).await?;
        return Ok(());
    }
    if command_name == "events" {
        let framed = ipc::send_stream_request(sock_path, request).await?;
        run_event_stream(framed).await?;
        return Ok(());
    } else {
        let response = ipc::send_request(sock_path, request).await?;
        if !response.ok {
            anyhow::bail!("{}", response.message);
        }
        if json {
            if let Some(data) = response.data {
                println!("{}", serde_json::to_string_pretty(&data)?);
            } else {
                println!("{}", &response.message);
            }
            return Ok(());
        }
        match command_name {
            "status" => {
                if let Some(data) = response.data {
                    let statuses: Vec<supervisor::ProgramStatus> =
                        serde_json::from_value(data)?;
                    for status in statuses {
                        let name = format!("{:<24}", status.name);
                        match status.state.as_str() {
                            "RUNNING" => {
                                let pid = status.pid.unwrap_or(0);
                                let uptime = status.uptime.unwrap_or(0);
                                let hours = uptime / 3600;
                                let mins = (uptime % 3600) / 60;
                                let secs = uptime % 60;
                                println!(
                                    "{}RUNNING   pid {}, uptime {:02}:{:02}:{:02}",
                                    name, pid, hours, mins, secs
                                );
                            }
                            "STOPPED" => {
                                println!("{}STOPPED   Not started", name);
                            }
                            "STARTING" => {
                                println!("{}STARTING  Starting", name);
                            }
                            "BACKOFF" => {
                                println!("{}BACKOFF   Backing off", name);
                            }
                            "FATAL" => {
                                println!("{}FATAL     Exited too quickly", name);
                            }
                            other => {
                                println!("{}{}   Unknown", name, other);
                            }
                        }
                    }
                }
            }
            "reread" => {
                if let Some(data) = response.data {
                    let summary: supervisor::RereadSummary = serde_json::from_value(data)?;
                    for name in summary.added {
                        println!("{}: available", name);
                    }
                    for name in summary.changed {
                        println!("{}: changed", name);
                    }
                    for name in summary.removed {
                        println!("{}: removed", name);
                    }
                }
            }
            "update" => {
                if let Some(data) = response.data {
                    let summary: supervisor::UpdateSummary = serde_json::from_value(data)?;
                    for name in summary.added {
                        println!("{}: added", name);
                    }
                    for name in summary.removed {
                        println!("{}: removed", name);
                    }
                    for name in summary.started {
                        println!("{}: started", name);
                    }
                    for name in summary.stopped {
                        println!("{}: stopped", name);
                    }
                    for name in summary.restarted {
                        println!("{}: restarted", name);
                    }
                }
            }
            "reload" => {
                if let Some(data) = response.data {
                    let summary: supervisor::ReloadSummary = serde_json::from_value(data)?;
                    println!("reread added: {:?}", summary.reread.added);
                    println!("reread changed: {:?}", summary.reread.changed);
                    println!("reread removed: {:?}", summary.reread.removed);
                    println!("update added: {:?}", summary.update.added);
                    println!("update removed: {:?}", summary.update.removed);
                    println!("update started: {:?}", summary.update.started);
                    println!("update stopped: {:?}", summary.update.stopped);
                    println!("update restarted: {:?}", summary.update.restarted);
                }
            }
            "pid" => {
                if let Some(data) = response.data {
                    let pid: u32 = serde_json::from_value(data)?;
                    println!("{pid}");
                }
            }
            "logtail" => {
                if let Some(data) = response.data {
                    let reply: ipc::LogTailReply = serde_json::from_value(data)?;
                    for line in reply.lines {
                        println!("{line}");
                    }
                }
            }
            "maintail" => {
                if let Some(data) = response.data {
                    let reply: ipc::LogTailReply = serde_json::from_value(data)?;
                    for line in reply.lines {
                        println!("{line}");
                    }
                }
            }
            "avail" => {
                if let Some(data) = response.data {
                    let names: Vec<String> = serde_json::from_value(data)?;
                    for name in names {
                        println!("{name}");
                    }
                }
            }
            _ => {
                println!("{}", response.message);
            }
        }
        Ok(())
    }
}

async fn run_logtail_stream(
    mut framed: tokio_util::codec::Framed<tokio::net::UnixStream, tokio_util::codec::LengthDelimitedCodec>,
) -> anyhow::Result<()> {
    use futures::StreamExt;
    while let Some(frame) = framed.next().await {
        let frame = frame?;
        let response: ipc::Response = serde_json::from_slice(&frame)?;
        if !response.ok {
            anyhow::bail!("{}", response.message);
        }
        if let Some(data) = response.data {
            let reply: ipc::LogTailReply = serde_json::from_value(data)?;
            for line in reply.lines {
                println!("{line}");
            }
        }
    }
    Ok(())
}

async fn run_event_stream(
    mut framed: tokio_util::codec::Framed<tokio::net::UnixStream, tokio_util::codec::LengthDelimitedCodec>,
) -> anyhow::Result<()> {
    use futures::StreamExt;
    while let Some(frame) = framed.next().await {
        let frame = frame?;
        let response: ipc::Response = serde_json::from_slice(&frame)?;
        if !response.ok {
            anyhow::bail!("{}", response.message);
        }
        if let Some(data) = response.data {
            let event: supervisor::Event = serde_json::from_value(data)?;
            println!(
                "{} {} {} {} {}",
                event.event_type,
                event.serial,
                event.name,
                event.state,
                event.pid.map(|p| p.to_string()).unwrap_or_else(|| "0".to_string())
            );
        }
    }
    Ok(())
}

fn print_help(command: Option<&str>) {
    match command.unwrap_or("all") {
        "status" => println!("status [program] - show status"),
        "start" => println!("start [program] - start program(s)"),
        "stop" => println!("stop [program] - stop program(s)"),
        "restart" => println!("restart [program] - restart program(s)"),
        "reread" => println!("reread - reload config and report changes"),
        "update" => println!("update - apply config changes"),
        "reload" => println!("reload - reread and update"),
        "shutdown" => println!("shutdown - stop all and exit"),
        "pid" => println!("pid - show supervisord pid"),
        "signal" => println!("signal <signal> [program] - send signal"),
        "tail" => println!("tail [--follow] <program> - tail program logs"),
        "maintail" => println!("maintail [--follow] - tail supervisord log"),
        "clear" => println!("clear [program] - clear logs"),
        "add" => println!("add <program> - add program from config"),
        "remove" => println!("remove <program> - remove program"),
        "avail" => println!("avail - list available programs"),
        "events" => println!("events - subscribe to event stream"),
        "fg" => println!("fg <program> - start and follow logs"),
        _ => {
            println!("Available commands:");
            println!("  status start stop restart reread update reload shutdown pid");
            println!("  signal tail maintail clear add remove avail events fg");
            println!("  shell help");
        }
    }
}

async fn run_shell(sock_path: &Path) -> anyhow::Result<()> {
    use std::io::{self, BufRead};
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    println!("supervisorctl shell (type 'exit' to quit)");
    for line in stdin.lock().lines() {
        let line = line?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if line == "exit" || line == "quit" {
            break;
        }
        let parts: Vec<&str> = line.split_whitespace().collect();
        let cmd = parts[0];
        let args = &parts[1..];
        let ctl = parse_shell_command(cmd, args)?;
        run_ctl_shell(sock_path, ctl).await?;
        let _ = stdout.flush();
    }
    Ok(())
}

async fn run_ctl_shell(sock_path: &Path, command: CtlCommand) -> anyhow::Result<()> {
    match command {
        CtlCommand::Shell => Ok(()),
        CtlCommand::Help { command } => {
            print_help(command.as_deref());
            Ok(())
        }
        other => run_ctl_inner(sock_path, other, false).await,
    }
}

fn parse_shell_command(cmd: &str, args: &[&str]) -> anyhow::Result<CtlCommand> {
    Ok(match cmd {
        "status" => CtlCommand::Status { program: args.get(0).map(|s| s.to_string()) },
        "start" => CtlCommand::Start { program: args.get(0).map(|s| s.to_string()) },
        "stop" => CtlCommand::Stop { program: args.get(0).map(|s| s.to_string()) },
        "restart" => CtlCommand::Restart { program: args.get(0).map(|s| s.to_string()) },
        "reread" => CtlCommand::Reread,
        "update" => CtlCommand::Update,
        "reload" => CtlCommand::Reload,
        "shutdown" => CtlCommand::Shutdown,
        "pid" => CtlCommand::Pid,
        "signal" => {
            let signal = args.get(0).ok_or_else(|| anyhow::anyhow!("signal required"))?;
            let program = args.get(1).map(|s| s.to_string());
            CtlCommand::Signal { signal: signal.to_string(), program }
        }
        "tail" => {
            let program = args.get(0).ok_or_else(|| anyhow::anyhow!("program required"))?;
            CtlCommand::Tail { program: program.to_string(), lines: 10, stderr: false, follow: false }
        }
        "maintail" => CtlCommand::Maintail { lines: 10, follow: false },
        "clear" => CtlCommand::Clear { program: args.get(0).map(|s| s.to_string()) },
        "add" => {
            let program = args.get(0).ok_or_else(|| anyhow::anyhow!("program required"))?;
            CtlCommand::Add { program: program.to_string() }
        }
        "remove" => {
            let program = args.get(0).ok_or_else(|| anyhow::anyhow!("program required"))?;
            CtlCommand::Remove { program: program.to_string() }
        }
        "avail" => CtlCommand::Avail,
        "events" => CtlCommand::Events,
        "fg" => {
            let program = args.get(0).ok_or_else(|| anyhow::anyhow!("program required"))?;
            CtlCommand::Fg { program: program.to_string() }
        }
        "help" => CtlCommand::Help { command: args.get(0).map(|s| s.to_string()) },
        other => anyhow::bail!("unknown command {}", other),
    })
}

fn load_env_file(path: Option<&Path>) -> anyhow::Result<std::collections::HashMap<String, String>> {
    let mut envs = std::collections::HashMap::new();
    let Some(path) = path else {
        return Ok(envs);
    };
    let content = std::fs::read_to_string(path)?;
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((key, value)) = line.split_once('=') {
            envs.insert(key.trim().to_string(), value.trim().to_string());
        }
    }
    Ok(envs)
}

fn apply_umask(umask: Option<u32>) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        if let Some(value) = umask {
            let mask = nix::sys::stat::Mode::from_bits_truncate(value as libc::mode_t);
            nix::sys::stat::umask(mask);
        }
    }
    Ok(())
}

fn ensure_minfds(minfds: Option<u32>) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use nix::sys::resource::{getrlimit, setrlimit, Resource};
        if let Some(min) = minfds {
            let (soft, hard) = getrlimit(Resource::RLIMIT_NOFILE)?;
            let target = min as u64;
            if soft < target {
                let new_hard = hard.max(target);
                let _ = setrlimit(Resource::RLIMIT_NOFILE, target, new_hard);
            }
        }
    }
    Ok(())
}

fn daemonize() -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use nix::fcntl::{open, OFlag};
        use nix::sys::stat::Mode;
        use nix::unistd::{chdir, close, dup2, fork, setsid, ForkResult};

        match unsafe { fork()? } {
            ForkResult::Parent { .. } => std::process::exit(0),
            ForkResult::Child => {}
        }

        setsid()?;

        match unsafe { fork()? } {
            ForkResult::Parent { .. } => std::process::exit(0),
            ForkResult::Child => {}
        }

        chdir("/")?;

        let devnull = open("/dev/null", OFlag::O_RDWR, Mode::empty())?;
        let _ = dup2(devnull, 0);
        let _ = dup2(devnull, 1);
        let _ = dup2(devnull, 2);
        if devnull > 2 {
            let _ = close(devnull);
        }
    }
    Ok(())
}

fn check_single_instance(pidfile: &Path) -> anyhow::Result<()> {
    let Ok(content) = std::fs::read_to_string(pidfile) else {
        return Ok(());
    };
    let Ok(pid) = content.trim().parse::<i32>() else {
        return Ok(());
    };
    #[cfg(unix)]
    {
        use nix::sys::signal::kill;
        use nix::unistd::Pid;
        // kill(pid, None) checks if process exists without sending a signal
        if kill(Pid::from_raw(pid), None).is_ok() {
            anyhow::bail!(
                "another instance is already running (pid {pid}, pidfile {})",
                pidfile.display()
            );
        }
    }
    Ok(())
}

fn write_pidfile(path: &Path) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, std::process::id().to_string())?;
    Ok(())
}

fn setup_signal_handlers(supervisor: Arc<Mutex<supervisor::Supervisor>>) {
    #[cfg(unix)]
    {
        let sup_hup = supervisor.clone();
        tokio::spawn(async move {
            use tokio::signal::unix::{signal, SignalKind};
            if let Ok(mut hup) = signal(SignalKind::hangup()) {
                while hup.recv().await.is_some() {
                    let _ = supervisor::update(sup_hup.clone()).await;
                }
            }
        });
        let sup_shutdown = supervisor.clone();
        tokio::spawn(async move {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm = signal(SignalKind::terminate()).ok();
            let mut sigint = signal(SignalKind::interrupt()).ok();
            let mut sigquit = signal(SignalKind::quit()).ok();
            loop {
                tokio::select! {
                    _ = async { if let Some(sig) = &mut sigterm { sig.recv().await } else { None } } => {},
                    _ = async { if let Some(sig) = &mut sigint { sig.recv().await } else { None } } => {},
                    _ = async { if let Some(sig) = &mut sigquit { sig.recv().await } else { None } } => {},
                }
                let _ = supervisor::shutdown(sup_shutdown.clone()).await;
                std::process::exit(0);
            }
        });
    }
}
