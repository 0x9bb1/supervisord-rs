use anyhow::Context;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy)]
pub enum ServiceCommand {
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

pub fn run(command: ServiceCommand, config_path: Option<&Path>) -> anyhow::Result<String> {
    let service_dir = resolve_service_dir()?;
    let service_path = service_file_path(&service_dir)?;
    match command {
        ServiceCommand::Install => {
            std::fs::create_dir_all(&service_dir)?;
            let content = service_content(config_path)?;
            std::fs::write(&service_path, content)?;
            if std::env::var("SUPERVISORD_SERVICE_NOOP").is_err() {
                daemon_reload()?;
                enable_service()?;
            }
            Ok(format!("installed {}", service_path.display()))
        }
        ServiceCommand::Uninstall => {
            if service_path.exists() {
                std::fs::remove_file(&service_path)?;
            }
            if std::env::var("SUPERVISORD_SERVICE_NOOP").is_err() {
                disable_service()?;
                daemon_reload()?;
            }
            Ok(format!("uninstalled {}", service_path.display()))
        }
        ServiceCommand::Start => {
            if std::env::var("SUPERVISORD_SERVICE_NOOP").is_ok() {
                return Ok("start noop".to_string());
            }
            ensure_installed(&service_path)?;
            start_service(&service_path)
        }
        ServiceCommand::Stop => {
            if std::env::var("SUPERVISORD_SERVICE_NOOP").is_ok() {
                return Ok("stop noop".to_string());
            }
            ensure_installed(&service_path)?;
            stop_service(&service_path)
        }
        ServiceCommand::Status => {
            if std::env::var("SUPERVISORD_SERVICE_NOOP").is_ok() {
                return Ok("status noop".to_string());
            }
            ensure_installed(&service_path)?;
            status_service(&service_path)
        }
        ServiceCommand::Enable => {
            if std::env::var("SUPERVISORD_SERVICE_NOOP").is_ok() {
                return Ok("enable noop".to_string());
            }
            ensure_installed(&service_path)?;
            enable_service()?;
            Ok("enabled".to_string())
        }
        ServiceCommand::Disable => {
            if std::env::var("SUPERVISORD_SERVICE_NOOP").is_ok() {
                return Ok("disable noop".to_string());
            }
            ensure_installed(&service_path)?;
            disable_service()?;
            Ok("disabled".to_string())
        }
        ServiceCommand::Restart => {
            if std::env::var("SUPERVISORD_SERVICE_NOOP").is_ok() {
                return Ok("restart noop".to_string());
            }
            ensure_installed(&service_path)?;
            stop_service(&service_path)?;
            start_service(&service_path)
        }
        ServiceCommand::Reload => {
            if std::env::var("SUPERVISORD_SERVICE_NOOP").is_ok() {
                return Ok("reload noop".to_string());
            }
            ensure_installed(&service_path)?;
            reload_service(&service_path)
        }
    }
}

fn resolve_service_dir() -> anyhow::Result<PathBuf> {
    if let Ok(dir) = std::env::var("SUPERVISORD_SERVICE_DIR") {
        return Ok(PathBuf::from(dir));
    }
    let home = std::env::var("HOME").context("HOME is not set")?;
    #[cfg(target_os = "macos")]
    {
        return Ok(PathBuf::from(home).join("Library/LaunchAgents"));
    }
    #[cfg(not(target_os = "macos"))]
    {
        return Ok(PathBuf::from(home).join(".config/systemd/user"));
    }
}

fn service_file_path(dir: &Path) -> anyhow::Result<PathBuf> {
    #[cfg(target_os = "macos")]
    {
        return Ok(dir.join("com.supervisord.rs.plist"));
    }
    #[cfg(not(target_os = "macos"))]
    {
        return Ok(dir.join("supervisord-rs.service"));
    }
}

fn service_content(config_path: Option<&Path>) -> anyhow::Result<String> {
    let exe = std::env::current_exe().context("resolve current exe")?;
    let config_arg = config_path
        .map(|p| format!(" -c {}", p.display()))
        .unwrap_or_default();
    #[cfg(target_os = "macos")]
    {
        return Ok(format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.supervisord.rs</string>
  <key>ProgramArguments</key>
  <array>
    <string>{}</string>
    <string>run</string>
{}
  </array>
  <key>RunAtLoad</key>
  <true/>
</dict>
</plist>
"#,
            exe.display(),
            config_arg_to_plist(config_path)
        ));
    }
    #[cfg(not(target_os = "macos"))]
    {
        return Ok(format!(
            r#"[Unit]
Description=supervisord-rs

[Service]
ExecStart={} run{}
Restart=on-failure

[Install]
WantedBy=default.target
"#,
            exe.display(),
            config_arg
        ));
    }
}

#[cfg(target_os = "macos")]
fn config_arg_to_plist(config_path: Option<&Path>) -> String {
    match config_path {
        Some(path) => format!(
            "    <string>-c</string>\n    <string>{}</string>",
            path.display()
        ),
        None => String::new(),
    }
}

fn start_service(_path: &Path) -> anyhow::Result<String> {
    #[cfg(target_os = "macos")]
    {
        let status = std::process::Command::new("launchctl")
            .arg("load")
            .arg(_path)
            .status()
            .context("launchctl load")?;
        ensure_success("launchctl load", status)?;
        return Ok("started".to_string());
    }
    #[cfg(not(target_os = "macos"))]
    {
        let status = std::process::Command::new("systemctl")
            .arg("--user")
            .arg("start")
            .arg("supervisord-rs.service")
            .status()
            .context("systemctl start")?;
        ensure_success("systemctl start", status)?;
        return Ok("started".to_string());
    }
}

fn stop_service(_path: &Path) -> anyhow::Result<String> {
    #[cfg(target_os = "macos")]
    {
        let status = std::process::Command::new("launchctl")
            .arg("unload")
            .arg(_path)
            .status()
            .context("launchctl unload")?;
        ensure_success("launchctl unload", status)?;
        return Ok("stopped".to_string());
    }
    #[cfg(not(target_os = "macos"))]
    {
        let status = std::process::Command::new("systemctl")
            .arg("--user")
            .arg("stop")
            .arg("supervisord-rs.service")
            .status()
            .context("systemctl stop")?;
        ensure_success("systemctl stop", status)?;
        return Ok("stopped".to_string());
    }
}

fn status_service(_path: &Path) -> anyhow::Result<String> {
    #[cfg(target_os = "macos")]
    {
        let output = std::process::Command::new("launchctl")
            .arg("list")
            .arg("com.supervisord.rs")
            .output()
            .context("launchctl list")?;
        if !output.status.success() {
            return Ok("inactive".to_string());
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        let mut pid = "-".to_string();
        let mut status = "-".to_string();
        for line in stdout.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 3 && parts[2] == "com.supervisord.rs" {
                pid = parts[0].to_string();
                status = parts[1].to_string();
                break;
            }
        }
        return Ok(format!("active pid={pid} status={status}"));
    }
    #[cfg(not(target_os = "macos"))]
    {
        let output = std::process::Command::new("systemctl")
            .arg("--user")
            .arg("show")
            .arg("supervisord-rs.service")
            .arg("-p")
            .arg("ActiveState")
            .arg("-p")
            .arg("SubState")
            .arg("-p")
            .arg("MainPID")
            .arg("-p")
            .arg("ExecMainStatus")
            .arg("-p")
            .arg("UnitFileState")
            .output()
            .context("systemctl show")?;
        if !output.status.success() {
            return Ok("inactive".to_string());
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        let mut map = std::collections::HashMap::new();
        for line in stdout.lines() {
            if let Some((key, value)) = line.split_once('=') {
                map.insert(key.to_string(), value.to_string());
            }
        }
        let active = map.get("ActiveState").cloned().unwrap_or_default();
        let sub = map.get("SubState").cloned().unwrap_or_default();
        let pid = map.get("MainPID").cloned().unwrap_or_default();
        let code = map.get("ExecMainStatus").cloned().unwrap_or_default();
        let unit = map.get("UnitFileState").cloned().unwrap_or_default();
        return Ok(format!(
            "active_state={active} sub_state={sub} pid={pid} exit_code={code} unit_state={unit}"
        ));
    }
}

fn reload_service(_path: &Path) -> anyhow::Result<String> {
    #[cfg(target_os = "macos")]
    {
        use nix::unistd::Uid;
        let uid = Uid::current().as_raw();
        let target = format!("gui/{uid}/com.supervisord.rs");
        let status = std::process::Command::new("launchctl")
            .arg("kickstart")
            .arg("-k")
            .arg(&target)
            .status()
            .context("launchctl kickstart")?;
        ensure_success("launchctl kickstart", status)?;
        return Ok("reloaded".to_string());
    }
    #[cfg(not(target_os = "macos"))]
    {
        let status = std::process::Command::new("systemctl")
            .arg("--user")
            .arg("reload")
            .arg("supervisord-rs.service")
            .status()
            .context("systemctl reload")?;
        ensure_success("systemctl reload", status)?;
        return Ok("reloaded".to_string());
    }
}

fn daemon_reload() -> anyhow::Result<()> {
    #[cfg(target_os = "macos")]
    {
        return Ok(());
    }
    #[cfg(not(target_os = "macos"))]
    {
        let status = std::process::Command::new("systemctl")
            .arg("--user")
            .arg("daemon-reload")
            .status()
            .context("systemctl daemon-reload")?;
        ensure_success("systemctl daemon-reload", status)?;
        return Ok(());
    }
}

fn enable_service() -> anyhow::Result<()> {
    #[cfg(target_os = "macos")]
    {
        return Ok(());
    }
    #[cfg(not(target_os = "macos"))]
    {
        let status = std::process::Command::new("systemctl")
            .arg("--user")
            .arg("enable")
            .arg("supervisord-rs.service")
            .status()
            .context("systemctl enable")?;
        ensure_success("systemctl enable", status)?;
        return Ok(());
    }
}

fn disable_service() -> anyhow::Result<()> {
    #[cfg(target_os = "macos")]
    {
        return Ok(());
    }
    #[cfg(not(target_os = "macos"))]
    {
        let status = std::process::Command::new("systemctl")
            .arg("--user")
            .arg("disable")
            .arg("supervisord-rs.service")
            .status()
            .context("systemctl disable")?;
        ensure_success("systemctl disable", status)?;
        return Ok(());
    }
}

fn ensure_success(context: &str, status: std::process::ExitStatus) -> anyhow::Result<()> {
    if status.success() {
        Ok(())
    } else {
        anyhow::bail!("{} failed with {}", context, status)
    }
}

fn ensure_installed(path: &Path) -> anyhow::Result<()> {
    if path.exists() {
        Ok(())
    } else {
        anyhow::bail!("service file not found at {}", path.display())
    }
}
