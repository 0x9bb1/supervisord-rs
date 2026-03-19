use std::path::{Path, PathBuf};
use std::time::Duration;

use rvisor::{actor, config, ipc, service, supervisor};
use std::sync::{Mutex as StdMutex, OnceLock};
use tempfile::TempDir;

fn write_config(path: &Path, content: &str) {
    std::fs::write(path, content).expect("write config");
}

fn base_config(sock_path: &Path) -> String {
    format!(
        r#"[supervisord]
sock_path = "{}"
"#,
        sock_path.display()
    )
}

fn program_config(name: &str, command: &str, stdout_log: Option<&Path>, extra: &str) -> String {
    let escaped_command = command.replace('\'', "''");
    let mut config = format!(
        r#"
[[programs]]
name = "{name}"
command = '{escaped_command}'
"#
    );
    if let Some(path) = stdout_log {
        config.push_str(&format!("stdout_log = \"{}\"\n", path.display()));
    }
    config.push_str(extra);
    config
}

async fn start_daemon(config_path: &Path) -> anyhow::Result<tokio::task::JoinHandle<()>> {
    let config = config::load(Some(config_path))?;
    let sock_path = config.supervisor.sock_path.clone();
    let allowed_uids = config.supervisor.allowed_uids.clone();
    // spawn_actor handles autostart internally (with expanded numprocs names)
    let handle = actor::spawn_actor(
        config_path.to_path_buf(),
        config.programs.clone(),
        std::collections::HashMap::new(),
        config.supervisor.pidfile.clone(),
        config.supervisor.sock_path.clone(),
        config.supervisor.logfile.clone(),
    );
    let sock_path_run = sock_path.clone();
    let server_handle = tokio::spawn(async move {
        let _ = ipc::run_server(&sock_path_run, handle, allowed_uids).await;
    });
    for _ in 0..20 {
        if sock_path.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Ok(server_handle)
}

async fn request(sock_path: &Path, command: &str, program: Option<&str>) -> ipc::Response {
    let request = ipc::Request {
        command: command.to_string(),
        program: program.map(|p| p.to_string()),
        ..Default::default()
    };
    ipc::send_request(sock_path, request)
        .await
        .expect("send request")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_start_stop_flow() {
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let config_text = format!(
        "{}{}",
        base_config(&sock_path),
        program_config(
            "sleepy",
            "sleep 5",
            None,
            "autostart = false\nstartsecs = 0\n"
        )
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    let response = request(&sock_path, "status", Some("sleepy")).await;
    assert!(response.ok);
    let data = response.data.unwrap();
    let statuses: Vec<supervisor::ProgramStatus> = serde_json::from_value(data).unwrap();
    assert_eq!(statuses[0].state, "STOPPED");

    let response = request(&sock_path, "start", Some("sleepy")).await;
    assert!(response.ok);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let response = request(&sock_path, "status", Some("sleepy")).await;
    let statuses: Vec<supervisor::ProgramStatus> =
        serde_json::from_value(response.data.unwrap()).unwrap();
    assert_eq!(statuses[0].state, "RUNNING");
    assert!(statuses[0].pid.is_some());

    let response = request(&sock_path, "stop", Some("sleepy")).await;
    assert!(response.ok);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let response = request(&sock_path, "status", Some("sleepy")).await;
    let statuses: Vec<supervisor::ProgramStatus> =
        serde_json::from_value(response.data.unwrap()).unwrap();
    assert_eq!(statuses[0].state, "STOPPED");

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_changes_pid() {
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let config_text = format!(
        "{}{}",
        base_config(&sock_path),
        program_config(
            "restartable",
            "sleep 10",
            None,
            "autostart = false\nstartsecs = 0\n"
        )
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    request(&sock_path, "start", Some("restartable")).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let response = request(&sock_path, "status", Some("restartable")).await;
    let statuses: Vec<supervisor::ProgramStatus> =
        serde_json::from_value(response.data.unwrap()).unwrap();
    let first_pid = statuses[0].pid;

    let response = request(&sock_path, "restart", Some("restartable")).await;
    assert!(response.ok);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let response = request(&sock_path, "status", Some("restartable")).await;
    let statuses: Vec<supervisor::ProgramStatus> =
        serde_json::from_value(response.data.unwrap()).unwrap();
    assert_eq!(statuses[0].state, "RUNNING");
    assert!(first_pid.is_some());
    assert_ne!(statuses[0].pid, first_pid);

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reread_update_detects_changes() {
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let config_text = format!(
        "{}{}",
        base_config(&sock_path),
        program_config(
            "alpha",
            "sleep 10",
            None,
            "autostart = false\nstartsecs = 0\n"
        )
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    request(&sock_path, "start", Some("alpha")).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let response = request(&sock_path, "status", Some("alpha")).await;
    let statuses: Vec<supervisor::ProgramStatus> =
        serde_json::from_value(response.data.unwrap()).unwrap();
    let alpha_pid = statuses[0].pid;

    let updated_text = format!(
        "{}{}{}",
        base_config(&sock_path),
        program_config(
            "alpha",
            "sleep 11",
            None,
            "autostart = false\nstartsecs = 0\n"
        ),
        program_config(
            "beta",
            "sleep 5",
            None,
            "autostart = false\nstartsecs = 0\n"
        ),
    );
    write_config(&config_path, &updated_text);

    let response = request(&sock_path, "reread", None).await;
    assert!(response.ok);
    let data = response.data.unwrap();
    let reread: supervisor::RereadSummary = serde_json::from_value(data).unwrap();
    assert_eq!(reread.added, vec!["beta"]);
    assert_eq!(reread.changed, vec!["alpha"]);
    assert!(reread.removed.is_empty());

    let response = request(&sock_path, "update", None).await;
    assert!(response.ok);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let response = request(&sock_path, "status", Some("alpha")).await;
    let statuses: Vec<supervisor::ProgramStatus> =
        serde_json::from_value(response.data.unwrap()).unwrap();
    assert_eq!(statuses[0].state, "RUNNING");
    assert_ne!(statuses[0].pid, alpha_pid);

    let response = request(&sock_path, "status", Some("beta")).await;
    let statuses: Vec<supervisor::ProgramStatus> =
        serde_json::from_value(response.data.unwrap()).unwrap();
    assert_eq!(statuses[0].state, "STOPPED");

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn logtail_returns_last_lines() {
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let stdout_log = temp.path().join("demo.out.log");
    let config_text = format!(
        "{}{}",
        base_config(&sock_path),
        program_config(
            "talker",
            "sh -c \"printf \\\"a\\\\n\\\"; printf \\\"b\\\\n\\\"; printf \\\"c\\\\n\\\"\"",
            Some(&stdout_log),
            "autostart = false\n"
        )
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    request(&sock_path, "start", Some("talker")).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let response = ipc::send_request(
        &sock_path,
        ipc::Request {
            command: "logtail".to_string(),
            program: Some("talker".to_string()),
            lines: Some(2),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(response.ok);
    let data = response.data.unwrap();
    let reply: ipc::LogTailReply = serde_json::from_value(data).unwrap();
    assert_eq!(reply.lines, vec!["b", "c"]);

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn events_stream_emits_process_state() {
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let config_text = format!(
        "{}{}",
        base_config(&sock_path),
        program_config("events", "sleep 1", None, "autostart = false\n")
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    let mut framed = ipc::send_stream_request(
        &sock_path,
        ipc::Request {
            command: "events".to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    request(&sock_path, "start", Some("events")).await;
    use futures::StreamExt;
    let frame = tokio::time::timeout(Duration::from_secs(2), framed.next())
        .await
        .expect("timeout")
        .expect("stream ended")
        .expect("frame");
    let response: ipc::Response = serde_json::from_slice(&frame).unwrap();
    let event: supervisor::Event = serde_json::from_value(response.data.unwrap()).unwrap();
    assert!(event.event_type.starts_with("PROCESS_STATE_"));
    assert!(!event.payload.is_empty());

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn avail_lists_programs() {
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let config_text = format!(
        "{}{}{}",
        base_config(&sock_path),
        program_config("alpha", "sleep 10", None, "autostart = false\n"),
        program_config("beta", "sleep 5", None, "autostart = false\n")
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    let response = ipc::send_request(
        &sock_path,
        ipc::Request {
            command: "avail".to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let list: Vec<String> = serde_json::from_value(response.data.unwrap()).unwrap();
    assert_eq!(list, vec!["alpha", "beta"]);

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clear_truncates_logs() {
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let stdout_log = temp.path().join("clear.out.log");
    let config_text = format!(
        "{}{}",
        base_config(&sock_path),
        program_config(
            "clearme",
            "sh -c \"printf hello\\\\n\"",
            Some(&stdout_log),
            "autostart = false\n"
        )
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    request(&sock_path, "start", Some("clearme")).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(stdout_log.exists());
    let response = ipc::send_request(
        &sock_path,
        ipc::Request {
            command: "clear".to_string(),
            program: Some("clearme".to_string()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(response.ok);
    let len = std::fs::metadata(&stdout_log).map(|m| m.len()).unwrap_or(1);
    assert_eq!(len, 0);

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn add_and_remove_program() {
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let config_text = format!(
        "{}{}",
        base_config(&sock_path),
        program_config("alpha", "sleep 10", None, "autostart = false\n")
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    let response = ipc::send_request(
        &sock_path,
        ipc::Request {
            command: "add".to_string(),
            program: Some("alpha".to_string()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(response.ok);
    let response = ipc::send_request(
        &sock_path,
        ipc::Request {
            command: "remove".to_string(),
            program: Some("alpha".to_string()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(response.ok);

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn logtail_bytes_limits_output() {
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let stdout_log = temp.path().join("bytes.out.log");
    let config_text = format!(
        "{}{}",
        base_config(&sock_path),
        program_config(
            "bytes",
            "sh -c \"printf hello\\\\n; printf world\\\\n\"",
            Some(&stdout_log),
            "autostart = false\n"
        )
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    request(&sock_path, "start", Some("bytes")).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let response = ipc::send_request(
        &sock_path,
        ipc::Request {
            command: "logtail".to_string(),
            program: Some("bytes".to_string()),
            lines: Some(10),
            bytes: Some(5),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let reply: ipc::LogTailReply = serde_json::from_value(response.data.unwrap()).unwrap();
    assert!(reply.lines.iter().any(|line| line.contains("orld")));

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn logtail_since_filters_old_logs() {
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let stdout_log = temp.path().join("since.out.log");
    let config_text = format!(
        "{}{}",
        base_config(&sock_path),
        program_config(
            "since",
            "sh -c \"printf old\\\\n\"",
            Some(&stdout_log),
            "autostart = false\n"
        )
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    request(&sock_path, "start", Some("since")).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let future = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600;

    let response = ipc::send_request(
        &sock_path,
        ipc::Request {
            command: "logtail".to_string(),
            program: Some("since".to_string()),
            lines: Some(10),
            since: Some(future),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let reply: ipc::LogTailReply = serde_json::from_value(response.data.unwrap()).unwrap();
    assert!(reply.lines.is_empty());

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn log_rotation_creates_backups() {
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let stdout_log = temp.path().join("rotate.out.log");
    let config_text = format!(
        "{}{}",
        base_config(&sock_path),
        program_config(
            "rotator",
            "sh -c \"printf 0123456789%.0s 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30\"",
            Some(&stdout_log),
            "autostart = false\nstdout_log_max_bytes = 50\nstdout_log_backups = 2\n"
        )
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    let response = request(&sock_path, "start", Some("rotator")).await;
    assert!(response.ok);
    for _ in 0..20 {
        let response = request(&sock_path, "status", Some("rotator")).await;
        let statuses: Vec<supervisor::ProgramStatus> =
            serde_json::from_value(response.data.unwrap()).unwrap();
        if statuses[0].state == "STOPPED" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    for _ in 0..10 {
        let first = PathBuf::from(format!("{}.1", stdout_log.display()));
        if first.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let first = PathBuf::from(format!("{}.1", stdout_log.display()));
    let second = PathBuf::from(format!("{}.2", stdout_log.display()));
    assert!(stdout_log.exists());
    assert!(first.exists());
    assert!(second.exists());

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn autorestart_backoff_transitions_to_fatal() {
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let config_text = format!(
        "{}{}",
        base_config(&sock_path),
        program_config(
            "flaky",
            "sh -c \"exit 1\"",
            None,
            "autostart = false\nautorestart = true\nstartretries = 0\nstartsecs = 1\n"
        )
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    request(&sock_path, "start", Some("flaky")).await;
    let mut state = String::new();
    for _ in 0..10 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let response = request(&sock_path, "status", Some("flaky")).await;
        let statuses: Vec<supervisor::ProgramStatus> =
            serde_json::from_value(response.data.unwrap()).unwrap();
        state = statuses[0].state.clone();
        if state == "FATAL" {
            break;
        }
    }
    assert_eq!(state, "FATAL");

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn autorestart_unexpected_respects_exitcodes() {
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let config_text = format!(
        "{}{}",
        base_config(&sock_path),
        program_config(
            "exitok",
            "sh -c \"exit 2\"",
            None,
            "autostart = false\nautorestart = \"unexpected\"\nstartretries = 1\nstartsecs = 0\nexitcodes = [2]\n"
        )
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    request(&sock_path, "start", Some("exitok")).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let response = request(&sock_path, "status", Some("exitok")).await;
    let statuses: Vec<supervisor::ProgramStatus> =
        serde_json::from_value(response.data.unwrap()).unwrap();
    // Expected exit with autorestart=unexpected → no restart → EXITED (not STOPPED)
    assert_eq!(statuses[0].state, "EXITED");

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pid_returns_daemon_pid() {
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let config_text = base_config(&sock_path);
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    let response = request(&sock_path, "pid", None).await;
    assert!(response.ok);
    let pid: u32 = serde_json::from_value(response.data.unwrap()).unwrap();
    assert_eq!(pid, std::process::id());

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signal_terminates_program() {
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let config_text = format!(
        "{}{}",
        base_config(&sock_path),
        program_config(
            "signalme",
            "sleep 10",
            None,
            "autostart = false\nautorestart = false\n"
        )
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    request(&sock_path, "start", Some("signalme")).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let response = ipc::send_request(
        &sock_path,
        ipc::Request {
            command: "signal".to_string(),
            program: Some("signalme".to_string()),
            signal: Some("TERM".to_string()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(response.ok);

    let mut state = String::new();
    for _ in 0..10 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let response = request(&sock_path, "status", Some("signalme")).await;
        let statuses: Vec<supervisor::ProgramStatus> =
            serde_json::from_value(response.data.unwrap()).unwrap();
        state = statuses[0].state.clone();
        if state == "EXITED" || state == "STOPPED" {
            break;
        }
    }
    // Killed by an external signal → process exited naturally → EXITED
    assert_eq!(state, "EXITED");

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn logtail_follow_returns_incremental_lines() {
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let stdout_log = temp.path().join("follow.out.log");
    let config_text = format!(
        "{}{}",
        base_config(&sock_path),
        program_config(
            "follower",
            "sh -c \"printf a\\\\n; sleep 0.2; printf b\\\\n; sleep 0.2; printf c\\\\n\"",
            Some(&stdout_log),
            "autostart = false\n"
        )
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    request(&sock_path, "start", Some("follower")).await;
    tokio::time::sleep(Duration::from_millis(150)).await;

    let mut framed = ipc::send_stream_request(
        &sock_path,
        ipc::Request {
            command: "logtail".to_string(),
            program: Some("follower".to_string()),
            lines: Some(1),
            follow: Some(true),
            offset: Some(0),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    use futures::StreamExt;
    let mut collected = Vec::new();
    for _ in 0..3 {
        let frame = tokio::time::timeout(Duration::from_secs(2), framed.next())
            .await
            .expect("timeout")
            .expect("stream ended")
            .expect("frame");
        let response: ipc::Response = serde_json::from_slice(&frame).unwrap();
        let reply: ipc::LogTailReply = serde_json::from_value(response.data.unwrap()).unwrap();
        collected.extend(reply.lines);
        if collected.len() >= 2 {
            break;
        }
    }
    assert!(!collected.is_empty());

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reload_updates_configuration() {
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let config_text = format!(
        "{}{}",
        base_config(&sock_path),
        program_config(
            "alpha",
            "sleep 10",
            None,
            "autostart = false\nstartsecs = 0\n"
        )
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    request(&sock_path, "start", Some("alpha")).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let updated_text = format!(
        "{}{}{}",
        base_config(&sock_path),
        program_config(
            "alpha",
            "sleep 11",
            None,
            "autostart = false\nstartsecs = 0\n"
        ),
        program_config(
            "beta",
            "sleep 5",
            None,
            "autostart = false\nstartsecs = 0\n"
        ),
    );
    write_config(&config_path, &updated_text);

    let response = request(&sock_path, "reload", None).await;
    assert!(response.ok);
    let summary: supervisor::ReloadSummary =
        serde_json::from_value(response.data.unwrap()).unwrap();
    assert_eq!(summary.reread.added, vec!["beta"]);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let response = request(&sock_path, "status", Some("beta")).await;
    let statuses: Vec<supervisor::ProgramStatus> =
        serde_json::from_value(response.data.unwrap()).unwrap();
    assert_eq!(statuses[0].state, "STOPPED");

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_removes_pidfile() {
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let pidfile = temp.path().join("rvisor.pid");
    let config_path = temp.path().join("supervisord.toml");
    let config_text = format!(
        r#"[supervisord]
sock_path = "{}"
pidfile = "{}"
"#,
        sock_path.display(),
        pidfile.display()
    );
    write_config(&config_path, &config_text);

    let binary = env!("CARGO_BIN_EXE_rvisor");
    let mut child = tokio::process::Command::new(binary)
        .arg("-c")
        .arg(&config_path)
        .arg("run")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn daemon");

    for _ in 0..20 {
        if sock_path.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(sock_path.exists());

    let response = ipc::send_request(
        &sock_path,
        ipc::Request {
            command: "shutdown".to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(response.ok);

    let _ = tokio::time::timeout(Duration::from_secs(3), child.wait())
        .await
        .expect("shutdown timed out");
    assert!(!pidfile.exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sighup_triggers_reload() {
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let config_text = format!(
        "{}{}",
        base_config(&sock_path),
        program_config("alpha", "sleep 10", None, "autostart = false\n")
    );
    write_config(&config_path, &config_text);

    let binary = env!("CARGO_BIN_EXE_rvisor");
    let mut child = tokio::process::Command::new(binary)
        .arg("-c")
        .arg(&config_path)
        .arg("run")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn daemon");

    for _ in 0..20 {
        if sock_path.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(sock_path.exists());

    let updated_text = format!(
        "{}{}{}",
        base_config(&sock_path),
        program_config("alpha", "sleep 11", None, "autostart = false\n"),
        program_config("beta", "sleep 5", None, "autostart = false\n"),
    );
    write_config(&config_path, &updated_text);

    #[cfg(unix)]
    {
        use nix::sys::signal::{kill, Signal};
        use nix::unistd::Pid;
        let _ = kill(Pid::from_raw(child.id().unwrap() as i32), Signal::SIGHUP);
    }

    tokio::time::sleep(Duration::from_millis(200)).await;
    let response = request(&sock_path, "status", Some("beta")).await;
    let statuses: Vec<supervisor::ProgramStatus> =
        serde_json::from_value(response.data.unwrap()).unwrap();
    assert_eq!(statuses[0].state, "STOPPED");

    let _ = ipc::send_request(
        &sock_path,
        ipc::Request {
            command: "shutdown".to_string(),
            ..Default::default()
        },
    )
    .await;
    let _ = tokio::time::timeout(Duration::from_secs(3), child.wait())
        .await
        .expect("shutdown timed out");
}

// ── backoff tests ─────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backoff_delay_grows_exponentially() {
    // Program exits immediately (before startsecs=1 elapses) → each exit is an
    // "early exit" → retries accumulate → backoff delay should grow.
    //
    // With startretries=2 the sequence is:
    //   attempt 1 → BACKOFF, sleep d1
    //   attempt 2 → BACKOFF, sleep d2
    //   attempt 3 → retries exhausted → FATAL
    //
    // Exponential backoff (1s, 2s): time-to-FATAL ≥ 1+2 = 3s
    // Fixed 1s backoff:             time-to-FATAL ≈ 1+1 = 2s
    //
    // We assert ≥ 2.7s, which reliably distinguishes the two.
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let config_text = format!(
        "{}{}",
        base_config(&sock_path),
        program_config(
            "crasher",
            "sh -c \"exit 1\"",
            None,
            "autostart = false\nautorestart = \"always\"\nstartretries = 2\nstartsecs = 1\nexitcodes = [0]\n",
        )
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    request(&sock_path, "start", Some("crasher")).await;
    let t0 = std::time::Instant::now();

    // Wait up to 8s for FATAL.
    let mut final_state = String::new();
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let resp = request(&sock_path, "status", Some("crasher")).await;
        if let Some(data) = resp.data {
            let statuses: Vec<supervisor::ProgramStatus> = serde_json::from_value(data).unwrap();
            final_state = statuses[0].state.clone();
            if final_state == "FATAL" {
                break;
            }
        }
    }
    let elapsed = t0.elapsed();

    assert_eq!(final_state, "FATAL");
    assert!(
        elapsed >= Duration::from_millis(2700),
        "time-to-FATAL was {elapsed:?}; expected ≥2.7s (exponential backoff 1s+2s)"
    );

    handle.abort();
}

// ── log handle tests ──────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn log_fully_flushed_before_stop_returns() {
    // After stop_program returns the log file must already contain all output.
    // No extra sleep between stop and the file read is allowed.
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let stdout_log = temp.path().join("output.log");
    // Program writes continuously while sleeping; stop() kills it mid-stream.
    // The log must be fully flushed before stop returns.
    let config_text = format!(
        "{}{}",
        base_config(&sock_path),
        program_config(
            "writer",
            // Write a line every 5 ms for a long time so the process is
            // actively writing when stop() arrives.
            "sh -c \"i=0; while true; do echo line$i; i=$((i+1)); sleep 0.005; done\"",
            Some(&stdout_log),
            "autostart = false\nstartsecs = 0\nautorestart = false\n",
        )
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    request(&sock_path, "start", Some("writer")).await;
    // Let the program write for a bit.
    tokio::time::sleep(Duration::from_millis(300)).await;

    request(&sock_path, "stop", Some("writer")).await;
    // No sleep here — stop must guarantee log is flushed before returning.

    let size_at_stop = std::fs::metadata(&stdout_log).map(|m| m.len()).unwrap_or(0);

    // Wait a little and check the file did NOT grow after stop returned,
    // proving all buffered data was already flushed.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let size_after_wait = std::fs::metadata(&stdout_log).map(|m| m.len()).unwrap_or(0);

    assert!(size_at_stop > 0, "log file should have content");
    assert_eq!(
        size_at_stop, size_after_wait,
        "log file grew after stop returned — data was not fully flushed"
    );

    handle.abort();
}

// ── startsecs tests ───────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startsecs_zero_is_running_immediately() {
    // startsecs = 0: process should be RUNNING right after start, no wait.
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let config_text = format!(
        "{}{}",
        base_config(&sock_path),
        program_config(
            "fast",
            "sleep 10",
            None,
            "autostart = false\nstartsecs = 0\n"
        )
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    request(&sock_path, "start", Some("fast")).await;
    // Give the process a moment to spawn but not more than needed.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let response = request(&sock_path, "status", Some("fast")).await;
    let statuses: Vec<supervisor::ProgramStatus> =
        serde_json::from_value(response.data.unwrap()).unwrap();
    assert_eq!(statuses[0].state, "RUNNING");

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startsecs_keeps_starting_state_during_wait() {
    // While startsecs has not elapsed the program should report STARTING.
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let config_text = format!(
        "{}{}",
        base_config(&sock_path),
        program_config(
            "slow",
            "sleep 10",
            None,
            "autostart = false\nstartsecs = 2\n"
        )
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    request(&sock_path, "start", Some("slow")).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let response = request(&sock_path, "status", Some("slow")).await;
    let statuses: Vec<supervisor::ProgramStatus> =
        serde_json::from_value(response.data.unwrap()).unwrap();
    assert_eq!(statuses[0].state, "STARTING");

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startsecs_transitions_to_running_after_elapsed() {
    // After startsecs elapses with the process still alive → RUNNING.
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let config_text = format!(
        "{}{}",
        base_config(&sock_path),
        program_config(
            "delayed",
            "sleep 10",
            None,
            "autostart = false\nstartsecs = 1\n"
        )
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    request(&sock_path, "start", Some("delayed")).await;
    // Wait longer than startsecs.
    tokio::time::sleep(Duration::from_millis(1400)).await;

    let response = request(&sock_path, "status", Some("delayed")).await;
    let statuses: Vec<supervisor::ProgramStatus> =
        serde_json::from_value(response.data.unwrap()).unwrap();
    assert_eq!(statuses[0].state, "RUNNING");

    handle.abort();
}

// ── STOPPING state tests ──────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stopping_state_visible_during_kill_wait() {
    // A process that ignores SIGCONT (stopsignal=CONT) stays alive for the
    // full stopwaitsecs window.  During that window its state must be STOPPING.
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let config_text = format!(
        "{}{}",
        base_config(&sock_path),
        program_config(
            "stubborn",
            "sleep 30",
            None,
            "autostart = false\nstartsecs = 0\nautorestart = false\nstopwaitsecs = 3\nstopsignal = \"CONT\"\n",
        )
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    request(&sock_path, "start", Some("stubborn")).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Fire stop in background; it will block for ~stopwaitsecs.
    let sock2 = sock_path.clone();
    tokio::spawn(async move {
        request(&sock2, "stop", Some("stubborn")).await;
    });

    // Give the stop command time to send the signal and change state.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let resp = request(&sock_path, "status", Some("stubborn")).await;
    let statuses: Vec<supervisor::ProgramStatus> =
        serde_json::from_value(resp.data.unwrap()).unwrap();
    assert_eq!(statuses[0].state, "STOPPING");

    handle.abort();
}

// ── EXITED state tests ────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exited_state_on_natural_exit() {
    // When autorestart=never and the process exits on its own → EXITED (not STOPPED).
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let config_text = format!(
        "{}{}",
        base_config(&sock_path),
        program_config(
            "short",
            "sh -c \"exit 0\"",
            None,
            "autostart = false\nstartsecs = 0\nautorestart = false\n",
        )
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    request(&sock_path, "start", Some("short")).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let resp = request(&sock_path, "status", Some("short")).await;
    let statuses: Vec<supervisor::ProgramStatus> =
        serde_json::from_value(resp.data.unwrap()).unwrap();
    assert_eq!(statuses[0].state, "EXITED");

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stopped_state_on_manual_stop() {
    // Manual stop must still show STOPPED, not EXITED.
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let config_text = format!(
        "{}{}",
        base_config(&sock_path),
        program_config(
            "svc",
            "sleep 30",
            None,
            "autostart = false\nstartsecs = 0\n"
        )
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    request(&sock_path, "start", Some("svc")).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    request(&sock_path, "stop", Some("svc")).await;

    let resp = request(&sock_path, "status", Some("svc")).await;
    let statuses: Vec<supervisor::ProgramStatus> =
        serde_json::from_value(resp.data.unwrap()).unwrap();
    assert_eq!(statuses[0].state, "STOPPED");

    handle.abort();
}

// ── Socket auth tests ─────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn socket_allows_authorized_uid() {
    // allowed_uids contains the current uid → status should succeed.
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    #[cfg(unix)]
    let current_uid = unsafe { libc::getuid() };
    #[cfg(not(unix))]
    let current_uid = 0u32;
    let config_text = format!(
        "[supervisord]\nsock_path = \"{}\"\nallowed_uids = [{}]\n",
        sock_path.display(),
        current_uid
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    let response = request(&sock_path, "status", None).await;
    assert!(
        response.ok,
        "authorized uid should be allowed: {:?}",
        response.message
    );

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn socket_rejects_unauthorized_uid() {
    // allowed_uids contains only uid 65534 (nobody); running as a different
    // user → daemon must return ok=false and close the connection.
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let config_text = format!(
        "[supervisord]\nsock_path = \"{}\"\nallowed_uids = [65534]\n",
        sock_path.display()
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    // The daemon should reject us and return an error response.
    let result = ipc::send_request(
        &sock_path,
        ipc::Request {
            command: "status".to_string(),
            ..Default::default()
        },
    )
    .await;
    if let Ok(resp) = result {
        assert!(!resp.ok, "unauthorized uid should be rejected");
    }

    handle.abort();
}

// ── numprocs tests ────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn numprocs_one_keeps_plain_name() {
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let config_text = format!(
        "{}{}",
        base_config(&sock_path),
        program_config("worker", "sleep 10", None, "autostart = false\n")
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    let response = request(&sock_path, "avail", None).await;
    assert!(response.ok);
    let mut names: Vec<String> = serde_json::from_value(response.data.unwrap()).unwrap();
    names.sort();
    assert_eq!(names, vec!["worker"]);

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn numprocs_avail_shows_all_instances() {
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let config_text = format!(
        "{}{}",
        base_config(&sock_path),
        program_config(
            "worker",
            "sleep 10",
            None,
            "autostart = false\nnumprocs = 3\n"
        )
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    let response = request(&sock_path, "avail", None).await;
    assert!(response.ok);
    let mut names: Vec<String> = serde_json::from_value(response.data.unwrap()).unwrap();
    names.sort();
    assert_eq!(
        names,
        vec!["worker:worker_00", "worker:worker_01", "worker:worker_02"]
    );

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn numprocs_all_autostart() {
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let config_text = format!(
        "{}{}",
        base_config(&sock_path),
        program_config(
            "worker",
            "sleep 10",
            None,
            "autostart = true\nnumprocs = 2\nstartsecs = 0\n"
        )
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let response = request(&sock_path, "status", None).await;
    assert!(response.ok);
    let statuses: Vec<supervisor::ProgramStatus> =
        serde_json::from_value(response.data.unwrap()).unwrap();
    assert_eq!(statuses.len(), 2);
    for s in &statuses {
        assert_eq!(s.state, "RUNNING", "instance {} not running", s.name);
    }

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn numprocs_stop_one_does_not_affect_others() {
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let config_text = format!(
        "{}{}",
        base_config(&sock_path),
        program_config(
            "worker",
            "sleep 10",
            None,
            "autostart = false\nnumprocs = 2\nstartsecs = 0\n"
        )
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    request(&sock_path, "start", Some("worker:worker_00")).await;
    request(&sock_path, "start", Some("worker:worker_01")).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    request(&sock_path, "stop", Some("worker:worker_00")).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let r0 = request(&sock_path, "status", Some("worker:worker_00")).await;
    let r1 = request(&sock_path, "status", Some("worker:worker_01")).await;
    let s0: Vec<supervisor::ProgramStatus> = serde_json::from_value(r0.data.unwrap()).unwrap();
    let s1: Vec<supervisor::ProgramStatus> = serde_json::from_value(r1.data.unwrap()).unwrap();
    assert_eq!(s0[0].state, "STOPPED");
    assert_eq!(s1[0].state, "RUNNING");

    handle.abort();
}

#[test]
fn service_install_and_uninstall_writes_file() {
    let _guard = service_env_lock().lock().unwrap();
    let temp = TempDir::new().unwrap();
    std::env::set_var("RVISOR_SERVICE_DIR", temp.path());
    std::env::set_var("RVISOR_SERVICE_NOOP", "1");
    let result = service::run(service::ServiceCommand::Install, None).unwrap();
    assert!(result.contains("installed"));
    let installed_path = result.trim_start_matches("installed ").trim();
    assert!(std::path::Path::new(installed_path).exists());

    let result = service::run(service::ServiceCommand::Uninstall, None).unwrap();
    assert!(result.contains("uninstalled"));
    assert!(!std::path::Path::new(installed_path).exists());
    std::env::remove_var("RVISOR_SERVICE_DIR");
    std::env::remove_var("RVISOR_SERVICE_NOOP");
}

#[test]
fn service_start_stop_status_noop() {
    let _guard = service_env_lock().lock().unwrap();
    let temp = TempDir::new().unwrap();
    std::env::set_var("RVISOR_SERVICE_DIR", temp.path());
    std::env::set_var("RVISOR_SERVICE_NOOP", "1");

    let _ = service::run(service::ServiceCommand::Install, None).unwrap();
    let result = service::run(service::ServiceCommand::Start, None).unwrap();
    assert_eq!(result, "start noop");
    let result = service::run(service::ServiceCommand::Stop, None).unwrap();
    assert_eq!(result, "stop noop");
    let result = service::run(service::ServiceCommand::Status, None).unwrap();
    assert_eq!(result, "status noop");
    let result = service::run(service::ServiceCommand::Enable, None).unwrap();
    assert_eq!(result, "enable noop");
    let result = service::run(service::ServiceCommand::Disable, None).unwrap();
    assert_eq!(result, "disable noop");
    let result = service::run(service::ServiceCommand::Restart, None).unwrap();
    assert_eq!(result, "restart noop");
    let result = service::run(service::ServiceCommand::Reload, None).unwrap();
    assert_eq!(result, "reload noop");

    let _ = service::run(service::ServiceCommand::Uninstall, None).unwrap();
    std::env::remove_var("RVISOR_SERVICE_DIR");
    std::env::remove_var("RVISOR_SERVICE_NOOP");
}
fn service_env_lock() -> &'static StdMutex<()> {
    static LOCK: OnceLock<StdMutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| StdMutex::new(()))
}

// ── New integration tests ───────────────────────────────────────────────────

/// Helper: spawn the real binary and wait for the socket to appear.
async fn spawn_binary_daemon(config_path: &Path, sock_path: &Path) -> tokio::process::Child {
    let binary = env!("CARGO_BIN_EXE_rvisor");
    let child = tokio::process::Command::new(binary)
        .arg("-c")
        .arg(config_path)
        .arg("run")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn daemon binary");
    for _ in 0..40 {
        if sock_path.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    child
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn autostart_true_starts_program_at_daemon_launch() {
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let config_text = format!(
        "{}{}",
        base_config(&sock_path),
        program_config(
            "autostarted",
            "sleep 30",
            None,
            "autostart = true\nstartsecs = 0\n"
        )
    );
    write_config(&config_path, &config_text);
    let mut child = spawn_binary_daemon(&config_path, &sock_path).await;

    // Give autostart a moment to take effect
    tokio::time::sleep(Duration::from_millis(300)).await;

    let response = request(&sock_path, "status", Some("autostarted")).await;
    let statuses: Vec<supervisor::ProgramStatus> =
        serde_json::from_value(response.data.unwrap()).unwrap();
    assert_eq!(statuses[0].state, "RUNNING");

    let _ = request(&sock_path, "shutdown", None).await;
    let _ = tokio::time::timeout(Duration::from_secs(3), child.wait()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startretries_exhausted_transitions_to_fatal() {
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    // startretries=1 → 2 total attempts before FATAL
    // startsecs=1 → immediate exit counts as early, incrementing retries
    let config_text = format!(
        "{}{}",
        base_config(&sock_path),
        program_config(
            "flaky2",
            "sh -c \"exit 1\"",
            None,
            "autostart = false\nautorestart = true\nstartretries = 1\nstartsecs = 1\n"
        )
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    request(&sock_path, "start", Some("flaky2")).await;
    let mut state = String::new();
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(300)).await;
        let response = request(&sock_path, "status", Some("flaky2")).await;
        let statuses: Vec<supervisor::ProgramStatus> =
            serde_json::from_value(response.data.unwrap()).unwrap();
        state = statuses[0].state.clone();
        if state == "FATAL" {
            break;
        }
    }
    assert_eq!(state, "FATAL");

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn autorestart_unexpected_restarts_on_unexpected_exit_code() {
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    // Exit code 1 is not in exitcodes=[0] → unexpected → should restart.
    // startretries=1, startsecs=1 → reaches FATAL after 2 rapid exits.
    let config_text = format!(
        "{}{}",
        base_config(&sock_path),
        program_config(
            "unexpexit",
            "sh -c \"exit 1\"",
            None,
            "autostart = false\nautorestart = \"unexpected\"\nexitcodes = [0]\nstartretries = 1\nstartsecs = 1\n"
        )
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    request(&sock_path, "start", Some("unexpexit")).await;
    let mut state = String::new();
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(300)).await;
        let response = request(&sock_path, "status", Some("unexpexit")).await;
        let statuses: Vec<supervisor::ProgramStatus> =
            serde_json::from_value(response.data.unwrap()).unwrap();
        state = statuses[0].state.clone();
        if state == "FATAL" {
            break;
        }
    }
    // Reaching FATAL confirms unexpected exit triggered restart attempts
    assert_eq!(state, "FATAL");

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn environment_vars_injected_into_process() {
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let stdout_log = temp.path().join("env.out.log");
    let config_text = format!(
        "{}{}",
        base_config(&sock_path),
        program_config(
            "envtest",
            "sh -c \"echo $SUPERVISORD_TEST_GREETING\"",
            Some(&stdout_log),
            "autostart = false\nenvironment = {SUPERVISORD_TEST_GREETING = \"hello_world\"}\n"
        )
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    request(&sock_path, "start", Some("envtest")).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let content = std::fs::read_to_string(&stdout_log).unwrap_or_default();
    assert!(
        content.contains("hello_world"),
        "stdout_log content: {content:?}"
    );

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cwd_sets_working_directory() {
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let stdout_log = temp.path().join("cwd.out.log");
    let work_dir = temp.path().join("workdir");
    std::fs::create_dir_all(&work_dir).unwrap();
    let config_text = format!(
        "{}{}",
        base_config(&sock_path),
        program_config(
            "cwdtest",
            "pwd",
            Some(&stdout_log),
            &format!("autostart = false\ncwd = \"{}\"\n", work_dir.display())
        )
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    request(&sock_path, "start", Some("cwdtest")).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let content = std::fs::read_to_string(&stdout_log).unwrap_or_default();
    let logged = PathBuf::from(content.trim())
        .canonicalize()
        .unwrap_or_else(|_| PathBuf::from(content.trim()));
    let expected = work_dir.canonicalize().unwrap_or(work_dir);
    assert_eq!(logged, expected);

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stderr_log_captures_stderr_output() {
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let stderr_log = temp.path().join("err.log");
    let config_text = format!(
        "{}{}",
        base_config(&sock_path),
        program_config(
            "stderrtest",
            r#"sh -c "echo errline >&2""#,
            None,
            &format!(
                "autostart = false\nstderr_log = \"{}\"\n",
                stderr_log.display()
            )
        )
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    request(&sock_path, "start", Some("stderrtest")).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let content = std::fs::read_to_string(&stderr_log).unwrap_or_default();
    assert!(
        content.contains("errline"),
        "stderr_log content: {content:?}"
    );

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn logtail_stderr_stream_reads_stderr_log() {
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let stderr_log = temp.path().join("tail_err.log");
    let config_text = format!(
        "{}{}",
        base_config(&sock_path),
        program_config(
            "stderrtail",
            r#"sh -c "echo x >&2; echo y >&2; echo z >&2""#,
            None,
            &format!(
                "autostart = false\nstderr_log = \"{}\"\n",
                stderr_log.display()
            )
        )
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    request(&sock_path, "start", Some("stderrtail")).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let response = ipc::send_request(
        &sock_path,
        ipc::Request {
            command: "logtail".to_string(),
            program: Some("stderrtail".to_string()),
            lines: Some(2),
            stream: Some("stderr".to_string()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(response.ok);
    let reply: ipc::LogTailReply = serde_json::from_value(response.data.unwrap()).unwrap();
    assert_eq!(reply.lines, vec!["y", "z"]);

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_all_starts_every_stopped_program() {
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let config_text = format!(
        "{}{}{}",
        base_config(&sock_path),
        program_config("p1", "sleep 30", None, "autostart = false\nstartsecs = 0\n"),
        program_config("p2", "sleep 30", None, "autostart = false\nstartsecs = 0\n"),
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    let response = request(&sock_path, "start", None).await;
    assert!(response.ok);
    tokio::time::sleep(Duration::from_millis(200)).await;

    for name in &["p1", "p2"] {
        let resp = request(&sock_path, "status", Some(name)).await;
        let statuses: Vec<supervisor::ProgramStatus> =
            serde_json::from_value(resp.data.unwrap()).unwrap();
        assert_eq!(statuses[0].state, "RUNNING", "{name} should be RUNNING");
    }

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_all_is_concurrent() {
    // Use stopsignal=CONT: sleep ignores SIGCONT (already running), so the
    // full stopwaitsecs=2 always elapses before SIGKILL is sent.
    // Serial stop: 2s + 2s = 4s.  Concurrent stop: ~2s.
    // We assert elapsed < 3.5s to distinguish the two.
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let config_text = format!(
        "{}{}{}",
        base_config(&sock_path),
        program_config(
            "p1",
            "sleep 30",
            None,
            "autostart = false\nstartsecs = 0\nautorestart = false\nstopwaitsecs = 2\nstopsignal = \"CONT\"\n",
        ),
        program_config(
            "p2",
            "sleep 30",
            None,
            "autostart = false\nstartsecs = 0\nautorestart = false\nstopwaitsecs = 2\nstopsignal = \"CONT\"\n",
        ),
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    request(&sock_path, "start", Some("p1")).await;
    request(&sock_path, "start", Some("p2")).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let t0 = std::time::Instant::now();
    request(&sock_path, "stop", None).await; // stop all
    let elapsed = t0.elapsed();

    assert!(
        elapsed < Duration::from_millis(3500),
        "stop_all took {elapsed:?}; expected <3.5s (concurrent, not ≥4s serial)"
    );

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_all_stops_every_running_program() {
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let config_text = format!(
        "{}{}{}",
        base_config(&sock_path),
        program_config("q1", "sleep 30", None, "autostart = false\n"),
        program_config("q2", "sleep 30", None, "autostart = false\n"),
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    request(&sock_path, "start", None).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let response = request(&sock_path, "stop", None).await;
    assert!(response.ok);
    tokio::time::sleep(Duration::from_millis(200)).await;

    for name in &["q1", "q2"] {
        let resp = request(&sock_path, "status", Some(name)).await;
        let statuses: Vec<supervisor::ProgramStatus> =
            serde_json::from_value(resp.data.unwrap()).unwrap();
        assert_eq!(statuses[0].state, "STOPPED", "{name} should be STOPPED");
    }

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_exits_without_autorestart_becomes_stopped() {
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let config_text = format!(
        "{}{}",
        base_config(&sock_path),
        program_config(
            "oneshot",
            "sh -c \"exit 0\"",
            None,
            "autostart = false\nautorestart = false\n"
        )
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    request(&sock_path, "start", Some("oneshot")).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let response = request(&sock_path, "status", Some("oneshot")).await;
    let statuses: Vec<supervisor::ProgramStatus> =
        serde_json::from_value(response.data.unwrap()).unwrap();
    // Process exited on its own (autorestart=never) → EXITED, not STOPPED
    assert_eq!(statuses[0].state, "EXITED");

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_already_running_program_is_idempotent() {
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let config_text = format!(
        "{}{}",
        base_config(&sock_path),
        program_config("idempotent", "sleep 30", None, "autostart = false\n")
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    request(&sock_path, "start", Some("idempotent")).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let response = request(&sock_path, "start", Some("idempotent")).await;
    assert!(response.ok);
    assert!(
        response.message.contains("already running"),
        "message: {}",
        response.message
    );

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maintail_reads_from_daemon_logfile() {
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let logfile = temp.path().join("rvisor.log");
    std::fs::write(&logfile, "alpha\nbeta\ngamma\n").unwrap();

    let config_text = format!(
        "[supervisord]\nsock_path = \"{}\"\nlogfile = \"{}\"\n",
        sock_path.display(),
        logfile.display()
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    let response = ipc::send_request(
        &sock_path,
        ipc::Request {
            command: "maintail".to_string(),
            lines: Some(2),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(response.ok);
    let reply: ipc::LogTailReply = serde_json::from_value(response.data.unwrap()).unwrap();
    assert_eq!(reply.lines, vec!["beta", "gamma"]);

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maintail_without_logfile_returns_error() {
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    write_config(&config_path, &base_config(&sock_path));
    let handle = start_daemon(&config_path).await.unwrap();

    let response = ipc::send_request(
        &sock_path,
        ipc::Request {
            command: "maintail".to_string(),
            lines: Some(10),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(!response.ok);
    assert!(
        response.message.contains("logfile"),
        "message: {}",
        response.message
    );

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_removes_program_deleted_from_config() {
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    let config_text = format!(
        "{}{}",
        base_config(&sock_path),
        program_config("todelete", "sleep 30", None, "autostart = false\n")
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    request(&sock_path, "start", Some("todelete")).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Remove the program from config
    write_config(&config_path, &base_config(&sock_path));

    let response = request(&sock_path, "update", None).await;
    assert!(response.ok);
    let summary: supervisor::UpdateSummary =
        serde_json::from_value(response.data.unwrap()).unwrap();
    assert!(summary.removed.contains(&"todelete".to_string()));

    // Program is fully removed from the supervisor after update.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let status_resp = request(&sock_path, "status", Some("todelete")).await;
    let statuses: Vec<supervisor::ProgramStatus> =
        serde_json::from_value(status_resp.data.unwrap()).unwrap();
    assert!(
        statuses.is_empty(),
        "removed program should not appear in status"
    );

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stopwaitsecs_force_kills_unresponsive_process() {
    let temp = TempDir::new().unwrap();
    let sock_path = temp.path().join("supervisord.sock");
    let config_path = temp.path().join("supervisord.toml");
    // Process ignores TERM; should be killed by SIGKILL after stopwaitsecs=1.
    let config_text = format!(
        "{}{}",
        base_config(&sock_path),
        program_config(
            "stubborn",
            "sh -c \"trap \\\"\\\" TERM; sleep 30\"",
            None,
            "autostart = false\nautorestart = false\nstopwaitsecs = 1\n"
        )
    );
    write_config(&config_path, &config_text);
    let handle = start_daemon(&config_path).await.unwrap();

    request(&sock_path, "start", Some("stubborn")).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // stop blocks until the process is dead; succeeds when SIGKILL escalation works
    let response = request(&sock_path, "stop", Some("stubborn")).await;
    assert!(response.ok);

    let resp = request(&sock_path, "status", Some("stubborn")).await;
    let statuses: Vec<supervisor::ProgramStatus> =
        serde_json::from_value(resp.data.unwrap()).unwrap();
    assert_eq!(statuses[0].state, "STOPPED");

    handle.abort();
}
