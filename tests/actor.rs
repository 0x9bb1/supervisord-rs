use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use rvisor::{actor, config};

fn make_program(name: &str, command: &str) -> config::ProgramConfig {
    config::ProgramConfig {
        name: name.to_string(),
        command: command.to_string(),
        autostart: false,
        autorestart: config::Autorestart::Never,
        startsecs: 0,
        startretries: 3,
        stopwaitsecs: 5,
        exitcodes: vec![0],
        stopsignal: "TERM".to_string(),
        killasgroup: false,
        numprocs: 1,
        cwd: None,
        environment: HashMap::new(),
        stdout_log: None,
        stderr_log: None,
        stdout_log_max_bytes: None,
        stdout_log_backups: None,
        stderr_log_max_bytes: None,
        stderr_log_backups: None,
    }
}

fn spawn_test_actor(programs: Vec<config::ProgramConfig>) -> actor::RvisorHandle {
    actor::spawn_actor(
        PathBuf::from("/tmp/test.toml"),
        programs,
        HashMap::new(),
        None,
        PathBuf::from("/tmp/test.sock"),
        None,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_returns_stopped_for_new_program() {
    let prog = make_program("alpha", "sleep 100");
    let handle = spawn_test_actor(vec![prog]);

    let statuses = handle.status(Some("alpha".to_string())).await.unwrap();
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].state, "STOPPED");
    assert!(statuses[0].pid.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_transitions_to_running() {
    let prog = make_program("beta", "sleep 100");
    let handle = spawn_test_actor(vec![prog]);

    let result = handle.start(Some("beta".to_string())).await.unwrap();
    assert!(result.contains("started") || result.contains("running"));

    // startsecs=0 so InternalReady should fire immediately
    tokio::time::sleep(Duration::from_millis(50)).await;

    let statuses = handle.status(Some("beta".to_string())).await.unwrap();
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].state, "RUNNING");
    assert!(statuses[0].pid.is_some());

    // Clean up
    let _ = handle.stop(Some("beta".to_string())).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_already_running_is_idempotent() {
    let prog = make_program("gamma", "sleep 100");
    let handle = spawn_test_actor(vec![prog]);

    handle.start(Some("gamma".to_string())).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Second start should return "already running"
    let result = handle.start(Some("gamma".to_string())).await.unwrap();
    assert!(
        result.contains("already running"),
        "expected 'already running', got: {}",
        result
    );

    // Status should still be RUNNING with a pid
    let statuses = handle.status(Some("gamma".to_string())).await.unwrap();
    assert_eq!(statuses[0].state, "RUNNING");

    // Clean up
    let _ = handle.stop(Some("gamma".to_string())).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_running_program_transitions_to_stopped() {
    let prog = make_program("delta", "sleep 100");
    let handle = spawn_test_actor(vec![prog]);

    handle.start(Some("delta".to_string())).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let statuses = handle.status(Some("delta".to_string())).await.unwrap();
    assert_eq!(statuses[0].state, "RUNNING");

    let result = handle.stop(Some("delta".to_string())).await.unwrap();
    assert!(
        result.contains("stopped"),
        "expected 'stopped', got: {}",
        result
    );

    let statuses = handle.status(Some("delta".to_string())).await.unwrap();
    assert_eq!(statuses[0].state, "STOPPED");
    assert!(statuses[0].pid.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_stops_then_starts() {
    let prog = make_program("epsilon", "sleep 100");
    let handle = spawn_test_actor(vec![prog]);

    handle.start(Some("epsilon".to_string())).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let statuses_before = handle.status(Some("epsilon".to_string())).await.unwrap();
    let pid_before = statuses_before[0].pid.unwrap();

    handle.restart(Some("epsilon".to_string())).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let statuses_after = handle.status(Some("epsilon".to_string())).await.unwrap();
    assert_eq!(statuses_after[0].state, "RUNNING");
    // The pid should be different after restart (new process)
    let pid_after = statuses_after[0].pid.unwrap();
    assert_ne!(pid_before, pid_after, "pid should change after restart");

    // Clean up
    let _ = handle.stop(Some("epsilon".to_string())).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exit_autorestart_never_transitions_to_exited() {
    // Use 'true' which exits immediately with code 0
    let mut prog = make_program("zeta", "true");
    prog.autorestart = config::Autorestart::Never;
    prog.startsecs = 0;
    let handle = spawn_test_actor(vec![prog]);

    handle.start(Some("zeta".to_string())).await.unwrap();

    // Wait for the process to exit and state to update
    let mut final_state = String::new();
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let statuses = handle.status(Some("zeta".to_string())).await.unwrap();
        let state = statuses[0].state.clone();
        if state == "EXITED" || state == "STOPPED" {
            final_state = state;
            break;
        }
    }
    assert_eq!(final_state, "EXITED", "expected EXITED state");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signal_unknown_program_returns_error() {
    let handle = spawn_test_actor(vec![]);

    let result = handle
        .signal(Some("nonexistent".to_string()), "TERM".to_string())
        .await;
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("unknown program") || msg.contains("nonexistent"),
        "error: {}",
        msg
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn avail_returns_sorted_names() {
    let programs = vec![
        make_program("zebra", "sleep 100"),
        make_program("alpha", "sleep 100"),
        make_program("mango", "sleep 100"),
    ];
    let handle = spawn_test_actor(programs);

    let names = handle.avail().await.unwrap();
    assert_eq!(names, vec!["alpha", "mango", "zebra"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn events_subscribe_receives_state_transitions() {
    let prog = make_program("eta", "sleep 100");
    let handle = spawn_test_actor(vec![prog]);

    let mut events_rx = handle.events_subscribe().await.unwrap();

    handle.start(Some("eta".to_string())).await.unwrap();

    // We should receive at least a STARTING event
    let event = tokio::time::timeout(Duration::from_secs(2), events_rx.recv())
        .await
        .expect("timeout waiting for event")
        .expect("channel closed");

    assert_eq!(event.name, "eta");
    assert!(!event.event_type.is_empty());

    // Clean up
    let _ = handle.stop(Some("eta".to_string())).await;
}
