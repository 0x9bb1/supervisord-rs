use crate::config::ProgramConfig;
use anyhow::Context;
use std::path::PathBuf;
use std::process::Stdio;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::task::JoinHandle;
use std::collections::HashMap;

pub struct SpawnedProcess {
    pub pid: i32,
    pub child: tokio::process::Child,
    pub log_handles: Vec<JoinHandle<anyhow::Result<()>>>,
}

pub fn spawn_program(
    program: &ProgramConfig,
    global_env: &HashMap<String, String>,
) -> anyhow::Result<SpawnedProcess> {
    let mut cmd = Command::new("sh");
    cmd.arg("-c").arg(&program.command);
    #[cfg(unix)]
    unsafe {
        cmd.pre_exec(|| {
            // Put the child in its own process group so killasgroup works correctly.
            nix::unistd::setpgid(nix::unistd::Pid::from_raw(0), nix::unistd::Pid::from_raw(0))
                .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err))?;
            // Ask the kernel to send SIGKILL to this child (and its entire process
            // group) when the supervisor process dies for any reason, including
            // SIGKILL or a crash.  This prevents orphaned processes.
            let ret = libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL);
            if ret != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    if let Some(cwd) = &program.cwd {
        cmd.current_dir(cwd);
    }
    if !global_env.is_empty() {
        cmd.envs(global_env.clone());
    }
    if !program.environment.is_empty() {
        cmd.envs(program.environment.clone());
    }

    let stdout_spec = LogSpec::new(
        program.stdout_log.clone(),
        program.stdout_log_max_bytes,
        program.stdout_log_backups,
    );
    let stderr_spec = LogSpec::new(
        program.stderr_log.clone(),
        program.stderr_log_max_bytes,
        program.stderr_log_backups,
    );
    if stdout_spec.is_some() {
        cmd.stdout(Stdio::piped());
    } else {
        cmd.stdout(Stdio::null());
    }
    if stderr_spec.is_some() {
        cmd.stderr(Stdio::piped());
    } else {
        cmd.stderr(Stdio::null());
    }

    let mut child = cmd.spawn().context("spawn program")?;
    let pid = child
        .id()
        .ok_or_else(|| anyhow::anyhow!("spawned process missing pid"))? as i32;
    let mut log_handles = Vec::new();
    if let (Some(reader), Some(spec)) = (child.stdout.take(), stdout_spec) {
        log_handles.push(tokio::spawn(write_with_rotation(
            reader,
            spec.path,
            spec.max_bytes,
            spec.backups,
        )));
    }
    if let (Some(reader), Some(spec)) = (child.stderr.take(), stderr_spec) {
        log_handles.push(tokio::spawn(write_with_rotation(
            reader,
            spec.path,
            spec.max_bytes,
            spec.backups,
        )));
    }
    Ok(SpawnedProcess {
        pid,
        child,
        log_handles,
    })
}

struct LogSpec {
    path: PathBuf,
    max_bytes: Option<u64>,
    backups: u32,
}

impl LogSpec {
    fn new(path: Option<PathBuf>, max_bytes: Option<u64>, backups: Option<u32>) -> Option<Self> {
        path.map(|path| Self {
            path,
            max_bytes,
            backups: backups.unwrap_or(0),
        })
    }
}

async fn write_with_rotation<R: AsyncRead + Unpin>(
    mut reader: R,
    path: PathBuf,
    max_bytes: Option<u64>,
    backups: u32,
) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create log dir {}", parent.display()))?;
    }
    let mut file = open_log_file(&path, false).await?;
    let mut size = file_len(&path).await.unwrap_or(0);
    let mut buffer = vec![0u8; 8192];
    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        let mut offset = 0;
        while offset < read {
            if let Some(limit) = max_bytes {
                if limit > 0 && size >= limit {
                    file.flush().await?;
                    drop(file);
                    rotate_logs(&path, backups).await?;
                    file = open_log_file(&path, true).await?;
                    size = 0;
                }
            }
            let chunk = if let Some(limit) = max_bytes {
                if limit == 0 {
                    read - offset
                } else {
                    let remaining = (limit - size).min((read - offset) as u64) as usize;
                    remaining
                }
            } else {
                read - offset
            };
            file.write_all(&buffer[offset..offset + chunk]).await?;
            size += chunk as u64;
            offset += chunk;
        }
    }
    Ok(())
}

async fn open_log_file(path: &PathBuf, truncate: bool) -> anyhow::Result<tokio::fs::File> {
    let mut options = tokio::fs::OpenOptions::new();
    options.create(true).write(true);
    if truncate {
        options.truncate(true);
    } else {
        options.append(true);
    }
    let file = options
        .open(path)
        .await
        .with_context(|| format!("open log file {}", path.display()))?;
    Ok(file)
}

async fn file_len(path: &PathBuf) -> Option<u64> {
    tokio::fs::metadata(path).await.ok().map(|m| m.len())
}

async fn rotate_logs(path: &PathBuf, backups: u32) -> anyhow::Result<()> {
    if backups == 0 {
        let _ = tokio::fs::remove_file(path).await;
        return Ok(());
    }
    for i in (1..=backups).rev() {
        let src = PathBuf::from(format!("{}.{}", path.display(), i));
        let dst = PathBuf::from(format!("{}.{}", path.display(), i + 1));
        if tokio::fs::try_exists(&src).await.unwrap_or(false) {
            if tokio::fs::rename(&src, &dst).await.is_err() {
                if tokio::fs::copy(&src, &dst).await.is_ok() {
                    let _ = tokio::fs::remove_file(&src).await;
                }
            }
        }
    }
    if tokio::fs::try_exists(path).await.unwrap_or(false) {
        let dst = PathBuf::from(format!("{}.1", path.display()));
        if tokio::fs::rename(path, &dst).await.is_err() {
            if tokio::fs::copy(path, &dst).await.is_ok() {
                let _ = tokio::fs::remove_file(path).await;
            }
        }
    }
    Ok(())
}
