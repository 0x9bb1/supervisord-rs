use anyhow::Context;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct Config {
    pub supervisor: SupervisorConfig,
    pub programs: Vec<ProgramConfig>,
}

#[derive(Debug, Clone)]
pub struct SupervisorConfig {
    pub logfile: Option<PathBuf>,
    pub pidfile: Option<PathBuf>,
    pub sock_path: PathBuf,
    pub umask: Option<u32>,
    pub minfds: Option<u32>,
    /// UIDs allowed to connect to the control socket.
    /// An empty list means all local users are permitted (default).
    pub allowed_uids: Vec<u32>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct ProgramConfig {
    pub name: String,
    pub command: String,
    pub cwd: Option<PathBuf>,
    #[serde(default = "default_true")]
    pub autostart: bool,
    #[serde(default)]
    pub autorestart: Autorestart,
    #[serde(default = "default_numprocs")]
    pub numprocs: u32,
    #[serde(default)]
    pub environment: HashMap<String, String>,
    pub stdout_log: Option<PathBuf>,
    pub stderr_log: Option<PathBuf>,
    #[serde(default)]
    pub stdout_log_max_bytes: Option<u64>,
    #[serde(default)]
    pub stdout_log_backups: Option<u32>,
    #[serde(default)]
    pub stderr_log_max_bytes: Option<u64>,
    #[serde(default)]
    pub stderr_log_backups: Option<u32>,
    #[serde(default = "default_startretries")]
    pub startretries: u32,
    #[serde(default = "default_startsecs")]
    pub startsecs: u64,
    #[serde(default = "default_exitcodes")]
    pub exitcodes: Vec<i32>,
    #[serde(default = "default_stopsignal")]
    pub stopsignal: String,
    #[serde(default = "default_stopwaitsecs")]
    pub stopwaitsecs: u64,
    #[serde(default)]
    pub killasgroup: bool,
}

#[derive(Debug, Deserialize)]
struct ConfigFile {
    #[serde(rename = "supervisord")]
    supervisor: Option<SupervisorSection>,
    programs: Option<Vec<ProgramConfig>>,
}

#[derive(Debug, Deserialize)]
struct SupervisorSection {
    logfile: Option<PathBuf>,
    pidfile: Option<PathBuf>,
    sock_path: Option<PathBuf>,
    umask: Option<u32>,
    minfds: Option<u32>,
    #[serde(default)]
    allowed_uids: Vec<u32>,
}

fn default_true() -> bool {
    true
}

fn default_numprocs() -> u32 {
    1
}

fn default_startretries() -> u32 {
    3
}

fn default_startsecs() -> u64 {
    1
}

fn default_exitcodes() -> Vec<i32> {
    vec![0]
}

fn default_stopsignal() -> String {
    "TERM".to_string()
}

fn default_stopwaitsecs() -> u64 {
    10
}

pub fn load(path: Option<&Path>) -> anyhow::Result<Config> {
    let path = path.unwrap_or_else(|| Path::new("supervisord.toml"));
    if !path.exists() {
        return Ok(Config {
            supervisor: SupervisorConfig::default(),
            programs: Vec::new(),
        });
    }
    let content =
        std::fs::read_to_string(path).with_context(|| format!("read config {}", path.display()))?;
    let parsed: ConfigFile =
        toml::from_str(&content).with_context(|| format!("parse config {}", path.display()))?;
    let supervisor = parsed
        .supervisor
        .map(SupervisorConfig::from)
        .unwrap_or_default();
    let programs = parsed.programs.unwrap_or_default();
    validate_programs(&programs)?;
    Ok(Config {
        supervisor,
        programs,
    })
}

pub fn template() -> String {
    r#"# supervisord.toml
[supervisord]
logfile = "/var/log/rvisor.log"
pidfile = "/var/run/rvisor.pid"
sock_path = "/tmp/rvisor.sock"
umask = 22
minfds = 1024

[[programs]]
name = "example"
command = "sleep 60"
cwd = "."
autostart = true
autorestart = false
numprocs = 1
stdout_log = "/tmp/example.out.log"
stderr_log = "/tmp/example.err.log"
stdout_log_max_bytes = 1048576
stdout_log_backups = 5
stderr_log_max_bytes = 1048576
stderr_log_backups = 5
startretries = 3
startsecs = 1
exitcodes = [0]
stopsignal = "TERM"
stopwaitsecs = 10
killasgroup = false
"#
    .to_string()
}

impl Default for SupervisorConfig {
    fn default() -> Self {
        Self {
            logfile: None,
            pidfile: None,
            sock_path: PathBuf::from("/tmp/rvisor.sock"),
            umask: None,
            minfds: None,
            allowed_uids: Vec::new(),
        }
    }
}

impl From<SupervisorSection> for SupervisorConfig {
    fn from(section: SupervisorSection) -> Self {
        let mut config = SupervisorConfig::default();
        if let Some(path) = section.logfile {
            config.logfile = Some(path);
        }
        if let Some(path) = section.pidfile {
            config.pidfile = Some(path);
        }
        if let Some(path) = section.sock_path {
            config.sock_path = path;
        }
        config.umask = section.umask;
        config.minfds = section.minfds;
        config.allowed_uids = section.allowed_uids;
        config
    }
}

impl Config {
    pub fn autostart_programs(&self) -> Vec<String> {
        self.programs
            .iter()
            .filter(|p| p.autostart)
            .map(|p| p.name.clone())
            .collect()
    }
}

fn validate_programs(programs: &[ProgramConfig]) -> anyhow::Result<()> {
    let mut names = HashSet::new();
    for program in programs {
        if program.name.trim().is_empty() {
            anyhow::bail!("program name must not be empty");
        }
        if program.command.trim().is_empty() {
            anyhow::bail!("program {} has empty command", program.name);
        }
        if program.numprocs == 0 {
            anyhow::bail!("program {} numprocs must be >= 1", program.name);
        }
        if program.exitcodes.is_empty() {
            anyhow::bail!("program {} exitcodes must not be empty", program.name);
        }
        if !names.insert(program.name.clone()) {
            anyhow::bail!("duplicate program name {}", program.name);
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Autorestart {
    Never,
    Always,
    #[default]
    Unexpected,
}

impl<'de> Deserialize<'de> for Autorestart {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct AutorestartVisitor;

        impl<'de> serde::de::Visitor<'de> for AutorestartVisitor {
            type Value = Autorestart;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("bool or string (true/false/always/never/unexpected)")
            }

            fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(if v {
                    Autorestart::Always
                } else {
                    Autorestart::Never
                })
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match v.to_lowercase().as_str() {
                    "true" | "always" => Ok(Autorestart::Always),
                    "false" | "never" => Ok(Autorestart::Never),
                    "unexpected" => Ok(Autorestart::Unexpected),
                    other => Err(E::custom(format!("invalid autorestart {other}"))),
                }
            }
        }

        deserializer.deserialize_any(AutorestartVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn make_program(name: &str, command: &str) -> ProgramConfig {
        ProgramConfig {
            name: name.to_string(),
            command: command.to_string(),
            cwd: None,
            autostart: true,
            autorestart: Autorestart::Unexpected,
            numprocs: 1,
            environment: HashMap::new(),
            stdout_log: None,
            stderr_log: None,
            stdout_log_max_bytes: None,
            stdout_log_backups: None,
            stderr_log_max_bytes: None,
            stderr_log_backups: None,
            startretries: 3,
            startsecs: 1,
            exitcodes: vec![0],
            stopsignal: "TERM".to_string(),
            stopwaitsecs: 10,
            killasgroup: false,
        }
    }

    // --- Autorestart deserialization ---

    #[test]
    fn autorestart_bool_true_is_always() {
        let p: ProgramConfig =
            toml::from_str("name = \"p\"\ncommand = \"echo\"\nautorestart = true\n").unwrap();
        assert_eq!(p.autorestart, Autorestart::Always);
    }

    #[test]
    fn autorestart_bool_false_is_never() {
        let p: ProgramConfig =
            toml::from_str("name = \"p\"\ncommand = \"echo\"\nautorestart = false\n").unwrap();
        assert_eq!(p.autorestart, Autorestart::Never);
    }

    #[test]
    fn autorestart_string_always() {
        let p: ProgramConfig =
            toml::from_str("name = \"p\"\ncommand = \"echo\"\nautorestart = \"always\"\n").unwrap();
        assert_eq!(p.autorestart, Autorestart::Always);
    }

    #[test]
    fn autorestart_string_never() {
        let p: ProgramConfig =
            toml::from_str("name = \"p\"\ncommand = \"echo\"\nautorestart = \"never\"\n").unwrap();
        assert_eq!(p.autorestart, Autorestart::Never);
    }

    #[test]
    fn autorestart_string_unexpected() {
        let p: ProgramConfig =
            toml::from_str("name = \"p\"\ncommand = \"echo\"\nautorestart = \"unexpected\"\n")
                .unwrap();
        assert_eq!(p.autorestart, Autorestart::Unexpected);
    }

    #[test]
    fn autorestart_default_is_unexpected() {
        let p: ProgramConfig = toml::from_str("name = \"p\"\ncommand = \"echo\"\n").unwrap();
        assert_eq!(p.autorestart, Autorestart::Unexpected);
    }

    // --- Default field values ---

    #[test]
    fn defaults_applied_on_minimal_program() {
        let p: ProgramConfig = toml::from_str("name = \"p\"\ncommand = \"echo\"\n").unwrap();
        assert!(p.autostart);
        assert_eq!(p.numprocs, 1);
        assert_eq!(p.startretries, 3);
        assert_eq!(p.startsecs, 1);
        assert_eq!(p.exitcodes, vec![0]);
        assert_eq!(p.stopsignal, "TERM");
        assert_eq!(p.stopwaitsecs, 10);
        assert!(!p.killasgroup);
    }

    // --- validate_programs error cases ---

    #[test]
    fn validate_rejects_empty_name() {
        assert!(validate_programs(&[make_program("", "echo")]).is_err());
    }

    #[test]
    fn validate_rejects_whitespace_only_name() {
        assert!(validate_programs(&[make_program("   ", "echo")]).is_err());
    }

    #[test]
    fn validate_rejects_empty_command() {
        assert!(validate_programs(&[make_program("p", "")]).is_err());
    }

    #[test]
    fn validate_rejects_whitespace_only_command() {
        assert!(validate_programs(&[make_program("p", "   ")]).is_err());
    }

    #[test]
    fn validate_rejects_zero_numprocs() {
        let mut p = make_program("p", "echo");
        p.numprocs = 0;
        assert!(validate_programs(&[p]).is_err());
    }

    #[test]
    fn validate_rejects_duplicate_names() {
        assert!(validate_programs(&[make_program("p", "echo"), make_program("p", "ls")]).is_err());
    }

    #[test]
    fn validate_rejects_empty_exitcodes() {
        let mut p = make_program("p", "echo");
        p.exitcodes = vec![];
        assert!(validate_programs(&[p]).is_err());
    }

    #[test]
    fn validate_accepts_valid_programs() {
        assert!(
            validate_programs(&[make_program("a", "echo"), make_program("b", "sleep 1")]).is_ok()
        );
    }

    // --- Config::autostart_programs ---

    #[test]
    fn autostart_programs_only_returns_enabled() {
        let mut a = make_program("a", "echo");
        a.autostart = true;
        let mut b = make_program("b", "echo");
        b.autostart = false;
        let config = Config {
            supervisor: SupervisorConfig::default(),
            programs: vec![a, b],
        };
        assert_eq!(config.autostart_programs(), vec!["a"]);
    }

    #[test]
    fn autostart_programs_empty_when_all_disabled() {
        let mut p = make_program("p", "echo");
        p.autostart = false;
        let config = Config {
            supervisor: SupervisorConfig::default(),
            programs: vec![p],
        };
        assert!(config.autostart_programs().is_empty());
    }

    // --- load() ---

    #[test]
    fn load_from_toml_file() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            r#"[supervisord]
sock_path = "/tmp/test_load.sock"

[[programs]]
name = "hello"
command = "echo hello"
autostart = false"#
        )
        .unwrap();
        let config = load(Some(file.path())).unwrap();
        assert_eq!(
            config.supervisor.sock_path,
            PathBuf::from("/tmp/test_load.sock")
        );
        assert_eq!(config.programs.len(), 1);
        assert_eq!(config.programs[0].name, "hello");
        assert!(!config.programs[0].autostart);
    }

    #[test]
    fn load_nonexistent_path_returns_defaults() {
        let config = load(Some(Path::new("/nonexistent/path/supervisord.toml"))).unwrap();
        assert!(config.programs.is_empty());
        assert_eq!(
            config.supervisor.sock_path,
            PathBuf::from("/tmp/rvisor.sock")
        );
    }

    #[test]
    fn load_invalid_toml_returns_error() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "[[programs]]\nname = !!invalid").unwrap();
        assert!(load(Some(file.path())).is_err());
    }

    #[test]
    fn load_duplicate_program_names_returns_error() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            "[[programs]]\nname = \"p\"\ncommand = \"echo\"\n\n[[programs]]\nname = \"p\"\ncommand = \"ls\"\n"
        )
        .unwrap();
        assert!(load(Some(file.path())).is_err());
    }
}
