//! Runtime-only system unit for the service tests. Call the private namespace
//! guard before constructing a unit. The relay joins that namespace at exec.
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use serde_json::Value;
use tokio::time::{Instant, sleep};

use super::PASSWORD;

pub const SYSTEMD_OWNER: &str = "systemd";
pub const LEGACY_OWNER: &str = "legacy";
pub const NO_OWNER: &str = "disabled";

const STOCK: &str = include_str!("../../install/belabox/systemd/moblink-relay-service.service");

pub struct Unit {
    name: String,
    state: PathBuf,
}

impl Unit {
    pub fn install(interfaces: &str, streamer: Option<SocketAddr>) -> Self {
        assert_eq!(
            std::env::var("MOBLINK_TEST_SYSTEMD").as_deref(),
            Ok("1"),
            "see the test instructions before running the systemd tests"
        );
        let binary = release_binary("MOBLINK_RELAY_SERVICE_BIN");
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let slot = COUNTER.fetch_add(1, Ordering::Relaxed);
        assert!(slot < 2, "launcher reserves two unit names per case");
        let prefix = std::env::var("MOBLINK_TEST_UNIT_PREFIX").expect("unit prefix from launcher");
        assert!(
            prefix.starts_with("moblink-tests.")
                && prefix
                    .bytes()
                    .all(|c| { c.is_ascii_alphanumeric() || matches!(c, b'.' | b'-' | b'_') })
        );
        let name = format!("{prefix}-{slot}.service");
        let runtime = Path::new("/run/systemd/system");
        let path = runtime.join(&name);
        let dropin = runtime.join(format!("{name}.d"));
        let state = std::env::temp_dir().join(&name);
        fs::create_dir(&state).expect("create test state directory");
        // The launcher owns teardown so that a panic and a killed harness
        // use the same stop confirmation and runtime-file cleanup.
        let unit = Self { name, state };
        unit.set_owner(NO_OWNER);
        unit.set_ready(true);

        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .expect("create runtime test unit");
        writeln!(file, "{STOCK}").unwrap();
        fs::create_dir(&dropin).expect("create test drop-in directory");
        let state = unit_quote(&unit.state);
        let owner = unit_quote(&unit.state.join("owner"));
        let ready = unit_quote(&unit.state.join("ready"));
        let database = unit_quote(&unit.state.join("relay.json"));
        let status = unit_quote(&unit.state.join("status.json"));
        let binary = unit_quote(&binary);
        let pid = std::process::id();
        let streamer = streamer
            .map(|addr| format!(" --streamer-url ws://{addr}"))
            .unwrap_or_default();
        fs::write(
            dropin.join("box.conf"),
            format!(
                include_str!("../fixtures/box.conf"),
                state = state,
                pid = pid,
                owner = owner,
                ready = ready,
                binary = binary,
                password = PASSWORD,
                interfaces = interfaces,
                database = database,
                status = status,
                streamer = streamer,
            ),
        )
        .expect("write test drop-in");
        assert_ok(ctl(&["daemon-reload"]));
        unit
    }

    /// Rename the activation record atomically. Both fixture owners consult
    /// this one record, so a partial write cannot grant ownership.
    pub fn set_owner(&self, owner: &str) {
        let temp = self.state.join("owner.new");
        fs::write(&temp, format!("v1 {owner}\n")).unwrap();
        fs::rename(temp, self.state.join("owner")).unwrap();
    }

    pub fn owner(&self) -> String {
        fs::read_to_string(self.state.join("owner"))
            .unwrap()
            .trim_end()
            .strip_prefix("v1 ")
            .unwrap_or(NO_OWNER)
            .to_string()
    }

    pub fn set_ready(&self, ready: bool) {
        let path = self.state.join("ready");
        if ready {
            fs::write(path, "").unwrap();
        } else {
            fs::remove_file(path).unwrap();
        }
    }

    pub fn action(&self, verb: &str) {
        assert_ok(ctl(&[verb, &self.name]));
    }

    pub async fn assert_start_rejected(&self) {
        let output = ctl(&["start", &self.name]);
        assert!(
            !output.status.success(),
            "rate-limited unit accepted a start: {output:?}"
        );
        self.wait_for("systemd start-rate rejection in journal or result", || {
            self.property("Result") == "start-limit-hit"
                || self
                    .journal()
                    .iter()
                    .filter(|entry| start_limited(entry))
                    .count()
                    >= 2
        })
        .await;
    }

    fn journal(&self) -> Vec<Value> {
        let output = Command::new("journalctl")
            .args(["--no-pager", "--output=json", "--unit", &self.name])
            .output()
            .expect("read system journal");
        assert!(output.status.success(), "{output:?}");
        String::from_utf8(output.stdout)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    pub async fn assert_backoff_journal(&self) {
        self.wait_for("start-limited attempt in journal", || {
            self.journal().iter().any(start_limited)
        })
        .await;
        let journal = self.journal();
        assert_eq!(
            journal
                .iter()
                .filter(|entry| entry["MESSAGE"] == "fixture-pre-start-failed")
                .count(),
            2
        );
        let events: Vec<_> = journal
            .iter()
            .filter_map(|entry| {
                let message = entry["MESSAGE"].as_str()?;
                let time: u64 = entry["__MONOTONIC_TIMESTAMP"].as_str()?.parse().unwrap();
                let phase = match entry["N_RESTARTS"].as_str() {
                    Some("1") => "retry 1",
                    Some("2") => "retry 2",
                    Some("3") => "retry 3",
                    Some("4") => "retry 4",
                    Some(other) => panic!("unexpected restart counter {other}"),
                    None if message.contains("Main process exited")
                        && entry["EXIT_STATUS"] == "9" =>
                    {
                        "crash"
                    }
                    None if message.contains("Control process exited")
                        && entry["EXIT_STATUS"] == "1" =>
                    {
                        "pre-start exit"
                    }
                    None if start_limited(entry) => "limit",
                    None => return None,
                };
                Some((phase, time))
            })
            .collect();
        assert_eq!(
            events.iter().map(|(phase, _)| *phase).collect::<Vec<_>>(),
            [
                "crash",
                "retry 1",
                "crash",
                "retry 2",
                "pre-start exit",
                "retry 3",
                "pre-start exit",
                "retry 4",
                "limit"
            ],
            "{events:?}"
        );
        // RestartSec=1s, RestartSteps=2 and RestartMaxDelaySec=3s
        // give 1, sqrt(3), 3 and 3 seconds. Use manager-to-manager
        // timestamps to exclude the start job and pre-start process time.
        // Leave room for timer coalescing and journal ingestion, while
        // keeping the first and intermediate retry windows disjoint.
        for (pair, (min_ms, max_ms)) in
            events
                .chunks_exact(2)
                .zip([(950, 1400), (1650, 2100), (2900, 3400), (2900, 3400)])
        {
            let gap = Duration::from_micros(pair[1].1 - pair[0].1);
            assert!(
                (Duration::from_millis(min_ms)..=Duration::from_millis(max_ms)).contains(&gap),
                "{} after {} outside {min_ms}–{max_ms} ms: {gap:?}",
                pair[1].0,
                pair[0].0,
            );
        }
    }

    pub fn crash(&self) {
        assert_ok(ctl(&["kill", "--signal=SIGKILL", &self.name]));
    }

    pub fn property(&self, property: &str) -> String {
        let output = ctl(&["show", "--value", "--property", property, &self.name]);
        assert!(output.status.success(), "{output:?}");
        String::from_utf8(output.stdout).unwrap().trim().into()
    }

    pub fn pid(&self) -> u32 {
        self.property("MainPID").parse().unwrap()
    }

    pub fn status(&self) -> Option<Value> {
        let content = fs::read(self.state.join("status.json")).ok()?;
        Some(serde_json::from_slice(&content).expect("parse atomic runtime status"))
    }

    pub async fn wait_for(&self, what: &str, mut check: impl FnMut() -> bool) {
        let start = Instant::now();
        loop {
            assert!(
                start.elapsed() < Duration::from_secs(15),
                "{what} timed out, state={}, result={}, pid={}",
                self.property("ActiveState"),
                self.property("Result"),
                self.pid()
            );
            if check() {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    }
}

fn start_limited(entry: &Value) -> bool {
    entry["MESSAGE"]
        .as_str()
        .is_some_and(|message| message.contains("Start request repeated too quickly"))
}

fn ctl(args: &[&str]) -> Output {
    Command::new("timeout")
        .args(["--foreground", "15s", "systemctl", "--no-ask-password"])
        .args(args)
        .output()
        .expect("run systemctl")
}

fn assert_ok(output: Output) {
    assert!(output.status.success(), "{output:?}");
}

pub fn release_binary(key: &str) -> PathBuf {
    let path =
        std::env::var_os(key).unwrap_or_else(|| panic!("set {key} to the verified release binary"));
    fs::canonicalize(path).unwrap_or_else(|error| panic!("resolve {key}: {error}"))
}

fn unit_quote(path: &Path) -> String {
    let path = path.to_str().expect("UTF-8 test path");
    assert!(
        !path.contains(['\n', '\r', '\\', '"', '%', '$']),
        "unsupported characters in systemd test path"
    );
    format!("\"{path}\"")
}
