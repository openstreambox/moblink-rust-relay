#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use moblink_rust::relay::create_get_status_closure;
use tokio::sync::Mutex;
use tokio::time::timeout;
use uuid::Uuid;

/// Tests that write a script and run it hold this for their whole body. A
/// freshly written file can fail to execute with "text file busy" if another
/// thread forks between the write and the exec, so no two of them may do so at
/// once.
static SCRIPTS: Mutex<()> = Mutex::const_new(());

struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("moblink-status-limits-{}", Uuid::new_v4()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn file(&self, name: &str, contents: &[u8]) -> String {
        let path = self.0.join(name);
        std::fs::write(&path, contents).unwrap();
        path.to_string_lossy().into_owned()
    }

    fn script(&self, name: &str, body: &str) -> String {
        let path = self.file(name, format!("#!/bin/sh\n{body}\n").as_bytes());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        path
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).ok();
    }
}

async fn battery_from_file(path: String) -> Option<i32> {
    let closure = create_get_status_closure(&None, &Some(path)).unwrap();
    closure().await.battery_percentage
}

async fn battery_from_executable(path: String) -> Option<i32> {
    let closure = create_get_status_closure(&Some(path), &None).unwrap();
    closure().await.battery_percentage
}

/// A valid status followed by enough whitespace to pass the size limit.
fn oversized_status() -> Vec<u8> {
    let mut contents = br#"{"batteryPercentage":73}"#.to_vec();
    contents.resize(200_000, b' ');
    contents
}

#[tokio::test]
async fn status_file_within_the_limit_is_read() {
    let scratch = Scratch::new();
    let path = scratch.file("status.json", br#"{"batteryPercentage":73}"#);
    assert_eq!(battery_from_file(path).await, Some(73));
}

#[tokio::test]
async fn oversized_status_file_is_ignored() {
    let scratch = Scratch::new();
    let path = scratch.file("status.json", &oversized_status());
    assert_eq!(battery_from_file(path).await, None);
}

#[tokio::test]
async fn status_executable_within_the_limit_is_read() {
    let _guard = SCRIPTS.lock().await;
    let scratch = Scratch::new();
    let path = scratch.script("status", r#"printf '{"batteryPercentage":42}\n'"#);
    assert_eq!(battery_from_executable(path).await, Some(42));
}

#[tokio::test]
async fn oversized_status_executable_output_is_ignored() {
    let _guard = SCRIPTS.lock().await;
    let scratch = Scratch::new();
    let path = scratch.script(
        "status",
        r#"printf '{"batteryPercentage":73}'; head -c 200000 /dev/zero | tr '\0' ' '"#,
    );
    assert_eq!(battery_from_executable(path).await, None);
}

#[tokio::test]
async fn endless_status_executable_output_is_cut_off_without_waiting_for_the_timeout() {
    let _guard = SCRIPTS.lock().await;
    let scratch = Scratch::new();
    let path = scratch.script("status", "exec yes");
    let started = Instant::now();
    let battery = timeout(Duration::from_secs(10), battery_from_executable(path))
        .await
        .expect("an endless status must not hang the relay");
    assert_eq!(battery, None);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "took {:?}",
        started.elapsed()
    );
}
