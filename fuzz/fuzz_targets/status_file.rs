//! The `--status-file` path: arbitrary file contents through
//! `create_get_status_closure`.
//!
//! Whatever the file holds, the closure must return and never panic or hang.
#![no_main]

use std::path::PathBuf;

use libfuzzer_sys::fuzz_target;
use moblink_rust::relay::create_get_status_closure;

thread_local! {
    static RT: tokio::runtime::Runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    static PATH: PathBuf = {
        // tmpfs when available: the target rewrites this file on every input.
        let dir = if std::path::Path::new("/dev/shm").is_dir() {
            PathBuf::from("/dev/shm")
        } else {
            std::env::temp_dir()
        };
        dir.join(format!("moblink-fuzz-status-{}.json", std::process::id()))
    };
}

fuzz_target!(|data: &[u8]| {
    let path = PATH.with(|p| p.clone());
    std::fs::write(&path, data).unwrap();
    let closure = create_get_status_closure(&None, &Some(path.to_string_lossy().into_owned()))
        .expect("a status source was configured");
    let _status = RT.with(|rt| rt.block_on(closure()));
});
