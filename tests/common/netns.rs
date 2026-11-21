use std::fs::File;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::fs::MetadataExt;
use std::process::Command;

pub fn ip(args: &[&str]) {
    let output = Command::new("ip").args(args).output().expect("run ip(8)");
    assert!(
        output.status.success(),
        "ip {}: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
}

/// `NS_GET_USERNS` from linux/nsfs.h: returns the user namespace that owns a
/// namespace.
const NS_GET_USERNS: u64 = 0xb701;

/// Refuses to touch anything unless this runs in a user namespace other than
/// the initial one, and the network namespace is owned by that user namespace.
/// There, CAP_NET_ADMIN only covers namespaces it owns, so even a mistake
/// cannot change the host's network: the kernel would refuse with EPERM. Run
/// as real root, or in a user namespace without its own network namespace,
/// the tests fail before any change. Any lookup error also fails the test.
pub fn require_private_netns() {
    assert_eq!(
        std::env::var("MOBLINK_TEST_NETNS").as_deref(),
        Ok("1"),
        "set MOBLINK_TEST_NETNS=1 and run under unshare, see the file header"
    );
    let uid_map = std::fs::read_to_string("/proc/self/uid_map").expect("read /proc/self/uid_map");
    // The initial user namespace maps every id onto itself.
    let fields: Vec<&str> = uid_map.split_whitespace().collect();
    assert!(
        !fields.is_empty() && fields != ["0", "0", "4294967295"],
        "running in the initial user namespace; use unshare --map-root-user --net"
    );
    let net = File::open("/proc/self/ns/net").expect("open /proc/self/ns/net");
    // SAFETY: NS_GET_USERNS takes no argument and returns a new descriptor,
    // which the File below owns.
    let owner = unsafe { libc::ioctl(net.as_raw_fd(), NS_GET_USERNS as _) };
    assert!(
        owner >= 0,
        "NS_GET_USERNS failed: {}",
        std::io::Error::last_os_error()
    );
    let owner = unsafe { File::from_raw_fd(owner) }
        .metadata()
        .expect("stat owning user namespace");
    let current = std::fs::metadata("/proc/self/ns/user").expect("stat /proc/self/ns/user");
    assert!(
        owner.dev() == current.dev() && owner.ino() == current.ino(),
        "the network namespace is not owned by this user namespace; use unshare --map-root-user \
         --net"
    );
    let output = Command::new("ip")
        .args(["-o", "link", "show"])
        .output()
        .expect("run ip(8)");
    assert!(output.status.success(), "ip -o link show failed");
    let links = String::from_utf8_lossy(&output.stdout);
    assert!(
        links.lines().any(|line| line.contains(": lo:")),
        "no loopback interface listed"
    );
    for line in links.lines() {
        let name = line.split(':').nth(1).unwrap_or("").trim();
        let name = name.split('@').next().unwrap_or(name);
        assert!(
            name == "lo" || name.starts_with("mlt"),
            "interface {name} present: not a fresh network namespace"
        );
    }
}
