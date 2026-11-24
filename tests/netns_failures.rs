//! Uplink failure tests that change interfaces and addresses. They need
//! CAP_NET_ADMIN, so they are ignored by default and only run as an
//! unprivileged user inside a network namespace owned by a new user namespace:
//!
//! ```sh
//! cargo test --test netns_failures --no-run
//! MOBLINK_TEST_NETNS=1 unshare --map-root-user --net -- \
//!     sh -c 'ip link set lo up && cargo test --offline --test netns_failures -- --ignored'
//! ```
//!
//! Prerequisites: iproute2, unprivileged user namespaces (on Ubuntu 24.04 and
//! later, `sysctl kernel.apparmor_restrict_unprivileged_userns=0`), and the
//! dummy driver loaded beforehand with `modprobe dummy numdummies=0`, since
//! modules do not load from inside a user namespace.
//!
//! Each test gives the relay its own dummy interface as the uplink and binds
//! the relay's destination socket to its address. The fake streamer and the
//! receiver stay on loopback. The tests check that tagged probes do not reach
//! the receiver while the uplink address is gone, that the relay keeps
//! running, and that traffic flows again within a bound after it returns.

mod common;

use std::cell::Cell;
use std::fs::File;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::fs::MetadataExt;
use std::process::Command;
use std::time::Duration;

use common::*;
use tokio::time::{sleep, Instant};
use uuid::Uuid;

fn ip(args: &[&str]) {
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
fn require_private_netns() {
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

/// A dummy interface standing in for the relay's uplink. The name carries
/// the process id, and drop deletes the link only while this test created it,
/// so a failing test leaves nothing behind and never removes a link it does
/// not own.
struct Uplink {
    name: String,
    address: &'static str,
    created: Cell<bool>,
}

impl Uplink {
    fn add(index: u32, address: &'static str) -> Self {
        let uplink = Self {
            name: format!("mlt{index}p{}", std::process::id()),
            address,
            created: Cell::new(false),
        };
        uplink.plug();
        uplink
    }

    fn plug(&self) {
        ip(&["link", "add", &self.name, "type", "dummy"]);
        self.created.set(true);
        ip(&[
            "addr",
            "add",
            &format!("{}/32", self.address),
            "dev",
            &self.name,
        ]);
        ip(&["link", "set", &self.name, "up"]);
    }

    fn unplug(&self) {
        ip(&["link", "del", &self.name]);
        self.created.set(false);
    }
}

impl Drop for Uplink {
    fn drop(&mut self) {
        if self.created.get() {
            Command::new("ip")
                .args(["link", "del", &self.name])
                .output()
                .ok();
        }
    }
}

/// Waits until a session carries traffic to `receiver` again: the existing
/// tunnel if it recovers, or a tunnel on each new session the relay opens.
/// Returns how long it took.
async fn wait_for_working_tunnel(
    mut current: Option<(Session, u16)>,
    moblin: &mut FakeMoblin,
    receiver: &EchoReceiver,
    udp: &StreamerUdp,
    limit: Duration,
) -> Duration {
    let started = Instant::now();
    let mut attempt = 0u32;
    loop {
        assert!(
            started.elapsed() < limit,
            "no working tunnel within {limit:?}"
        );
        if let Some(mut session) = moblin.try_next_session() {
            let port = session.start_tunnel(receiver.addr).await;
            current = Some((session, port));
        }
        if let Some((session, port)) = &current {
            attempt += 1;
            let payload = format!("probe {attempt}");
            if !session.is_closed() && udp.round_trip(*port, payload.as_bytes()).await {
                // The echo may arrive after the deadline that let the probe
                // start, so check it again.
                let recovered = started.elapsed();
                assert!(
                    recovered < limit,
                    "traffic only resumed after {recovered:?}, limit {limit:?}"
                );
                return recovered;
            }
        }
        sleep(Duration::from_millis(250)).await;
    }
}

/// The uplink loses its address (DHCP lease lost, carrier dropped the PDP
/// context) and gets the same address back. Nothing may leak out while it is
/// gone, and the relay must carry traffic again after it returns.
#[tokio::test]
#[ignore = "needs a private network namespace, see the file header"]
async fn uplink_address_lost_and_restored() {
    require_private_netns();
    let uplink = Uplink::add(1, "192.0.2.1");
    let address = uplink.address;

    let mut moblin = FakeMoblin::start().await;
    let receiver = EchoReceiver::bind("127.0.0.1:0").await;
    let udp = StreamerUdp::bind().await;
    let mut relay = RelayProcess::spawn("address-loss", Uuid::new_v4(), moblin.addr, address);
    let mut session = moblin.next_session(Duration::from_secs(5)).await;
    let port = session.start_tunnel(receiver.addr).await;
    assert!(udp.round_trip(port, b"before address loss").await);

    ip(&["addr", "del", &format!("{address}/32"), "dev", &uplink.name]);
    assert!(!udp.round_trip(port, b"while the address is gone").await);
    sleep(Duration::from_secs(6)).await;
    assert!(!udp.round_trip(port, b"still gone").await);
    assert!(!receiver.has_received(b"while the address is gone"));
    assert!(!receiver.has_received(b"still gone"));

    ip(&["addr", "add", &format!("{address}/32"), "dev", &uplink.name]);
    let recovery = wait_for_working_tunnel(
        Some((session, port)),
        &mut moblin,
        &receiver,
        &udp,
        Duration::from_secs(45),
    )
    .await;
    eprintln!("address restored, traffic again after {recovery:?}");
    assert!(relay.is_running());
}

/// The uplink interface disappears and comes back with the same address, as
/// when a USB modem is unplugged and replugged. The relay must survive it and
/// carry traffic again afterwards.
#[tokio::test]
#[ignore = "needs a private network namespace, see the file header"]
async fn uplink_interface_removed_and_replugged() {
    require_private_netns();
    let uplink = Uplink::add(2, "192.0.2.2");
    let address = uplink.address;

    let mut moblin = FakeMoblin::start().await;
    let receiver = EchoReceiver::bind("127.0.0.1:0").await;
    let udp = StreamerUdp::bind().await;
    let mut relay = RelayProcess::spawn("replug", Uuid::new_v4(), moblin.addr, address);
    let mut session = moblin.next_session(Duration::from_secs(5)).await;
    let port = session.start_tunnel(receiver.addr).await;
    assert!(udp.round_trip(port, b"before unplug").await);

    uplink.unplug();
    assert!(!udp.round_trip(port, b"while unplugged").await);
    sleep(Duration::from_secs(6)).await;
    assert!(!udp.round_trip(port, b"still unplugged").await);
    assert!(!receiver.has_received(b"while unplugged"));
    assert!(!receiver.has_received(b"still unplugged"));
    assert!(relay.is_running(), "relay exited when its uplink vanished");

    uplink.plug();
    let recovery = wait_for_working_tunnel(
        Some((session, port)),
        &mut moblin,
        &receiver,
        &udp,
        Duration::from_secs(45),
    )
    .await;
    eprintln!("interface replugged, traffic again after {recovery:?}");
    assert!(relay.is_running());
}
