//! Uplink failure tests that change interfaces and addresses. They need
//! CAP_NET_ADMIN, so they are ignored by default and must run inside a private
//! network namespace, never on a host network:
//!
//! ```sh
//! cargo test --test netns_failures --no-run
//! MOBLINK_TEST_NETNS=1 unshare --map-root-user --net -- \
//!     sh -c 'ip link set lo up && cargo test --offline --test netns_failures -- --ignored'
//! ```
//!
//! Each test gives the relay its own dummy interface as the uplink and binds
//! the relay's destination socket to it. The fake streamer and the receiver
//! stay on loopback, which keeps working throughout.

mod common;

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

/// Refuses to touch anything unless this is a private namespace: the opt-in
/// variable is set and the only interfaces are loopback and our own.
fn require_private_netns() {
    assert_eq!(
        std::env::var("MOBLINK_TEST_NETNS").as_deref(),
        Ok("1"),
        "set MOBLINK_TEST_NETNS=1 and run inside a private network namespace"
    );
    let output = Command::new("ip")
        .args(["-o", "link", "show"])
        .output()
        .expect("run ip(8)");
    let links = String::from_utf8_lossy(&output.stdout);
    for line in links.lines() {
        let name = line.split(':').nth(1).unwrap_or("").trim();
        let name = name.split('@').next().unwrap_or(name);
        assert!(
            name == "lo" || name.starts_with("mlt"),
            "interface {name} present: not a private network namespace"
        );
    }
}

fn add_uplink(name: &str, address: &str) {
    ip(&["link", "add", name, "type", "dummy"]);
    ip(&["addr", "add", &format!("{address}/32"), "dev", name]);
    ip(&["link", "set", name, "up"]);
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
                return started.elapsed();
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
    let uplink = "mlt1";
    let address = "192.0.2.1";
    add_uplink(uplink, address);

    let mut moblin = FakeMoblin::start().await;
    let receiver = EchoReceiver::bind("127.0.0.1:0").await;
    let udp = StreamerUdp::bind().await;
    let mut relay = RelayProcess::spawn("address-loss", Uuid::new_v4(), moblin.addr, address);
    let mut session = moblin.next_session(Duration::from_secs(5)).await;
    let port = session.start_tunnel(receiver.addr).await;
    assert!(udp.round_trip(port, b"before address loss").await);

    ip(&["addr", "del", &format!("{address}/32"), "dev", uplink]);
    assert!(!udp.round_trip(port, b"while the address is gone").await);
    sleep(Duration::from_secs(6)).await;
    assert!(!udp.round_trip(port, b"still gone").await);
    assert!(!receiver.has_received(b"while the address is gone"));
    assert!(!receiver.has_received(b"still gone"));

    ip(&["addr", "add", &format!("{address}/32"), "dev", uplink]);
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
    ip(&["link", "del", uplink]);
}

/// The uplink interface disappears and comes back with the same address, as
/// when a USB modem is unplugged and replugged. The relay must survive it and
/// carry traffic again afterwards.
#[tokio::test]
#[ignore = "needs a private network namespace, see the file header"]
async fn uplink_interface_removed_and_replugged() {
    require_private_netns();
    let uplink = "mlt2";
    let address = "192.0.2.2";
    add_uplink(uplink, address);

    let mut moblin = FakeMoblin::start().await;
    let receiver = EchoReceiver::bind("127.0.0.1:0").await;
    let udp = StreamerUdp::bind().await;
    let mut relay = RelayProcess::spawn("replug", Uuid::new_v4(), moblin.addr, address);
    let mut session = moblin.next_session(Duration::from_secs(5)).await;
    let port = session.start_tunnel(receiver.addr).await;
    assert!(udp.round_trip(port, b"before unplug").await);

    ip(&["link", "del", uplink]);
    assert!(!udp.round_trip(port, b"while unplugged").await);
    sleep(Duration::from_secs(6)).await;
    assert!(!receiver.has_received(b"while unplugged"));
    assert!(relay.is_running(), "relay exited when its uplink vanished");

    add_uplink(uplink, address);
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
    ip(&["link", "del", uplink]);
}
