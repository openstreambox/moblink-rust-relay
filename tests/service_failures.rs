//! Systemd service qualification against checksum-verified release binaries.
//! Prerequisites and exact commands are in tests/README.md.
mod common;
#[path = "common/netns.rs"]
mod netns;
#[path = "common/systemd.rs"]
mod systemd;
#[path = "common/veth.rs"]
mod veth;

use std::net::SocketAddr;
use std::time::Duration;

use common::*;
use mdns_sd::{IfKind, ServiceDaemon, ServiceEvent, ServiceInfo, UnregisterStatus};
use netns::{ip, require_private_netns};
use systemd::{LEGACY_OWNER, NO_OWNER, SYSTEMD_OWNER, Unit, release_binary};
use tokio::time::{Instant, sleep, timeout};
use veth::{PHONE, Peer, WAN_A, WAN_B};

const START: Duration = Duration::from_secs(12);
const QUIET: Duration = Duration::from_millis(450);
const MDNS_TYPE: &str = "_moblink._tcp.local.";

struct Bench {
    peer: Peer,
    streamer: FakeMoblin,
    udp: StreamerUdp,
    receiver: EchoReceiver,
}

impl Bench {
    fn new() -> Self {
        require_private_netns();
        // Both binaries must come from the checksum verification step.
        release_binary("MOBLINK_RELAY_BIN");
        let peer = Peer::new();
        PHONE.add(&peer, 1);
        WAN_A.add(&peer, 1);
        let streamer = FakeMoblin::from_listener(peer.tcp(PHONE.ip(2)));
        let udp = StreamerUdp {
            socket: peer.udp(PHONE.ip(2)),
        };
        let receiver = EchoReceiver::from_socket(peer.udp(WAN_A.ip(2)));
        Self {
            peer,
            streamer,
            udp,
            receiver,
        }
    }
}

struct Tunnel {
    session: Session,
    socket: SocketAddr,
}

impl Tunnel {
    async fn start(mut session: Session, receiver: &EchoReceiver) -> Self {
        let port = session.start_tunnel(receiver.addr).await;
        Self {
            session,
            socket: (PHONE.ip(1), port).into(),
        }
    }

    async fn carries(&self, udp: &StreamerUdp, tag: &str) {
        for attempt in 0..3 {
            let payload = format!("{tag}-{attempt}");
            if udp.round_trip_to(self.socket, payload.as_bytes()).await {
                return;
            }
        }
        panic!("{tag}: no tagged echo through {}", self.socket);
    }

    async fn quiet(&self, udp: &StreamerUdp, tag: &str, receivers: &[&EchoReceiver]) {
        udp.assert_quiet_on(self.socket, tag.as_bytes(), receivers, QUIET)
            .await;
    }
}

async fn assert_netem(tunnel: &Tunnel, udp: &StreamerUdp, receiver: &EchoReceiver) {
    const COUNT: u8 = 120;
    let before = receiver.count();
    let (sent, all_sent) = tokio::sync::oneshot::channel();
    let mut echoed = Vec::new();
    let send = async {
        for sequence in 0..COUNT {
            udp.socket
                .send_to(&[0xfe, sequence], tunnel.socket)
                .await
                .unwrap();
            sleep(Duration::from_millis(2)).await;
        }
        sent.send(()).unwrap();
    };
    let recv = async {
        let mut buf = [0; 2048];
        loop {
            if let Ok((2, _)) = udp.socket.recv_from(&mut buf).await
                && buf[0] == 0xfe
            {
                echoed.push(buf[1]);
            }
        }
    };
    let drain = async {
        all_sent.await.unwrap();
        // Wait after the last send, so a slow sender does not turn delayed
        // packets into apparent loss. Both qdiscs have a 60 ms delay.
        sleep(Duration::from_millis(500)).await;
    };
    tokio::join!(send, async {
        tokio::select! { () = recv => {}, () = drain => {} }
    });
    let received = receiver.received();
    let ingress: Vec<_> = received[before..]
        .iter()
        .filter_map(|(p, source)| {
            assert_eq!(source.ip(), WAN_A.ip(1));
            (p.len() == 2 && p[0] == 0xfe).then(|| p[1])
        })
        .collect();
    assert!(
        (25..usize::from(COUNT)).contains(&ingress.len()),
        "egress loss: {}",
        ingress.len()
    );
    assert!(
        (15..ingress.len()).contains(&echoed.len()),
        "return loss: {}",
        echoed.len()
    );
    assert!(
        ingress.windows(2).any(|pair| pair[0] > pair[1]),
        "no egress reordering"
    );
    // Index echoes by receiver arrival order. Loss can remove entries
    // from this sequence, but only reverse-path reordering can invert it.
    let return_order: Vec<_> = echoed
        .iter()
        .map(|sequence| ingress.iter().position(|seen| seen == sequence).unwrap())
        .collect();
    assert!(
        return_order.windows(2).any(|pair| pair[0] > pair[1]),
        "no return-path reordering"
    );
    eprintln!(
        "netem delivered {} outbound and {} round trips",
        ingress.len(),
        echoed.len()
    );
}

#[tokio::test]
#[ignore = "requires the private systemd/netns suite"]
async fn service_uplinks_survive_netem_cuts_renumbering_and_replug() {
    let mut bench = Bench::new();
    WAN_B.add(&bench.peer, 1);
    let receiver_b = EchoReceiver::from_socket(bench.peer.udp(WAN_B.ip(2)));
    let udp_b = StreamerUdp {
        socket: bench.peer.udp(PHONE.ip(2)),
    };
    let unit = Unit::install("mltwan[ab]", Some(bench.streamer.addr));
    unit.set_owner(SYSTEMD_OWNER);
    unit.action("start");
    let pid = unit.pid();

    let first = bench.streamer.next_session(START).await;
    let second = bench.streamer.next_session(START).await;
    let (a, b) = if first.name == WAN_A.relay {
        (first, second)
    } else {
        (second, first)
    };
    assert_eq!(a.name, WAN_A.relay);
    assert_eq!(b.name, WAN_B.relay);
    let mut a = Tunnel::start(a, &bench.receiver).await;
    let b = Tunnel::start(b, &receiver_b).await;
    let id = a.session.relay_id;
    a.carries(&bench.udp, "before impairment A").await;
    b.carries(&udp_b, "before impairment B").await;

    // A deterministic delay first, then loss and reordering. Apply every qdisc
    // to both veth ends, so the impairment affects both directions.
    WAN_A.netem(&bench.peer, &["delay", "60ms"]);
    let delayed = Instant::now();
    assert!(
        bench
            .udp
            .round_trip_to(a.socket, b"single delayed packet")
            .await
    );
    assert!(
        delayed.elapsed() >= Duration::from_millis(110),
        "delay missing on one direction"
    );
    WAN_A.netem(
        &bench.peer,
        &["delay", "60ms", "loss", "20%", "reorder", "75%", "gap", "3"],
    );
    tokio::join!(
        assert_netem(&a, &bench.udp, &bench.receiver),
        b.carries(&udp_b, "unaffected during netem")
    );
    assert_eq!(receiver_b.received().last().unwrap().1.ip(), WAN_B.ip(1));

    // Demonstrate that the original tunnel can age without closing while
    // receiving replies. The cut must start a fresh destination deadline.
    WAN_A.netem(&bench.peer, &[]);
    let age_started = Instant::now();
    while age_started.elapsed() < Duration::from_secs(8) {
        let sequence = age_started.elapsed().as_millis();
        a.carries(&bench.udp, &format!("age uplink A {sequence}"))
            .await;
        b.carries(&udp_b, &format!("age uplink B {sequence}")).await;
        sleep(Duration::from_millis(250)).await;
    }
    let cut = Instant::now();
    WAN_A.netem(&bench.peer, &["loss", "100%"]);
    a.quiet(&bench.udp, "blackhole A", &[&bench.receiver, &receiver_b])
        .await;
    b.carries(&udp_b, "unaffected during blackhole").await;
    // Retain the IP and keep sending into both tunnels. Check control
    // during the outage and time the close as seen by the WebSocket task.
    let pump = async {
        let mut sequence = 0;
        loop {
            let payload = format!("blackhole keep sending {sequence}");
            bench
                .udp
                .socket
                .send_to(payload.as_bytes(), a.socket)
                .await
                .unwrap();
            b.carries(&udp_b, &format!("unaffected during blackhole {sequence}"))
                .await;
            sequence += 1;
            sleep(Duration::from_millis(250)).await;
        }
    };
    let observe = async {
        sleep(Duration::from_secs(12)).await;
        let status = a.session.status().await;
        assert!(status["response"]["result"].get("ok").is_some(), "{status}");
        a.session.wait_ended(Duration::from_secs(28)).await
    };
    let ended = tokio::select! {
        ended = observe => ended,
        () = pump => unreachable!(),
    };
    let elapsed = ended.at - cut;
    assert!(
        (Duration::from_secs(25)..=Duration::from_secs(40)).contains(&elapsed),
        "destination timeout outside its window: {elapsed:?}, {ended:?}"
    );
    for receiver in [&bench.receiver, &receiver_b] {
        assert!(
            !receiver
                .received()
                .iter()
                .any(|(payload, _)| { payload.starts_with(b"blackhole keep sending") })
        );
    }
    let retry = bench.streamer.next_session(START).await;
    assert_eq!(retry.relay_id, id);
    WAN_A.netem(&bench.peer, &[]);
    a.quiet(
        &bench.udp,
        "retired blackhole tunnel",
        &[&bench.receiver, &receiver_b],
    )
    .await;
    a = Tunnel::start(retry, &bench.receiver).await;
    a.carries(&bench.udp, "recovered from blackhole").await;
    // Cut a live path, then remove its address to exercise reconciliation.
    ip(&["link", "set", WAN_A.relay, "down"]);
    a.quiet(&bench.udp, "link down A", &[&bench.receiver, &receiver_b])
        .await;
    b.carries(&udp_b, "unaffected during link down").await;
    WAN_A.change_address("del", 1);
    a.session.wait_ended(START).await;
    ip(&["link", "set", WAN_A.relay, "up"]);

    // Keep the old port unallocated when proving it is quiet.
    a.quiet(
        &bench.udp,
        "address removed A",
        &[&bench.receiver, &receiver_b],
    )
    .await;
    WAN_A.change_address("add", 3);
    let renumbered = bench.streamer.next_session(START).await;
    assert_eq!(renumbered.relay_id, id);
    assert_ne!(renumbered.challenge, a.session.challenge);
    assert_retired_socket(a.socket.port());
    a = Tunnel::start(renumbered, &bench.receiver).await;
    a.carries(&bench.udp, "renumbered A").await;
    let (payload, source) = bench.receiver.received().pop().unwrap();
    assert!(payload.starts_with(b"renumbered A-"));
    assert_eq!(source.ip(), WAN_A.ip(3));

    ip(&["link", "del", WAN_A.relay]);
    a.session.wait_ended(START).await;
    a.quiet(&bench.udp, "unplugged A", &[&bench.receiver, &receiver_b])
        .await;
    b.carries(&udp_b, "unaffected during replug").await;
    // Restore both endpoints with their old addresses so a surviving
    // tunnel would have a route to its original receiver.
    WAN_A.add(&bench.peer, 3);
    let replugged = bench.streamer.next_session(START).await;
    a.quiet(
        &bench.udp,
        "old tunnel on restored link",
        &[&bench.receiver, &receiver_b],
    )
    .await;
    assert_retired_socket(a.socket.port());
    let fresh = Tunnel::start(replugged, &bench.receiver).await;
    assert_eq!(fresh.session.relay_id, id);
    fresh.carries(&bench.udp, "replugged A").await;
    assert_eq!(
        bench.receiver.received().last().unwrap().1.ip(),
        WAN_A.ip(3)
    );
    assert_eq!(unit.pid(), pid);
}

fn assert_retired_socket(port: u16) {
    // The streamer-facing socket binds wildcard IPv4.
    std::net::UdpSocket::bind(("0.0.0.0", port))
        .unwrap_or_else(|error| panic!("retired UDP port {port} is still bound: {error}"));
}

async fn assert_authenticated(unit: &Unit, pid: u32, manual: bool) {
    unit.wait_for("fresh authenticated status", || {
        unit.status().is_some_and(|status| {
            status["schema_version"] == 2
                && status["process_id"] == pid
                && status["manual_streamer"] == manual
                && status["connected"] == true
                && status["updated_at"].as_u64().is_some_and(|at| at > 0)
        })
    })
    .await;
}

#[tokio::test]
#[ignore = "requires the private systemd/netns suite"]
async fn service_crash_backoff_start_limit_and_operator_recovery() {
    let mut bench = Bench::new();
    let unit = Unit::install(WAN_A.relay, Some(bench.streamer.addr));
    unit.set_owner(SYSTEMD_OWNER);
    unit.action("start");
    let pid = unit.pid();
    let mut old = Tunnel::start(bench.streamer.next_session(START).await, &bench.receiver).await;
    old.carries(&bench.udp, "before crash").await;
    assert_authenticated(&unit, pid, true).await;
    assert_eq!(bench.streamer.accepted(), 1);

    let crashed = Instant::now();
    unit.crash();
    old.session.wait_ended(START).await;
    old.quiet(&bench.udp, "crashed tunnel", &[&bench.receiver])
        .await;
    let session = bench.streamer.next_session(START).await;
    assert_eq!(bench.streamer.accepted(), 2);
    let first_restart = session.accepted_at - crashed;
    assert!(
        (Duration::from_millis(900)..Duration::from_secs(10)).contains(&first_restart),
        "first restart outside its 1s backoff window: {first_restart:?}"
    );
    let restarted_pid = unit.pid();
    assert_ne!(restarted_pid, 0);
    assert_ne!(restarted_pid, pid);
    assert_eq!(session.relay_id, old.session.relay_id);
    assert_ne!(session.challenge, old.session.challenge);
    let mut restarted = Tunnel::start(session, &bench.receiver).await;
    restarted.carries(&bench.udp, "after restart").await;
    assert_authenticated(&unit, restarted_pid, true).await;

    // A validated input disappearing before a restart is represented by the
    // pre-start readiness check failing. Leave the activation record in place
    // so systemd must exhaust its restart budget.
    unit.set_ready(false);
    unit.crash();
    restarted.session.wait_ended(START).await;
    unit.wait_for("retry budget exhausted", || {
        unit.property("ActiveState") == "failed"
    })
    .await;
    unit.assert_backoff_journal();
    assert_eq!(unit.pid(), 0);
    restarted
        .quiet(&bench.udp, "rate-limited tunnel", &[&bench.receiver])
        .await;

    unit.set_ready(true);
    // Some systemd versions preserve the last exit code in Result, so
    // exercise the blocked start with the readiness check repaired.
    unit.assert_start_rejected();
    bench
        .streamer
        .assert_no_more_connections(Duration::from_millis(3200))
        .await;
    assert_eq!(unit.property("ActiveState"), "failed");
    unit.action("reset-failed");
    unit.action("start");
    let session = bench.streamer.next_session(START).await;
    assert_eq!(session.relay_id, old.session.relay_id);
    let recovered = Tunnel::start(session, &bench.receiver).await;
    recovered
        .carries(&bench.udp, "after operator recovery")
        .await;
    assert_authenticated(&unit, unit.pid(), true).await;
}

fn restore_legacy(unit: &Unit, id: uuid::Uuid, addr: SocketAddr) -> Option<RelayProcess> {
    if unit.owner() != LEGACY_OWNER {
        return None;
    }
    assert_eq!(unit.pid(), 0, "standalone owner is still running");
    Some(RelayProcess::spawn(
        "legacy",
        id,
        addr,
        &WAN_A.ip(1).to_string(),
    ))
}

#[tokio::test]
#[ignore = "requires the private systemd/netns suite"]
async fn service_rolls_back_to_one_legacy_owner() {
    let mut bench = Bench::new();
    let unit = Unit::install(WAN_A.relay, Some(bench.streamer.addr));
    unit.action("start");
    assert_eq!(unit.pid(), 0, "box without opt-in started a relay");
    bench.streamer.no_session_within(QUIET).await;

    let legacy_id = uuid::Uuid::new_v4();
    unit.set_owner(LEGACY_OWNER);
    let mut legacy = restore_legacy(&unit, legacy_id, bench.streamer.addr).unwrap();
    let mut before = Tunnel::start(bench.streamer.next_session(START).await, &bench.receiver).await;
    assert_eq!(before.session.relay_id, legacy_id);
    before.carries(&bench.udp, "legacy before cutover").await;
    unit.action("start");
    assert_eq!(unit.pid(), 0);
    bench.streamer.no_session_within(QUIET).await;

    // Simulate a transition interrupted after clearing ownership. A retry
    // with neither owner selected must keep the box opted out.
    unit.set_owner(NO_OWNER);
    legacy.kill();
    assert!(!legacy.is_running());
    before.session.wait_ended(START).await;
    before
        .quiet(&bench.udp, "retired legacy tunnel", &[&bench.receiver])
        .await;
    unit.action("start");
    assert_eq!(unit.pid(), 0);
    assert!(restore_legacy(&unit, legacy_id, bench.streamer.addr).is_none());

    unit.set_owner(SYSTEMD_OWNER);
    unit.action("start");
    assert!(restore_legacy(&unit, legacy_id, bench.streamer.addr).is_none());
    let mut standalone =
        Tunnel::start(bench.streamer.next_session(START).await, &bench.receiver).await;
    assert_eq!(standalone.session.name, WAN_A.relay);
    standalone
        .carries(&bench.udp, "standalone after cutover")
        .await;
    let pid = unit.pid();
    assert_ne!(pid, 0);

    unit.set_owner(NO_OWNER);
    unit.action("stop");
    assert_eq!(unit.pid(), 0);
    assert!(
        !std::path::Path::new(&format!("/proc/{pid}")).exists(),
        "old relay process survived stop"
    );
    standalone.session.wait_ended(START).await;
    standalone
        .quiet(&bench.udp, "retired standalone tunnel", &[&bench.receiver])
        .await;
    unit.set_owner(LEGACY_OWNER);
    let mut legacy = restore_legacy(&unit, legacy_id, bench.streamer.addr).unwrap();
    let restored = Tunnel::start(bench.streamer.next_session(START).await, &bench.receiver).await;
    assert_eq!(restored.session.relay_id, legacy_id);
    assert_ne!(restored.session.challenge, before.session.challenge);
    restored.carries(&bench.udp, "legacy after rollback").await;
    unit.action("start");
    assert_eq!(unit.pid(), 0);
    assert!(legacy.is_running());
    bench
        .streamer
        .assert_no_more_connections(Duration::from_secs(2))
        .await;
}

fn publish(publisher: &ServiceDaemon, instance: &str, addr: SocketAddr) -> String {
    let info = ServiceInfo::new(
        MDNS_TYPE,
        instance,
        &format!("{instance}.local."),
        addr.ip().to_string(),
        addr.port(),
        &[("name", "Fixture streamer")][..],
    )
    .unwrap();
    let fullname = info.get_fullname().to_owned();
    publisher.register(info).unwrap();
    fullname
}

async fn confirm_advertisement(name: &str, addr: SocketAddr) {
    let browser = ServiceDaemon::new().unwrap();
    browser.disable_interface(IfKind::All).unwrap();
    browser
        .enable_interface(IfKind::Name(PHONE.relay.into()))
        .unwrap();
    let events = browser.browse(MDNS_TYPE).unwrap();
    timeout(START, async {
        while let Ok(event) = events.recv_async().await {
            if let ServiceEvent::ServiceResolved(info) = event
                && info.get_fullname() == name
                && info.get_port() == addr.port()
                && let std::net::IpAddr::V4(ip) = addr.ip()
                && info.get_addresses_v4().contains(&ip)
            {
                return;
            }
        }
        panic!("mDNS observer shut down before resolving {name}");
    })
    .await
    .unwrap_or_else(|_| panic!("mDNS observer did not resolve {name} at {addr}"));
    timeout(START, browser.shutdown().unwrap().recv_async())
        .await
        .unwrap()
        .unwrap();
}

async fn assert_ignores_advertisements(unit: &Unit, listeners: &[(&FakeMoblin, u32)]) {
    let urls: Vec<_> = listeners
        .iter()
        .map(|(listener, _)| format!("ws://{}", listener.addr))
        .collect();
    let deadline = Instant::now() + Duration::from_secs(7);
    loop {
        for (listener, before) in listeners {
            assert_eq!(
                listener.accepted(),
                *before,
                "unexpected connection to {}",
                listener.addr
            );
        }
        let status = unit.status().expect("runtime status published");
        for field in ["streamers", "relays"] {
            for entry in status[field].as_array().unwrap() {
                assert!(
                    urls.iter()
                        .all(|url| entry["url"] != *url && entry["streamer_url"] != *url),
                    "unexpected advertisement in {field}: {entry}"
                );
            }
        }
        if Instant::now() >= deadline {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
}

async fn mdns_reannouncement(strict_retirement: bool) {
    let mut bench = Bench::new();
    bench.peer.enter(|| {
        // The same multicast LAN and instance, now with a different address.
        ip(&[
            "addr",
            "add",
            &format!("{}/24", PHONE.ip(3)),
            "dev",
            "mltmoblin",
        ]);
    });
    let mut moved = FakeMoblin::from_listener(bench.peer.tcp(PHONE.ip(3)));
    let publisher = bench.peer.enter(|| ServiceDaemon::new().unwrap());
    publisher.disable_interface(IfKind::All).unwrap();
    publisher
        .enable_interface(IfKind::Name("mltmoblin".into()))
        .unwrap();

    let unit = Unit::install(WAN_A.relay, None);
    unit.set_owner(SYSTEMD_OWNER);
    unit.action("start");
    bench.streamer.no_session_within(QUIET).await;
    let instance = format!("fixture-{}", uuid::Uuid::new_v4());
    let name = publish(&publisher, &instance, bench.streamer.addr);
    let mut original =
        Tunnel::start(bench.streamer.next_session(START).await, &bench.receiver).await;
    original.carries(&bench.udp, "initial mDNS discovery").await;
    assert_authenticated(&unit, unit.pid(), false).await;

    let result = publisher.unregister(&name).unwrap();
    assert!(matches!(
        timeout(START, result.recv_async()).await.unwrap().unwrap(),
        UnregisterStatus::OK
    ));
    // Keep the listener alive to catch stale retries. In the retirement
    // test the relay must close the active session on the goodbye alone.
    if !strict_retirement {
        original.session.close().await;
    }
    original.session.wait_ended(START).await;
    original
        .quiet(&bench.udp, "mDNS old port", &[&bench.receiver])
        .await;
    if strict_retirement {
        assert_eq!(bench.streamer.accepted(), 1);
        bench
            .streamer
            .assert_no_more_connections(Duration::from_secs(6))
            .await;
        let status = unit.status().unwrap();
        assert!(
            status["streamers"].as_array().unwrap().is_empty(),
            "{status}"
        );
        assert!(status["relays"].as_array().unwrap().is_empty(), "{status}");
    }
    publish(&publisher, &instance, moved.addr);
    let current = Tunnel::start(moved.next_session(START).await, &bench.receiver).await;
    assert_eq!(original.session.relay_id, current.session.relay_id);
    assert_ne!(original.session.challenge, current.session.challenge);
    current
        .carries(&bench.udp, "renumbered mDNS discovery")
        .await;

    if strict_retirement {
        let status = unit.status().unwrap();
        let moved_url = format!("ws://{}", moved.addr);
        let streamers = status["streamers"].as_array().unwrap();
        let relays = status["relays"].as_array().unwrap();
        assert_eq!(streamers.len(), 1, "{status}");
        assert_eq!(relays.len(), 1, "{status}");
        assert_eq!(streamers[0]["url"], moved_url);
        assert_eq!(relays[0]["streamer_url"], moved_url);
        return;
    }

    // The display TXT name is shared, but distinct instances must both
    // carry traffic. A service advertising one of the relay's own addresses
    // must be skipped, even though a listener exists there.
    let mut same_name = FakeMoblin::from_listener(bench.peer.tcp(PHONE.ip(3)));
    let local = FakeMoblin::bind((PHONE.ip(1), 0).into()).await;
    publish(&publisher, &format!("{instance}-other"), same_name.addr);
    let self_name = publish(&publisher, &format!("{instance}-self"), local.addr);
    confirm_advertisement(&self_name, local.addr).await;
    let other = Tunnel::start(same_name.next_session(START).await, &bench.receiver).await;
    other.carries(&bench.udp, "same display name").await;
    current.carries(&bench.udp, "original display name").await;
    assert_ignores_advertisements(&unit, &[(&local, 0)]).await;

    unit.action("stop");
    let fixed = Unit::install(WAN_A.relay, Some(bench.streamer.addr));
    let moved_before = moved.accepted();
    let same_name_before = same_name.accepted();
    fixed.set_owner(SYSTEMD_OWNER);
    // Old reconnections may have been queued before the mDNS unit stopped.
    while bench.streamer.try_next_session().is_some() {}
    fixed.action("start");
    let direct = Tunnel::start(bench.streamer.next_session(START).await, &bench.receiver).await;
    direct.carries(&bench.udp, "fixed URL bypass").await;
    assert_authenticated(&fixed, fixed.pid(), true).await;
    let unwanted = FakeMoblin::from_listener(bench.peer.tcp(PHONE.ip(3)));
    let fixed_name = publish(&publisher, &format!("{instance}-fixed"), unwanted.addr);
    confirm_advertisement(&fixed_name, unwanted.addr).await;
    assert_ignores_advertisements(
        &fixed,
        &[
            (&unwanted, 0),
            (&moved, moved_before),
            (&same_name, same_name_before),
            (&local, 0),
        ],
    )
    .await;
    let status = fixed.status().unwrap();
    assert_eq!(status["streamers"].as_array().unwrap().len(), 1);
    timeout(START, publisher.shutdown().unwrap().recv_async())
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
#[ignore = "requires the private systemd/netns suite"]
async fn service_discovers_multicast_streamer_and_rediscovery() {
    mdns_reannouncement(false).await;
}

#[tokio::test]
async fn service_retires_withdrawn_mdns_url() {
    // Plain Cargo runs lack a runtime unit, release artifacts and a private netns.
    if std::env::var_os("MOBLINK_TEST_SYSTEMD").is_none() {
        eprintln!("requires tests/run-service-failures.sh --strict-mdns");
        return;
    }
    mdns_reannouncement(true).await;
}
