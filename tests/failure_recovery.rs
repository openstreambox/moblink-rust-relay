//! Failure tests for `moblink-relay` against a fake Moblin streamer, over
//! loopback only. Each test starts its own relay process, streamer and UDP
//! receiver on ephemeral ports, so they can run in parallel.
//!
//! Timing bounds follow the relay's constants: it reconnects 5 s after losing
//! the streamer, pings every 15 s and gives up 45 s after the last frame it
//! received, and restarts the session when the destination is silent for 30 s.

mod common;

use std::time::Duration;

use common::*;
use tokio::time::{sleep, Instant};
use uuid::Uuid;

const FIRST_SESSION: Duration = Duration::from_secs(5);
const RETIRED_WINDOW: Duration = Duration::from_millis(500);

async fn connected_relay(
    name: &str,
) -> (
    FakeMoblin,
    RelayProcess,
    Session,
    EchoReceiver,
    StreamerUdp,
    u16,
) {
    let mut moblin = FakeMoblin::start().await;
    let receiver = EchoReceiver::bind("127.0.0.1:0").await;
    let udp = StreamerUdp::bind().await;
    let id = Uuid::new_v4();
    let relay = RelayProcess::spawn(name, id, moblin.addr, "127.0.0.1");
    let mut session = moblin.next_session(FIRST_SESSION).await;
    assert_eq!(session.relay_id, id);
    assert_eq!(session.name, name);
    let port = session.start_tunnel(receiver.addr).await;
    assert!(udp.round_trip(port, b"first payload").await);
    (moblin, relay, session, receiver, udp, port)
}

/// The relay process dies (crash, OOM kill, power loss on the relay device).
/// Moblin must see the connection go, the tunnel must stop, and a restarted
/// relay with the same id must authenticate against a new challenge and carry
/// traffic again.
#[tokio::test]
async fn relay_process_loss_and_restart() {
    let (mut moblin, mut relay, mut session, receiver, udp, port) =
        connected_relay("relay-loss").await;
    let id = session.relay_id;

    relay.kill();
    session.wait_closed(Duration::from_secs(2)).await;
    udp.assert_quiet(port, b"sent after relay loss", &receiver, RETIRED_WINDOW)
        .await;

    let mut relay = RelayProcess::spawn("relay-loss", id, moblin.addr, "127.0.0.1");
    let mut session2 = moblin.next_session(FIRST_SESSION).await;
    assert_eq!(session2.relay_id, id, "restarted relay changed identity");
    assert_ne!(session2.challenge, session.challenge);
    assert_ne!(session2.authentication, session.authentication);
    let port2 = session2.start_tunnel(receiver.addr).await;
    assert!(udp.round_trip(port2, b"after restart").await);
    assert!(session2.status().await["response"]["result"]["ok"].is_object());
    assert!(relay.is_running());
}

async fn reconnect_after_streamer_drop(graceful: bool) {
    let (mut moblin, mut relay, session, receiver, udp, port) =
        connected_relay(if graceful { "close" } else { "drop" }).await;
    let id = session.relay_id;
    let challenge = session.challenge.clone();

    let dropped_at = Instant::now();
    if graceful {
        let mut session = session;
        session.close().await;
        session.wait_closed(Duration::from_secs(2)).await;
    } else {
        // No close frame: the TCP connection just ends.
        drop(session);
    }
    // Give the relay a moment to notice, then the old tunnel must be gone.
    sleep(Duration::from_millis(200)).await;
    udp.assert_quiet(port, b"old tunnel after drop", &receiver, RETIRED_WINDOW)
        .await;

    let mut session2 = moblin.next_session(Duration::from_secs(10)).await;
    let delay = dropped_at.elapsed();
    assert!(
        delay >= Duration::from_secs(4) && delay < Duration::from_secs(9),
        "reconnected after {delay:?}, expected about 5 s"
    );
    assert_eq!(session2.relay_id, id);
    assert_ne!(session2.challenge, challenge, "challenge was reused");

    let port2 = session2.start_tunnel(receiver.addr).await;
    assert!(udp.round_trip(port2, b"after reconnect").await);
    if port2 != port {
        udp.assert_quiet(port, b"old tunnel later", &receiver, RETIRED_WINDOW)
            .await;
    }
    assert!(
        moblin.try_next_session().is_none(),
        "relay opened a second session"
    );
    assert!(relay.is_running());
}

/// Moblin closes the WebSocket cleanly (app restart, stream settings change).
#[tokio::test]
async fn reconnect_after_streamer_close() {
    reconnect_after_streamer_drop(true).await;
}

/// The control connection ends without a close frame.
#[tokio::test]
async fn reconnect_after_connection_drop() {
    reconnect_after_streamer_drop(false).await;
}

/// The destination port changes: Moblin asks for a new tunnel on the same
/// connection, as it does when the ingest moves or Moblin restarts the stream.
/// Traffic must follow to the new port and the old relay port must go quiet.
#[tokio::test]
async fn destination_port_change_moves_the_tunnel() {
    let (_moblin, mut relay, mut session, first, udp, port1) = connected_relay("port-change").await;
    let second = EchoReceiver::bind("127.0.0.1:0").await;
    assert!(first.has_received(b"first payload"));

    let port2 = session.start_tunnel(second.addr).await;
    assert!(udp.round_trip(port2, b"to the new port").await);
    assert!(second.has_received(b"to the new port"));
    assert!(!first.has_received(b"to the new port"));

    if port2 != port1 {
        udp.assert_quiet(port1, b"late on the old port", &first, RETIRED_WINDOW)
            .await;
        assert!(!second.has_received(b"late on the old port"));
    }

    // Replace it several more times on one connection; only the newest works.
    let mut previous = port2;
    for i in 0..5 {
        let port = session.start_tunnel(first.addr).await;
        let payload = format!("replacement {i}");
        assert!(udp.round_trip(port, payload.as_bytes()).await);
        if port != previous {
            let stale = format!("stale {i}");
            udp.assert_quiet(previous, stale.as_bytes(), &first, RETIRED_WINDOW)
                .await;
        }
        previous = port;
    }
    assert!(relay.is_running());
}

/// The streamer's source port changes mid-tunnel (NAT rebinding, Moblin
/// reopening its socket). Replies must follow the newest source address.
#[tokio::test]
async fn streamer_source_port_change_moves_replies() {
    let (_moblin, mut relay, _session, _receiver, udp, port) = connected_relay("source-port").await;
    let moved = StreamerUdp::bind().await;
    assert_ne!(moved.port(), udp.port());

    // The relay records the sender after forwarding, so the reply to the very
    // first datagram from a new port can still go to the old one. Moblin's
    // stream recovers from one lost reply; prime once, then require delivery.
    moved.round_trip(port, b"prime").await;
    assert!(moved.round_trip(port, b"from the new source port").await);
    assert!(udp.round_trip(port, b"back on the old port").await);
    assert!(relay.is_running());
}

/// Moblin stops answering mid-stream but its TCP connection stays open (the
/// app froze, the phone left Wi-Fi without a FIN, the path silently drops).
/// With no more datagrams through the tunnel the destination goes quiet too,
/// so the relay's 30 s tunnel timeout fires before the 45 s heartbeat does.
/// The relay must drop the tunnel and rejoin once Moblin answers again.
#[tokio::test]
async fn silent_streamer_mid_stream_is_detected_and_rejoined() {
    let (mut moblin, mut relay, session, receiver, udp, port) =
        connected_relay("silent-streamer").await;
    let id = session.relay_id;

    session.go_silent().await;
    let silent_at = Instant::now();
    let mut session2 = moblin.next_session(Duration::from_secs(45)).await;
    let detected = silent_at.elapsed();
    assert!(
        detected >= Duration::from_secs(25) && detected < Duration::from_secs(42),
        "rejoined after {detected:?}, expected 30 s tunnel timeout plus 5 s"
    );
    assert_eq!(session2.relay_id, id);
    udp.assert_quiet(port, b"old tunnel after silence", &receiver, RETIRED_WINDOW)
        .await;
    let port2 = session2.start_tunnel(receiver.addr).await;
    assert!(udp.round_trip(port2, b"after silence").await);
    drop(session);
    assert!(relay.is_running());
}

/// The same with no tunnel open (Moblin connected but not streaming): only the
/// WebSocket heartbeat can notice, 45 s after the last frame plus the 5 s
/// retry.
#[tokio::test]
async fn silent_streamer_without_tunnel_is_dropped_by_heartbeat() {
    let mut moblin = FakeMoblin::start().await;
    let id = Uuid::new_v4();
    let mut relay = RelayProcess::spawn("heartbeat", id, moblin.addr, "127.0.0.1");
    let mut session = moblin.next_session(FIRST_SESSION).await;
    assert!(session.status().await["response"]["result"]["ok"].is_object());

    session.go_silent().await;
    let silent_at = Instant::now();
    let mut session2 = moblin.next_session(Duration::from_secs(65)).await;
    let detected = silent_at.elapsed();
    assert!(
        detected >= Duration::from_secs(40) && detected < Duration::from_secs(60),
        "rejoined after {detected:?}, expected 45 s heartbeat timeout plus 5 s"
    );
    assert_eq!(session2.relay_id, id);
    assert!(session2.status().await["response"]["result"]["ok"].is_object());
    drop(session);
    assert!(relay.is_running());
}

/// Moblin goes away completely: the listener closes and connections are
/// refused. The relay must keep retrying without exiting and reconnect when
/// Moblin listens on the same address again.
#[tokio::test]
async fn streamer_disappears_and_returns() {
    let (moblin, mut relay, session, receiver, udp, _port) = connected_relay("streamer-gone").await;
    let id = session.relay_id;
    let addr = moblin.addr;

    moblin.stop_listening();
    drop(session);
    // At least two refused attempts at the 5 s retry interval.
    sleep(Duration::from_secs(12)).await;
    assert!(
        relay.is_running(),
        "relay exited while the streamer was gone"
    );

    let mut moblin = FakeMoblin::bind(addr).await;
    let mut session2 = moblin.next_session(Duration::from_secs(9)).await;
    assert_eq!(session2.relay_id, id);
    let port2 = session2.start_tunnel(receiver.addr).await;
    assert!(udp.round_trip(port2, b"after the streamer returned").await);
}

/// The destination stops answering (the relay's uplink or the ingest went
/// away). The relay restarts its session after 30 s without replies, and a new
/// tunnel works once the destination answers again.
#[tokio::test]
async fn silent_destination_restarts_the_session() {
    let (mut moblin, mut relay, mut session, receiver, udp, _port) =
        connected_relay("silent-destination").await;

    receiver.set_silent(true);
    let silent_at = Instant::now();
    session.wait_closed(Duration::from_secs(40)).await;
    let detected = silent_at.elapsed();
    assert!(
        detected >= Duration::from_secs(25),
        "session dropped after only {detected:?}, expected 30 s"
    );
    let mut session2 = moblin.next_session(Duration::from_secs(9)).await;
    receiver.set_silent(false);
    let port2 = session2.start_tunnel(receiver.addr).await;
    assert!(udp.round_trip(port2, b"destination answers again").await);
    assert!(relay.is_running());
}
