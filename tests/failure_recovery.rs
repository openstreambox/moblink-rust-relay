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
/// Longer than the relay's 5 s retry interval.
const RETRY_WINDOW: Duration = Duration::from_secs(7);

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

/// Keeps traffic flowing through the tunnel for `duration`, one round trip a
/// second, so a timer counted from tunnel start would show up as an early
/// drop in the tests below.
async fn stream_for(udp: &StreamerUdp, port: u16, duration: Duration) {
    let until = Instant::now() + duration;
    let mut n = 0;
    while Instant::now() < until {
        n += 1;
        let payload = format!("stream {n}");
        assert!(udp.round_trip(port, payload.as_bytes()).await);
        sleep(Duration::from_secs(1)).await;
    }
}

fn assert_between(what: &str, value: Duration, low: u64, high: u64) {
    assert!(
        value >= Duration::from_secs(low) && value < Duration::from_secs(high),
        "{what} after {value:?}, expected {low} to {high} s"
    );
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
    let ended = session.wait_ended(Duration::from_secs(2)).await;
    assert_ne!(ended.how, EndedBy::Dropped);
    udp.assert_quiet(port, b"sent after relay loss", &[&receiver], RETIRED_WINDOW)
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
    let (mut moblin, mut relay, mut session, receiver, udp, port) =
        connected_relay(if graceful { "close" } else { "drop" }).await;
    let id = session.relay_id;
    let challenge = session.challenge.clone();
    let accepted = moblin.accepted();

    let dropped_at = if graceful {
        // Keep the connection open until the relay answers the close frame.
        session.close().await;
        let ended = session.wait_ended(Duration::from_secs(3)).await;
        assert_eq!(
            ended.how,
            EndedBy::RelayCloseFrame,
            "relay did not answer the close frame"
        );
        ended.at
    } else {
        // No close frame: the TCP connection just ends.
        drop(session);
        Instant::now()
    };
    // Give the relay a moment to notice, then the old tunnel must be gone.
    sleep(Duration::from_millis(200)).await;
    udp.assert_quiet(port, b"old tunnel after drop", &[&receiver], RETIRED_WINDOW)
        .await;

    let mut session2 = moblin.next_session(Duration::from_secs(10)).await;
    assert_between("reconnected", dropped_at.elapsed(), 4, 9);
    assert_eq!(session2.relay_id, id);
    assert_ne!(session2.challenge, challenge, "challenge was reused");

    let port2 = session2.start_tunnel(receiver.addr).await;
    assert!(udp.round_trip(port2, b"after reconnect").await);
    if port2 != port {
        udp.assert_quiet(port, b"old tunnel later", &[&receiver], RETIRED_WINDOW)
            .await;
    }
    assert_eq!(moblin.accepted(), accepted + 1, "more than one reconnect");
    moblin.assert_no_more_connections(RETRY_WINDOW).await;
    assert!(!session2.is_closed());
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
        udp.assert_quiet(
            port1,
            b"late on the old port",
            &[&first, &second],
            RETIRED_WINDOW,
        )
        .await;
    }

    // Replace it several more times on one connection, alternating between
    // the receivers; only the newest tunnel works.
    let mut previous = port2;
    for i in 0..6 {
        let target = if i % 2 == 0 { &first } else { &second };
        let port = session.start_tunnel(target.addr).await;
        let payload = format!("replacement {i}");
        assert!(udp.round_trip(port, payload.as_bytes()).await);
        assert!(target.has_received(payload.as_bytes()));
        if port != previous {
            let stale = format!("stale {i}");
            udp.assert_quiet(
                previous,
                stale.as_bytes(),
                &[&first, &second],
                RETIRED_WINDOW,
            )
            .await;
        }
        previous = port;
    }
    assert!(relay.is_running());
}

/// The streamer's source port changes mid-tunnel (NAT rebinding, Moblin
/// reopening its socket). Replies must follow the newest source address, in
/// both directions of the move.
#[tokio::test]
async fn streamer_source_port_change_moves_replies() {
    let (_moblin, mut relay, _session, _receiver, udp, port) = connected_relay("source-port").await;
    let moved = StreamerUdp::bind().await;
    assert_ne!(moved.port(), udp.port());

    assert!(moved.round_trip_after_move(port, "to the new port").await);
    assert!(moved.round_trip(port, b"settled on the new port").await);
    assert!(
        udp.round_trip_after_move(port, "back on the old port")
            .await
    );
    assert!(udp.round_trip(port, b"settled on the old port").await);
    assert!(relay.is_running());
}

/// Moblin stops answering mid-stream but its TCP connection stays open (the
/// app froze, the phone left Wi-Fi without a FIN, the path silently drops).
/// With no more datagrams through the tunnel the destination goes quiet too,
/// so the relay's 30 s tunnel timeout fires before the 45 s heartbeat does.
/// The relay must hang up, drop the tunnel and rejoin once Moblin answers.
#[tokio::test]
async fn silent_streamer_mid_stream_is_detected_and_rejoined() {
    let (mut moblin, mut relay, mut session, receiver, udp, port) =
        connected_relay("silent-streamer").await;
    let id = session.relay_id;
    stream_for(&udp, port, Duration::from_secs(10)).await;
    // A fresh control exchange right before the silence, so a hang-up by the
    // 45 s heartbeat alone would land after the 25 to 40 s window.
    assert!(session.status().await["response"]["result"]["ok"].is_object());

    session.go_silent().await;
    let silent_at = Instant::now();
    let ended = session.wait_ended(Duration::from_secs(45)).await;
    assert_eq!(ended.how, EndedBy::RelayDisconnect);
    assert_between("relay hung up", ended.at - silent_at, 25, 40);

    let mut session2 = moblin.next_session(Duration::from_secs(9)).await;
    assert_eq!(session2.relay_id, id);
    udp.assert_quiet(
        port,
        b"old tunnel after silence",
        &[&receiver],
        RETIRED_WINDOW,
    )
    .await;
    let port2 = session2.start_tunnel(receiver.addr).await;
    assert!(udp.round_trip(port2, b"after silence").await);
    assert!(relay.is_running());
}

/// The same with no tunnel open (Moblin connected but not streaming): only the
/// WebSocket heartbeat can notice, 45 s after the last frame it received.
#[tokio::test]
async fn silent_streamer_without_tunnel_is_dropped_by_heartbeat() {
    let mut moblin = FakeMoblin::start().await;
    let id = Uuid::new_v4();
    let mut relay = RelayProcess::spawn("heartbeat", id, moblin.addr, "127.0.0.1");
    let mut session = moblin.next_session(FIRST_SESSION).await;
    // Stay responsive (pongs) for a while first, so a timer counted from the
    // connection would end too early. The status request is then the last
    // frame the relay gets from this session.
    sleep(Duration::from_secs(10)).await;
    assert!(session.status().await["response"]["result"]["ok"].is_object());
    let last_frame = Instant::now();
    session.go_silent().await;

    let ended = session.wait_ended(Duration::from_secs(55)).await;
    assert_eq!(ended.how, EndedBy::RelayDisconnect);
    assert_between("relay hung up", ended.at - last_frame, 43, 50);

    let mut session2 = moblin.next_session(Duration::from_secs(9)).await;
    assert_eq!(session2.relay_id, id);
    assert!(session2.status().await["response"]["result"]["ok"].is_object());
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

    moblin.stop_listening().await;
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
/// away) while the streamer keeps sending. The relay restarts its session
/// after 30 s without replies, and a new tunnel works once the destination
/// answers again.
#[tokio::test]
async fn silent_destination_restarts_the_session() {
    let (mut moblin, mut relay, mut session, receiver, udp, port) =
        connected_relay("silent-destination").await;
    stream_for(&udp, port, Duration::from_secs(10)).await;

    receiver.set_silent(true);
    let silent_at = Instant::now();
    let received_before = receiver.count();
    let mut probes = 0;
    let mut checked_control = false;
    while !session.is_closed() && silent_at.elapsed() < Duration::from_secs(45) {
        probes += 1;
        let payload = format!("into the silence {probes}");
        udp.socket
            .send_to(payload.as_bytes(), ("127.0.0.1", port))
            .await
            .expect("send probe");
        if !checked_control && silent_at.elapsed() > Duration::from_secs(20) {
            // The control connection still works before the timeout.
            assert!(session.status().await["response"]["result"]["ok"].is_object());
            checked_control = true;
        }
        sleep(Duration::from_secs(1)).await;
    }
    let ended = session.wait_ended(Duration::from_secs(1)).await;
    assert_eq!(ended.how, EndedBy::RelayCloseFrame);
    assert_between("session dropped", ended.at - silent_at, 25, 40);
    assert!(checked_control);
    // The probes kept reaching the destination: it was silent, not cut off.
    assert!(receiver.count() >= received_before + 20);

    let mut session2 = moblin.next_session(Duration::from_secs(9)).await;
    receiver.set_silent(false);
    let port2 = session2.start_tunnel(receiver.addr).await;
    assert!(udp.round_trip(port2, b"destination answers again").await);
    assert!(relay.is_running());
}
