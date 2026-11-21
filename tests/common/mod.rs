//! Shared harness for the failure tests: a fake Moblin streamer, a UDP echo
//! receiver standing in for the SRTLA ingest, and the relay as a child process.
//!
//! The relay under test is `MOBLINK_RELAY_BIN` when set, so CI can run the
//! tests against the release binary it just built. Otherwise the debug binary
//! that Cargo builds for this package is used.

#![allow(dead_code)]

use std::fs::File;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{accept_async, WebSocketStream};
use uuid::Uuid;

pub const PASSWORD: &str = "ci-failure-tests";

pub fn relay_binary() -> PathBuf {
    match std::env::var_os("MOBLINK_RELAY_BIN") {
        Some(path) => PathBuf::from(path),
        None => PathBuf::from(env!("CARGO_BIN_EXE_moblink-relay")),
    }
}

// ---------------------------------------------------------------------------
// Relay process

pub struct RelayProcess {
    child: Child,
    pub log: PathBuf,
}

impl RelayProcess {
    pub fn spawn(name: &str, id: Uuid, streamer: SocketAddr, bind_address: &str) -> Self {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let log = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("relay-{name}-{}-{n}.log", std::process::id()));
        let file = File::create(&log).expect("create relay log");
        let child = Command::new(relay_binary())
            .args(["--name", name])
            .args(["--id", &id.to_string()])
            .args(["--password", PASSWORD])
            .args(["--streamer-url", &format!("ws://{streamer}")])
            .args(["--bind-address", bind_address])
            .args(["--log-level", "debug", "--no-log-timestamps"])
            .stdin(Stdio::null())
            .stdout(file.try_clone().unwrap())
            .stderr(file)
            .spawn()
            .unwrap_or_else(|e| panic!("spawn {}: {e}", relay_binary().display()));
        Self { child, log }
    }

    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    /// SIGKILL and reap, the way a crashed or OOM-killed relay disappears.
    pub fn kill(&mut self) {
        self.child.kill().ok();
        self.child.wait().expect("reap relay");
    }

    pub fn is_running(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }

    pub fn log_contents(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }
}

impl Drop for RelayProcess {
    fn drop(&mut self) {
        if self.is_running() {
            self.kill();
        }
        if std::thread::panicking() {
            eprintln!(
                "--- relay log {} ---\n{}",
                self.log.display(),
                self.log_contents()
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Fake Moblin streamer

/// One authenticated relay connection, as Moblin sees it.
pub struct Session {
    pub relay_id: Uuid,
    pub name: String,
    pub challenge: String,
    pub authentication: String,
    pub peer: SocketAddr,
    commands: mpsc::Sender<SessionCommand>,
    responses: mpsc::Receiver<Value>,
    closed: watch::Receiver<bool>,
    next_id: u32,
}

enum SessionCommand {
    Send(Message),
    /// Stop reading and writing but keep the TCP connection open, like a
    /// frozen app or a path that silently drops everything.
    GoSilent(oneshot::Sender<()>),
}

impl Session {
    /// Sends `{"request": {"id": n, "data": data}}` and waits for the response
    /// carrying the same id.
    pub async fn request(&mut self, data: Value) -> Value {
        self.next_id += 1;
        let id = self.next_id;
        let text = json!({ "request": { "id": id, "data": data } }).to_string();
        self.commands
            .send(SessionCommand::Send(Message::Text(text.into())))
            .await
            .expect("session task alive");
        timeout(Duration::from_secs(5), async {
            loop {
                let value = self
                    .responses
                    .recv()
                    .await
                    .expect("relay closed the session");
                if value["response"]["id"] == id {
                    return value;
                }
            }
        })
        .await
        .expect("relay response")
    }

    /// Asks the relay for a tunnel to `destination` and returns the UDP port
    /// it listens on for the streamer side.
    pub async fn start_tunnel(&mut self, destination: SocketAddr) -> u16 {
        let response = self
            .request(json!({
                "startTunnel": {
                    "address": destination.ip().to_string(),
                    "port": destination.port(),
                }
            }))
            .await;
        assert!(
            response["response"]["result"].get("ok").is_some(),
            "{response}"
        );
        response["response"]["data"]["startTunnel"]["port"]
            .as_u64()
            .and_then(|port| u16::try_from(port).ok())
            .unwrap_or_else(|| panic!("no tunnel port in {response}"))
    }

    pub async fn status(&mut self) -> Value {
        self.request(json!({ "status": {} })).await
    }

    pub async fn close(&self) {
        self.commands
            .send(SessionCommand::Send(Message::Close(None)))
            .await
            .ok();
    }

    pub async fn go_silent(&self) {
        let (tx, rx) = oneshot::channel();
        self.commands.send(SessionCommand::GoSilent(tx)).await.ok();
        rx.await.ok();
    }

    pub fn is_closed(&self) -> bool {
        *self.closed.borrow()
    }

    pub async fn wait_closed(&mut self, limit: Duration) {
        timeout(limit, self.closed.wait_for(|closed| *closed))
            .await
            .unwrap_or_else(|_| panic!("relay kept the session open for {limit:?}"))
            .ok();
    }
}

pub struct FakeMoblin {
    pub addr: SocketAddr,
    sessions: mpsc::Receiver<Session>,
    rejected: watch::Receiver<u32>,
    accept_task: tokio::task::JoinHandle<()>,
}

impl FakeMoblin {
    pub async fn start() -> Self {
        Self::bind("127.0.0.1:0".parse().unwrap()).await
    }

    pub async fn bind(addr: SocketAddr) -> Self {
        let listener = TcpListener::bind(addr).await.expect("bind fake streamer");
        let addr = listener.local_addr().unwrap();
        let (session_tx, sessions) = mpsc::channel(16);
        let (rejected_tx, rejected) = watch::channel(0);
        let accept_task = tokio::spawn(async move {
            let mut connection = 0u32;
            loop {
                let Ok((stream, peer)) = listener.accept().await else {
                    return;
                };
                connection += 1;
                let session_tx = session_tx.clone();
                let rejected_tx = rejected_tx.clone();
                tokio::spawn(async move {
                    match handshake(stream, peer, connection).await {
                        Ok(Some(session)) => {
                            session_tx.send(session).await.ok();
                        }
                        Ok(None) => {
                            rejected_tx.send_modify(|n| *n += 1);
                        }
                        Err(error) => eprintln!("handshake with {peer} failed: {error}"),
                    }
                });
            }
        });
        Self {
            addr,
            sessions,
            rejected,
            accept_task,
        }
    }

    pub async fn next_session(&mut self, limit: Duration) -> Session {
        timeout(limit, self.sessions.recv())
            .await
            .unwrap_or_else(|_| panic!("no relay session within {limit:?}"))
            .expect("accept loop alive")
    }

    pub async fn no_session_within(&mut self, limit: Duration) {
        if let Ok(Some(session)) = timeout(limit, self.sessions.recv()).await {
            panic!("unexpected relay session from {}", session.peer);
        }
    }

    pub fn try_next_session(&mut self) -> Option<Session> {
        self.sessions.try_recv().ok()
    }

    pub fn rejected(&self) -> u32 {
        *self.rejected.borrow()
    }

    /// Closes the listening socket, as when Moblin quits. Sessions already
    /// handed out stay open until they are dropped.
    pub fn stop_listening(self) {
        self.accept_task.abort();
    }
}

/// Moblin's scheme, written out here rather than imported so the test checks
/// the wire format: base64(sha256(base64(sha256(password + salt)) +
/// challenge)).
fn expected_authentication(password: &str, salt: &str, challenge: &str) -> String {
    let inner = BASE64.encode(Sha256::digest(format!("{password}{salt}")));
    BASE64.encode(Sha256::digest(format!("{inner}{challenge}")))
}

async fn handshake(
    stream: TcpStream,
    peer: SocketAddr,
    connection: u32,
) -> Result<Option<Session>, Box<dyn std::error::Error + Send + Sync>> {
    let mut ws = accept_async(stream).await?;
    // Fresh per connection, so a replayed identify cannot pass.
    let salt = format!("salt-{connection}-{}", Uuid::new_v4());
    let challenge = format!("challenge-{connection}-{}", Uuid::new_v4());
    let hello = json!({
        "hello": {
            "apiVersion": "1.0",
            "authentication": { "challenge": challenge, "salt": salt },
        }
    });
    ws.send(Message::Text(hello.to_string().into())).await?;
    let identify = timeout(Duration::from_secs(5), async {
        while let Some(message) = ws.next().await {
            if let Message::Text(text) = message? {
                let value: Value = serde_json::from_str(&text)?;
                if let Some(identify) = value.get("identify") {
                    return Ok(identify.clone());
                }
            }
        }
        Err::<_, Box<dyn std::error::Error + Send + Sync>>("closed before identify".into())
    })
    .await??;
    let authentication = identify["authentication"].as_str().unwrap_or_default();
    let accepted = authentication == expected_authentication(PASSWORD, &salt, &challenge);
    let result = if accepted {
        json!({ "ok": {} })
    } else {
        json!({ "wrongPassword": {} })
    };
    let identified = json!({ "identified": { "result": result } });
    ws.send(Message::Text(identified.to_string().into()))
        .await?;
    if !accepted {
        // Moblin closes the connection after a wrong password.
        ws.close(None).await.ok();
        return Ok(None);
    }
    let (commands, command_rx) = mpsc::channel(16);
    let (response_tx, responses) = mpsc::channel(64);
    let (closed_tx, closed) = watch::channel(false);
    tokio::spawn(session_task(ws, command_rx, response_tx, closed_tx));
    Ok(Some(Session {
        relay_id: identify["id"]
            .as_str()
            .and_then(|id| id.parse().ok())
            .ok_or("identify without a valid id")?,
        name: identify["name"].as_str().unwrap_or_default().to_string(),
        authentication: authentication.to_string(),
        challenge,
        peer,
        commands,
        responses,
        closed,
        next_id: 0,
    }))
}

async fn session_task(
    mut ws: WebSocketStream<TcpStream>,
    mut commands: mpsc::Receiver<SessionCommand>,
    responses: mpsc::Sender<Value>,
    closed: watch::Sender<bool>,
) {
    loop {
        tokio::select! {
            message = ws.next() => match message {
                Some(Ok(Message::Text(text))) => {
                    if let Ok(value) = serde_json::from_str::<Value>(&text) {
                        responses.send(value).await.ok();
                    }
                }
                // tungstenite queues the pong for pings by itself.
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                Some(Ok(_)) => {}
            },
            command = commands.recv() => match command {
                Some(SessionCommand::Send(message)) => {
                    let close = matches!(message, Message::Close(_));
                    if ws.send(message).await.is_err() || close {
                        break;
                    }
                }
                Some(SessionCommand::GoSilent(done)) => {
                    done.send(()).ok();
                    // Hold the socket without polling it until the session is
                    // dropped (the command channel closes).
                    while commands.recv().await.is_some() {}
                    // A silent peer that comes back never sends a close frame.
                    closed.send_replace(true);
                    return;
                }
                // Session dropped: the TCP connection goes with the stream.
                None => break,
            },
        }
    }
    closed.send_replace(true);
}

// ---------------------------------------------------------------------------
// UDP endpoints

/// Echoes every datagram back to its sender and counts what it received.
pub struct EchoReceiver {
    pub addr: SocketAddr,
    received: watch::Receiver<Vec<Vec<u8>>>,
    silent: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl EchoReceiver {
    pub async fn bind(addr: &str) -> Self {
        let socket = UdpSocket::bind(addr).await.expect("bind receiver");
        let addr = socket.local_addr().unwrap();
        let (received_tx, received) = watch::channel(Vec::new());
        let (silent, silent_rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            loop {
                let Ok((size, from)) = socket.recv_from(&mut buf).await else {
                    // ICMP errors from earlier replies surface here; keep going.
                    continue;
                };
                received_tx.send_modify(|all| all.push(buf[..size].to_vec()));
                if !*silent_rx.borrow() {
                    socket.send_to(&buf[..size], from).await.ok();
                }
            }
        });
        Self {
            addr,
            received,
            silent,
            task,
        }
    }

    pub fn count(&self) -> usize {
        self.received.borrow().len()
    }

    pub fn has_received(&self, payload: &[u8]) -> bool {
        self.received.borrow().iter().any(|p| p == payload)
    }

    /// Keeps receiving but stops answering, like an ingest that went away.
    pub fn set_silent(&self, silent: bool) {
        self.silent.send_replace(silent);
    }
}

impl Drop for EchoReceiver {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// The streamer's UDP socket, the side of the tunnel Moblin sends from.
pub struct StreamerUdp {
    pub socket: UdpSocket,
}

impl StreamerUdp {
    pub async fn bind() -> Self {
        Self {
            socket: UdpSocket::bind("127.0.0.1:0").await.unwrap(),
        }
    }

    pub fn port(&self) -> u16 {
        self.socket.local_addr().unwrap().port()
    }

    /// Sends `payload` to the relay and waits for the same bytes to come back
    /// through the receiver's echo.
    pub async fn round_trip(&self, relay_port: u16, payload: &[u8]) -> bool {
        self.drain();
        if self
            .socket
            .send_to(payload, ("127.0.0.1", relay_port))
            .await
            .is_err()
        {
            return false;
        }
        let mut buf = [0u8; 2048];
        timeout(Duration::from_secs(2), async {
            loop {
                match self.socket.recv_from(&mut buf).await {
                    Ok((size, _)) if &buf[..size] == payload => return true,
                    Ok(_) => continue,
                    // ECONNREFUSED from an earlier send to a closed port.
                    Err(_) => continue,
                }
            }
        })
        .await
        .unwrap_or(false)
    }

    /// Sends `payload` to a port that should no longer forward and checks that
    /// it neither reaches `receiver` nor comes back within `window`.
    pub async fn assert_quiet(
        &self,
        relay_port: u16,
        payload: &[u8],
        receiver: &EchoReceiver,
        window: Duration,
    ) {
        self.drain();
        self.socket
            .send_to(payload, ("127.0.0.1", relay_port))
            .await
            .ok();
        let mut buf = [0u8; 2048];
        let echoed = timeout(window, async {
            loop {
                match self.socket.recv_from(&mut buf).await {
                    Ok((size, _)) if &buf[..size] == payload => return,
                    _ => continue,
                }
            }
        })
        .await
        .is_ok();
        assert!(!echoed, "retired port {relay_port} still answered");
        assert!(
            !receiver.has_received(payload),
            "retired port {relay_port} still forwarded to the receiver"
        );
    }

    fn drain(&self) {
        let mut buf = [0u8; 2048];
        while self.socket.try_recv_from(&mut buf).is_ok() {}
    }
}
