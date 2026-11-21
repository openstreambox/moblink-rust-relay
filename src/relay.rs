use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, Weak};

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use log::{debug, error, info};
use serde::Deserialize;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpStream, UdpSocket};
use tokio::process::Command;
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep, timeout};
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use uuid::Uuid;

use crate::protocol::*;
use crate::utils::{AnyError, resolve_host};

#[derive(Default, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Status {
    pub battery_percentage: Option<i32>,
}

pub type GetStatusClosure =
    Box<dyn Fn() -> Pin<Box<dyn Future<Output = Status> + Send + Sync>> + Send + Sync>;

struct RelayInner {
    me: Weak<Mutex<Self>>,
    /// Store a local IP address  for binding UDP sockets
    bind_address: String,
    relay_id: Uuid,
    streamer_url: String,
    password: String,
    name: String,
    on_status_updated: Option<Box<dyn Fn(String) + Send + Sync>>,
    get_status: Option<Arc<GetStatusClosure>>,
    ws_writer: Option<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>,
    ws_reader: Option<JoinHandle<()>>,
    started: bool,
    connected: bool,
    wrong_password: bool,
    start_on_reconnect_soon: Arc<Mutex<bool>>,
    relay_to_destination: Option<JoinHandle<Result<(), AnyError>>>,
    relay_to_streamer: Option<JoinHandle<()>>,
}

impl RelayInner {
    fn new() -> Arc<Mutex<Self>> {
        Arc::new_cyclic(|me| {
            Mutex::new(Self {
                me: me.clone(),
                bind_address: Self::get_default_bind_address(),
                relay_id: Uuid::new_v4(),
                streamer_url: "".to_string(),
                password: "".to_string(),
                name: "".to_string(),
                on_status_updated: None,
                get_status: None,
                ws_writer: None,
                ws_reader: None,
                started: false,
                connected: false,
                wrong_password: false,
                start_on_reconnect_soon: Arc::new(Mutex::new(false)),
                relay_to_destination: None,
                relay_to_streamer: None,
            })
        })
    }

    fn set_bind_address(&mut self, address: String) {
        self.bind_address = address;
    }

    async fn setup<F>(
        &mut self,
        streamer_url: String,
        password: String,
        relay_id: Uuid,
        name: String,
        on_status_updated: F,
        get_status: Option<GetStatusClosure>,
    ) where
        F: Fn(String) + Send + Sync + 'static,
    {
        self.on_status_updated = Some(Box::new(on_status_updated));
        self.get_status = get_status.map(Arc::new);
        self.relay_id = relay_id;
        self.streamer_url = streamer_url;
        self.password = password;
        self.name = name;
    }

    fn is_started(&self) -> bool {
        self.started
    }

    async fn start(&mut self) {
        if !self.started {
            self.started = true;
            self.start_internal().await;
        }
    }

    async fn stop(&mut self) {
        if self.started {
            self.started = false;
            self.stop_internal().await;
        }
    }

    fn get_default_bind_address() -> String {
        // Get main network interface
        let interfaces = pnet::datalink::interfaces();
        let interface = interfaces.iter().find(|interface| {
            interface.is_up() && !interface.is_loopback() && !interface.ips.is_empty()
        });

        // Only ipv4 addresses are supported
        let ipv4_addresses: Vec<String> = interface
            .expect("No available network interfaces found")
            .ips
            .iter()
            .filter_map(|ip| {
                let ip = ip.ip();
                ip.is_ipv4().then(|| ip.to_string())
            })
            .collect();

        // Return the first address
        ipv4_addresses
            .first()
            .cloned()
            .unwrap_or("0.0.0.0:0".to_string())
    }

    async fn start_internal(&mut self) {
        if !self.started {
            self.stop_internal().await;
            return;
        }

        let request = match url::Url::parse(&self.streamer_url) {
            Ok(url) => url,
            Err(e) => {
                error!("Failed to parse URL: {}", e);
                return;
            }
        };

        match timeout(Duration::from_secs(10), connect_async(request.to_string())).await {
            Ok(Ok((ws_stream, _))) => {
                debug!("Connected to {}", self.streamer_url);
                let (writer, reader) = ws_stream.split();
                self.ws_writer = Some(writer);
                self.start_websocket_receiver(reader);
            }
            Ok(Err(error)) => {
                debug!(
                    "Failed to connect to {} with error: {}",
                    self.streamer_url, error
                );
                self.reconnect_soon().await;
            }
            Err(_elapsed) => {
                debug!(
                    "Failed to connect to {} within 10 seconds",
                    self.streamer_url
                );
                self.reconnect_soon().await;
            }
        }
    }

    fn start_websocket_receiver(
        &mut self,
        mut reader: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    ) {
        // Task to process messages received from the channel.
        let relay = self.me.clone();

        self.ws_reader = Some(tokio::spawn(async move {
            while let Some(result) = reader.next().await {
                let Some(relay_arc) = relay.upgrade() else {
                    return;
                };
                let mut relay = relay_arc.lock().await;
                let reconnect = match result {
                    Ok(Message::Text(text)) => {
                        match serde_json::from_str::<MessageToRelay>(&text) {
                            Ok(message) => {
                                match timeout(Duration::from_secs(5), relay.handle_message(message))
                                    .await
                                {
                                    Ok(Ok(())) => false,
                                    Ok(Err(error)) => {
                                        error!("Message handling failed with error: {}", error);
                                        true
                                    }
                                    Err(_) => {
                                        debug!("WebSocket message handling timed out");
                                        true
                                    }
                                }
                            }
                            Err(_) => {
                                error!("Failed to deserialize message: {}", text);
                                false
                            }
                        }
                    }
                    Ok(Message::Ping(data)) => {
                        relay.send_message(Message::Pong(data)).await.ok();
                        false
                    }
                    Ok(Message::Close(frame)) => {
                        info!("Received close message: {:?}", frame);
                        true
                    }
                    Ok(_) => false,
                    Err(error) => {
                        debug!("Error processing message: {}", error);
                        if !error
                            .to_string()
                            .contains("Connection reset without closing handshake")
                        {
                            break;
                        }
                        true
                    }
                };
                if reconnect {
                    // Detach this task so stop can join the other tasks without
                    // waiting on the reader that is performing the reconnect.
                    relay.ws_reader.take();
                    drop(reader);
                    relay.reconnect_soon().await;
                    break;
                }
            }
        }));
    }

    async fn stop_internal(&mut self) {
        *self.start_on_reconnect_soon.lock().await = false;
        if let Some(reader) = self.ws_reader.take() {
            reader.abort();
            reader.await.ok();
        }
        self.stop_tunnel().await;
        if let Some(mut ws_writer) = self.ws_writer.take() {
            match timeout(Duration::from_secs(2), ws_writer.close()).await {
                Ok(Ok(())) => debug!("WebSocket closed successfully"),
                Ok(Err(error)) => debug!("Error closing WebSocket: {}", error),
                Err(_) => debug!("WebSocket close timed out"),
            }
        }
        self.connected = false;
        self.wrong_password = false;
        self.update_status();
    }

    async fn stop_tunnel(&mut self) {
        let outbound = self.relay_to_destination.take();
        let inbound = self.relay_to_streamer.take();
        if let Some(task) = &outbound {
            task.abort();
        }
        if let Some(task) = &inbound {
            task.abort();
        }
        if let Some(task) = outbound {
            task.await.ok();
        }
        if let Some(task) = inbound {
            task.await.ok();
        }
    }

    fn update_status(&self) {
        let Some(on_status_updated) = &self.on_status_updated else {
            return;
        };
        let status = if self.connected {
            "Connected to streamer"
        } else if self.wrong_password {
            "Wrong password"
        } else if self.started {
            "Connecting to streamer"
        } else {
            "Disconnected from streamer"
        };
        on_status_updated(status.to_string());
    }

    async fn reconnect_soon(&mut self) {
        self.stop_internal().await;
        let start_on_reconnect_soon = Arc::new(Mutex::new(true));
        self.start_on_reconnect_soon = start_on_reconnect_soon.clone();
        self.start_soon(start_on_reconnect_soon);
    }

    fn start_soon(&mut self, start_on_reconnect_soon: Arc<Mutex<bool>>) {
        let relay = self.me.clone();

        tokio::spawn(async move {
            sleep(Duration::from_secs(5)).await;

            if let Some(relay) = relay.upgrade() {
                let mut relay = relay.lock().await;
                let should_start = *start_on_reconnect_soon.lock().await;
                if should_start {
                    debug!("Reconnecting...");
                    relay.start_internal().await;
                }
            }
        });
    }

    async fn handle_message(&mut self, message: MessageToRelay) -> Result<(), AnyError> {
        match message {
            MessageToRelay::Hello(hello) => self.handle_message_hello(hello).await,
            MessageToRelay::Identified(identified) => {
                self.handle_message_identified(identified).await
            }
            MessageToRelay::Request(request) => self.handle_message_request(request).await,
        }
    }

    async fn handle_message_hello(&mut self, hello: Hello) -> Result<(), AnyError> {
        let authentication = calculate_authentication(
            &self.password,
            &hello.authentication.salt,
            &hello.authentication.challenge,
        );
        let identify = Identify {
            id: self.relay_id,
            name: self.name.clone(),
            authentication,
        };
        self.send(MessageToStreamer::Identify(identify)).await
    }

    async fn handle_message_identified(&mut self, identified: Identified) -> Result<(), AnyError> {
        match identified.result {
            MoblinkResult::Ok(_) => {
                self.connected = true;
            }
            MoblinkResult::WrongPassword(_) => {
                self.wrong_password = true;
            }
        }
        self.update_status();
        Ok(())
    }

    async fn handle_message_request(&mut self, request: MessageRequest) -> Result<(), AnyError> {
        match &request.data {
            MessageRequestData::StartTunnel(start_tunnel) => {
                self.handle_message_request_start_tunnel(&request, start_tunnel)
                    .await
            }
            MessageRequestData::Status(_) => self.handle_message_request_status(request).await,
        }
    }

    async fn handle_message_request_start_tunnel(
        &mut self,
        request: &MessageRequest,
        start_tunnel: &StartTunnelRequest,
    ) -> Result<(), AnyError> {
        self.stop_tunnel().await;

        // Pick bind addresses from the relay
        let local_bind_addr_for_streamer = parse_socket_addr("0.0.0.0")?;
        let local_bind_addr_for_destination = parse_socket_addr(&self.bind_address)?;

        debug!(
            "Binding streamer socket on: {}, destination socket on: {}",
            local_bind_addr_for_streamer, local_bind_addr_for_destination
        );
        // Create a UDP socket bound for receiving packets from the server.
        // Use dual-stack socket creation.
        let streamer_socket = create_dual_stack_udp_socket(local_bind_addr_for_streamer).await?;
        let streamer_port = streamer_socket.local_addr()?.port();
        let streamer_socket = Arc::new(streamer_socket);

        // Inform the server about the chosen port.
        let data = ResponseData::StartTunnel(StartTunnelResponseData {
            port: streamer_port,
        });
        let response = request.to_ok_response(data);
        self.send(MessageToStreamer::Response(response)).await?;

        // Create a new UDP socket for communication with the destination.
        // Use dual-stack socket creation.
        let destination_socket =
            create_dual_stack_udp_socket(local_bind_addr_for_destination).await?;

        let destination_socket = Arc::new(destination_socket);
        let destination_address = resolve_host(&start_tunnel.address).await?;
        let destination_address = match IpAddr::from_str(&destination_address)? {
            IpAddr::V4(v4) => IpAddr::V4(v4),
            IpAddr::V6(v6) => {
                // If it’s an IPv4-mapped IPv6 like ::ffff:x.x.x.x, convert to real IPv4
                if let Some(mapped_v4) = v6.to_ipv4() {
                    IpAddr::V4(mapped_v4)
                } else {
                    // Otherwise, keep it as IPv6
                    IpAddr::V6(v6)
                }
            }
        };

        let destination_address = SocketAddr::new(destination_address, start_tunnel.port);
        info!("Destination address: {}", destination_address);

        self.start_tunnel(streamer_socket, destination_socket, destination_address);
        Ok(())
    }

    fn start_tunnel(
        &mut self,
        streamer_socket: Arc<UdpSocket>,
        destination_socket: Arc<UdpSocket>,
        destination_addr: SocketAddr,
    ) {
        let streamer_address = Arc::new(Mutex::new(None));
        let first_packet = Arc::new(Notify::new());

        let relay = self.me.clone();
        let input = streamer_socket.clone();
        let output = destination_socket.clone();
        let address = streamer_address.clone();
        let ready = first_packet.clone();
        self.relay_to_streamer = Some(tokio::spawn(async move {
            // Start the return-path timeout only once the streamer is sending.
            ready.notified().await;
            loop {
                if let Err(error) =
                    relay_one_packet_from_destination_to_streamer(&input, &output, &address).await
                {
                    info!("(relay_to_streamer) Failed with error: {}", error);
                    break;
                }
            }

            drop((input, output));
            if let Some(relay) = relay.upgrade() {
                let mut relay = relay.lock().await;
                // This task owns the reconnect. Detach it before stopping
                // the tunnel so the relay does not try to join it here.
                relay.relay_to_streamer.take();
                relay.reconnect_soon().await;
            }
        }));

        self.relay_to_destination = Some(tokio::spawn(async move {
            let mut buf = [0; 2048];
            loop {
                let (size, remote_addr) = streamer_socket.recv_from(&mut buf).await?;
                destination_socket
                    .send_to(&buf[..size], &destination_addr)
                    .await?;
                if streamer_address.lock().await.replace(remote_addr).is_none() {
                    first_packet.notify_one();
                }
            }
        }));
    }

    async fn handle_message_request_status(
        &mut self,
        request: MessageRequest,
    ) -> Result<(), AnyError> {
        let mut battery_percentage = None;
        if let Some(get_status) = self.get_status.as_ref() {
            battery_percentage = get_status().await.battery_percentage;
        }
        let data = ResponseData::Status(StatusResponseData { battery_percentage });
        let response = request.to_ok_response(data);
        self.send(MessageToStreamer::Response(response)).await
    }

    async fn send(&mut self, message: MessageToStreamer) -> Result<(), AnyError> {
        let text = serde_json::to_string(&message)?;
        self.send_message(Message::Text(text.into())).await
    }

    async fn send_message(&mut self, message: Message) -> Result<(), AnyError> {
        let Some(writer) = self.ws_writer.as_mut() else {
            return Err("No websocket writer".into());
        };
        timeout(Duration::from_secs(5), writer.send(message)).await??;
        Ok(())
    }
}

pub struct Relay {
    inner: Arc<Mutex<RelayInner>>,
}

impl Default for Relay {
    fn default() -> Self {
        Self::new()
    }
}

impl Relay {
    pub fn new() -> Self {
        Self {
            inner: RelayInner::new(),
        }
    }

    pub async fn set_bind_address(&self, address: String) {
        self.inner.lock().await.set_bind_address(address);
    }

    pub async fn setup<F>(
        &self,
        streamer_url: String,
        password: String,
        relay_id: Uuid,
        name: String,
        on_status_updated: F,
        get_status: Option<GetStatusClosure>,
    ) where
        F: Fn(String) + Send + Sync + 'static,
    {
        self.inner
            .lock()
            .await
            .setup(
                streamer_url,
                password,
                relay_id,
                name,
                on_status_updated,
                get_status,
            )
            .await;
    }

    pub async fn is_started(&self) -> bool {
        self.inner.lock().await.is_started()
    }

    pub async fn start(&self) {
        self.inner.lock().await.start().await;
    }

    pub async fn stop(&self) {
        self.inner.lock().await.stop().await;
    }
}

async fn relay_one_packet_from_destination_to_streamer(
    streamer_socket: &Arc<UdpSocket>,
    destination_socket: &Arc<UdpSocket>,
    streamer_address: &Arc<Mutex<Option<SocketAddr>>>,
) -> Result<(), AnyError> {
    let mut buf = [0; 2048];
    let size = timeout(Duration::from_secs(30), destination_socket.recv(&mut buf)).await??;
    let streamer_addr = streamer_address
        .lock()
        .await
        .ok_or("Failed to get address lock")?;
    streamer_socket
        .send_to(&buf[..size], &streamer_addr)
        .await?;
    Ok(())
}

async fn create_dual_stack_udp_socket(
    addr: SocketAddr,
) -> Result<tokio::net::UdpSocket, std::io::Error> {
    let socket = match addr.is_ipv4() {
        true => {
            // Create an IPv4 socket
            tokio::net::UdpSocket::bind(addr).await?
        }
        false => {
            // Create a dual-stack socket (supporting both IPv4 and IPv6)
            let socket = socket2::Socket::new(
                socket2::Domain::IPV6,
                socket2::Type::DGRAM,
                Some(socket2::Protocol::UDP),
            )?;

            // Set IPV6_V6ONLY to false to enable dual-stack support
            socket.set_only_v6(false)?;

            // Bind the socket
            socket.bind(&socket2::SockAddr::from(addr))?;

            // Convert to a tokio UdpSocket
            tokio::net::UdpSocket::from_std(socket.into())?
        }
    };

    Ok(socket)
}

// Helper function to parse a string into a SocketAddr, handling IP addresses
// without ports.
fn parse_socket_addr(addr_str: &str) -> Result<SocketAddr, std::io::Error> {
    // Attempt to parse the string as a full SocketAddr (IP:port)
    if let Ok(socket_addr) = SocketAddr::from_str(addr_str) {
        return Ok(socket_addr);
    }

    // If parsing as SocketAddr fails, try parsing as IP address and append default
    // port
    if let Ok(ip_addr) = IpAddr::from_str(addr_str) {
        // Use 0 as the default port, allowing the OS to assign an available port
        return Ok(SocketAddr::new(ip_addr, 0));
    }

    // Return an error if both attempts fail
    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        "Invalid socket address syntax. Expected 'IP:port' or 'IP'.",
    ))
}

pub fn create_get_status_closure(
    status_executable: &Option<String>,
    status_file: &Option<String>,
) -> Option<GetStatusClosure> {
    let status_executable = status_executable.clone();
    let status_file = status_file.clone();
    Some(Box::new(move || {
        let status_executable = status_executable.clone();
        let status_file = status_file.clone();
        Box::pin(async move {
            let output = if let Some(status_executable) = &status_executable {
                let mut command = Command::new(status_executable);
                command.kill_on_drop(true);
                // Leave time for the status reply within the frame budget.
                let Ok(Ok(output)) = timeout(Duration::from_secs(3), command.output()).await else {
                    return Default::default();
                };
                output.stdout
            } else if let Some(status_file) = &status_file {
                let Ok(mut file) = File::open(status_file).await else {
                    return Default::default();
                };
                let mut contents = vec![];
                if file.read_to_end(&mut contents).await.is_err() {
                    return Default::default();
                }
                contents
            } else {
                return Default::default();
            };
            let output = String::from_utf8(output).unwrap_or_default();
            match serde_json::from_str(&output) {
                Ok(status) => status,
                Err(e) => {
                    error!("Failed to decode status with error: {e}");
                    Default::default()
                }
            }
        })
    }))
}
