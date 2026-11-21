use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::{Arc, Weak};
use std::time::Duration;

use log::{error, info};
use mdns_sd::{ServiceDaemon, ServiceEvent};
use network_interface::{NetworkInterface, NetworkInterfaceConfig};
use regex::Regex;
use serde::{Deserialize, Serialize};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::MDNS_SERVICE_TYPE;
use crate::relay::{GetStatusClosure, Relay, Status};
use crate::utils::{any_address_belongs_to_this_machine, get_first_ipv4_address};

#[derive(Serialize, Deserialize, Default)]
struct DatabaseContent {
    relay_ids: HashMap<String, Uuid>,
}

struct Database {
    path: PathBuf,
    content: DatabaseContent,
}

impl Database {
    async fn new(path: PathBuf) -> Self {
        let content = Self::load(&path).await;
        Self { path, content }
    }

    async fn load(path: &PathBuf) -> DatabaseContent {
        let mut content = "".to_string();
        if let Ok(mut file) = File::open(path).await {
            let mut buffer = vec![];
            if file.read_to_end(&mut buffer).await.is_ok() {
                content = String::from_utf8(buffer).unwrap_or_default();
            }
        }
        serde_json::from_str(&content).unwrap_or_default()
    }

    async fn store(&self) {
        let content = serde_json::to_string(&self.content).unwrap_or_default();
        if let Ok(mut file) = File::create(&self.path).await {
            file.write_all(content.as_bytes()).await.ok();
        }
    }

    async fn get_relay_id(&mut self, name: &str) -> Uuid {
        if !self.content.relay_ids.contains_key(name) {
            self.content
                .relay_ids
                .insert(name.to_string(), Uuid::new_v4());
            self.store().await;
        }
        *self.content.relay_ids.get(name).unwrap()
    }
}

struct ServiceRelay {
    interface_name: String,
    interface_address: Ipv4Addr,
    streamer_name: String,
    streamer_url: String,
    relay: Relay,
}

impl ServiceRelay {
    async fn new(
        interface_name: String,
        interface_address: Ipv4Addr,
        streamer_name: String,
        streamer_url: String,
        password: String,
        get_status: Option<GetStatusClosure>,
        database: Arc<Mutex<Database>>,
    ) -> Self {
        let relay = Relay::new();
        relay.set_bind_address(interface_address.to_string()).await;
        relay
            .setup(
                streamer_url.clone(),
                password,
                database.lock().await.get_relay_id(&interface_name).await,
                interface_name.clone(),
                |_| {},
                get_status,
            )
            .await;
        relay.start().await;
        Self {
            interface_name,
            interface_address,
            streamer_name,
            streamer_url,
            relay,
        }
    }
}

struct Streamer {
    name: String,
    url: String,
}

struct NetworkInterfaceFilter {
    patterns_to_allow: Option<Regex>,
    patterns_to_ignore: Option<Regex>,
}

impl NetworkInterfaceFilter {
    fn new(patterns_to_allow: Vec<String>, patterns_to_ignore: Vec<String>) -> Self {
        Self {
            patterns_to_allow: Self::compile(patterns_to_allow),
            patterns_to_ignore: Self::compile(patterns_to_ignore),
        }
    }

    fn filter(&self, interfaces: &mut Vec<NetworkInterface>) {
        if let Some(patterns_to_allow) = &self.patterns_to_allow {
            interfaces.retain(|interface| patterns_to_allow.is_match(&interface.name));
        }
        if let Some(patterns_to_ignore) = &self.patterns_to_ignore {
            interfaces.retain(|interface| !patterns_to_ignore.is_match(&interface.name));
        }
    }

    fn compile(patterns: Vec<String>) -> Option<Regex> {
        if !patterns.is_empty() {
            let pattern = format!("^{}$", patterns.join("|"));
            match Regex::new(&pattern) {
                Ok(regex) => return Some(regex),
                Err(error) => {
                    error!("Failed to compile regex {} with error: {}", pattern, error);
                }
            }
        }
        None
    }
}

struct RelayServiceInner {
    me: Weak<Mutex<Self>>,
    password: String,
    network_interface_filter: NetworkInterfaceFilter,
    get_status: Option<GetStatusClosure>,
    status: Status,
    relays: Vec<ServiceRelay>,
    network_interfaces: Vec<NetworkInterface>,
    discovered_streamers: HashMap<String, Streamer>,
    network_interface_monitor: Option<JoinHandle<()>>,
    streamers_monitor: Option<JoinHandle<()>>,
    get_status_updater: Option<JoinHandle<()>>,
    database: Arc<Mutex<Database>>,
}

impl RelayServiceInner {
    async fn new(
        password: String,
        network_interfaces_to_allow: Vec<String>,
        network_interfaces_to_ignore: Vec<String>,
        get_status: Option<GetStatusClosure>,
        database: PathBuf,
    ) -> Arc<Mutex<Self>> {
        let database = Arc::new(Mutex::new(Database::new(database).await));
        Arc::new_cyclic(|me| {
            Mutex::new(Self {
                me: me.clone(),
                password,
                network_interface_filter: NetworkInterfaceFilter::new(
                    network_interfaces_to_allow,
                    network_interfaces_to_ignore,
                ),
                get_status,
                status: Default::default(),
                relays: Vec::new(),
                network_interfaces: Vec::new(),
                discovered_streamers: HashMap::new(),
                network_interface_monitor: None,
                streamers_monitor: None,
                get_status_updater: None,
                database,
            })
        })
    }

    async fn start(&mut self) {
        self.start_network_interfaces_monitor();
        self.start_streamers_monitor();
        self.start_get_status_updater();
    }

    async fn stop(&mut self) {
        if let Some(network_interface_monitor) = self.network_interface_monitor.take() {
            network_interface_monitor.abort();
            network_interface_monitor.await.ok();
        }
        if let Some(streamers_finder) = self.streamers_monitor.take() {
            streamers_finder.abort();
            streamers_finder.await.ok();
        }
    }

    fn start_network_interfaces_monitor(&mut self) {
        let relay_service = self.me.clone();
        self.network_interface_monitor = Some(tokio::spawn(async move {
            loop {
                let Ok(interfaces) = NetworkInterface::show() else {
                    break;
                };
                let Some(relay_service) = relay_service.upgrade() else {
                    break;
                };
                {
                    let mut relay_service = relay_service.lock().await;
                    relay_service.update_network_interfaces(interfaces);
                    relay_service.updated().await;
                }
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        }));
    }

    fn update_network_interfaces(&mut self, mut interfaces: Vec<NetworkInterface>) {
        self.network_interface_filter.filter(&mut interfaces);
        self.network_interfaces = interfaces;
    }

    fn start_streamers_monitor(&mut self) {
        let relay_service = self.me.clone();
        self.streamers_monitor = Some(tokio::spawn(async move {
            loop {
                let Ok(browser) = ServiceDaemon::new() else {
                    return;
                };
                let Ok(receiver) = browser.browse(MDNS_SERVICE_TYPE) else {
                    return;
                };
                while let Ok(event) = receiver.recv_async().await {
                    let (fullname, streamer) = match event {
                        ServiceEvent::ServiceResolved(info) => {
                            info!(
                                "mDNS-SD: Found streamer {} {:?} {}",
                                info.get_fullname(),
                                info.get_addresses(),
                                info.get_port()
                            );
                            let addresses = info.get_addresses_v4();
                            let streamer = if any_address_belongs_to_this_machine(&addresses) {
                                None
                            } else {
                                info.get_property_val_str("name")
                                    .zip(addresses.iter().min())
                                    .map(|(name, address)| Streamer {
                                        name: name.to_owned(),
                                        url: format!("ws://{}:{}", address, info.get_port()),
                                    })
                            };
                            (info.get_fullname().to_owned(), streamer)
                        }
                        ServiceEvent::ServiceRemoved(_, fullname) => (fullname, None),
                        _ => continue,
                    };
                    let Some(relay_service) = relay_service.upgrade() else {
                        return;
                    };
                    let mut relay_service = relay_service.lock().await;
                    relay_service
                        .update_discovered_streamer(fullname, streamer)
                        .await;
                }
            }
        }));
    }

    fn start_get_status_updater(&mut self) {
        let relay_service = self.me.clone();
        self.get_status_updater = Some(tokio::spawn(async move {
            loop {
                let Some(relay_service) = relay_service.upgrade() else {
                    break;
                };
                relay_service.lock().await.update_status().await;
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }));
    }

    async fn update_status(&mut self) {
        self.status = if let Some(get_status) = &self.get_status {
            get_status().await
        } else {
            Status::default()
        }
    }

    async fn update_discovered_streamer(&mut self, fullname: String, streamer: Option<Streamer>) {
        let fullname = fullname.to_ascii_lowercase();
        if let Some(streamer) = streamer {
            self.discovered_streamers.insert(fullname, streamer);
        } else if self.discovered_streamers.remove(&fullname).is_none() {
            return;
        }
        self.updated().await;
    }

    async fn updated(&mut self) {
        let old_number_of_relays = self.relays.len();
        self.remove_relays().await;
        self.add_relays().await;
        let new_number_of_relays = self.relays.len();
        if new_number_of_relays != old_number_of_relays {
            info!("Number of relays: {}", new_number_of_relays);
        }
    }

    async fn add_relays(&mut self) {
        for interface in &self.network_interfaces {
            let Some(interface_address) = get_first_ipv4_address(interface) else {
                continue;
            };
            if interface_address.is_loopback() {
                continue;
            }
            for streamer in self.discovered_streamers.values() {
                if self.relay_already_added(interface_address, &streamer.url) {
                    continue;
                }
                info!(
                    "Adding relay called {} with interface address {} for streamer name {} and \
                     URL {}",
                    interface.name, interface_address, streamer.name, streamer.url
                );
                self.relays.push(
                    ServiceRelay::new(
                        interface.name.clone(),
                        interface_address,
                        streamer.name.clone(),
                        streamer.url.clone(),
                        self.password.clone(),
                        self.create_get_status_closure(),
                        self.database.clone(),
                    )
                    .await,
                );
            }
        }
    }

    pub fn create_get_status_closure(&self) -> Option<GetStatusClosure> {
        let relay_service = self.me.clone();
        Some(Box::new(move || {
            let relay_service = relay_service.clone();
            Box::pin(async move {
                if let Some(relay_service) = relay_service.upgrade() {
                    relay_service.lock().await.status.clone()
                } else {
                    Status::default()
                }
            })
        }))
    }

    fn relay_already_added(&self, interface_address: Ipv4Addr, streamer_url: &str) -> bool {
        self.relays.iter().any(|relay| {
            relay.interface_address == interface_address && relay.streamer_url == streamer_url
        })
    }

    async fn remove_relays(&mut self) {
        let mut relays_to_keep: Vec<ServiceRelay> = Vec::new();
        let mut relays_to_remove: Vec<ServiceRelay> = Vec::new();
        for relay in self.relays.drain(..) {
            let streamer_exists = self
                .discovered_streamers
                .values()
                .any(|streamer| streamer.url == relay.streamer_url);
            if streamer_exists
                && Self::should_keep_relay(&self.network_interfaces, relay.interface_address)
            {
                relays_to_keep.push(relay);
            } else {
                relays_to_remove.push(relay);
            }
        }
        self.relays = relays_to_keep;
        for relay in relays_to_remove {
            info!(
                "Removing relay called {} with interface address {} for streamer name {} and URL \
                 {}",
                relay.interface_name,
                relay.interface_address,
                relay.streamer_name,
                relay.streamer_url
            );
            relay.relay.stop().await;
        }
    }

    fn should_keep_relay(
        network_interfaces: &[NetworkInterface],
        interface_address: Ipv4Addr,
    ) -> bool {
        network_interfaces
            .iter()
            .any(|interface| get_first_ipv4_address(interface) == Some(interface_address))
    }
}

pub struct RelayService {
    inner: Arc<Mutex<RelayServiceInner>>,
}

impl RelayService {
    pub async fn new(
        password: String,
        network_interfaces_to_allow: Vec<String>,
        network_interfaces_to_ignore: Vec<String>,
        get_status: Option<GetStatusClosure>,
        database: PathBuf,
    ) -> Self {
        Self {
            inner: RelayServiceInner::new(
                password,
                network_interfaces_to_allow,
                network_interfaces_to_ignore,
                get_status,
                database,
            )
            .await,
        }
    }

    pub async fn start(&self) {
        self.inner.lock().await.start().await;
    }

    pub async fn stop(&self) {
        self.inner.lock().await.stop().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn replacing_or_removing_discovered_url_retires_only_its_relay() {
        let service =
            RelayServiceInner::new(String::new(), vec![], vec![], None, PathBuf::new()).await;
        let mut service = service.lock().await;
        // Keep the interface present while reconciling without opening sockets.
        let address = Ipv4Addr::LOCALHOST;
        service.network_interfaces = vec![NetworkInterface::new_afinet(
            "lo", address, None, None, 1, true,
        )];
        let fullname = format!("Streamer.{MDNS_SERVICE_TYPE}");
        let other = format!("Other.{MDNS_SERVICE_TYPE}");
        let old_url = "ws://192.0.2.1:7777";
        let new_url = "ws://192.0.2.2:7777";
        let streamer = |url: &str| Streamer {
            name: "Shared display name".into(),
            url: url.into(),
        };
        let relay = |url: &str| ServiceRelay {
            interface_name: "lo".into(),
            interface_address: address,
            streamer_name: streamer(url).name,
            streamer_url: url.into(),
            relay: Relay::new(),
        };
        service
            .update_discovered_streamer(fullname.clone(), Some(streamer(old_url)))
            .await;
        service
            .update_discovered_streamer(other.clone(), Some(streamer(new_url)))
            .await;
        service.relays.push(relay(old_url));
        service.relays.push(relay(new_url));

        service
            .update_discovered_streamer(fullname.clone(), Some(streamer(new_url)))
            .await;
        assert_eq!(service.discovered_streamers.len(), 2);
        assert_eq!(service.relays.len(), 1, "the replaced URL was retired");
        assert_eq!(service.relays[0].streamer_url, new_url);

        service.update_discovered_streamer(other, None).await;
        assert_eq!(
            service.relays.len(),
            1,
            "the remaining instance still advertises this URL"
        );
        service
            .update_discovered_streamer(fullname.to_ascii_uppercase(), None)
            .await;
        assert!(service.discovered_streamers.is_empty());
        assert!(service.relays.is_empty());
    }
}
