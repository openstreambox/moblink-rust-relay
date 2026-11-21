//! Private veth links connecting the relay namespace to a separate peer
//! namespace. Neither end lives in the host namespace.
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::os::fd::AsRawFd;
use std::process::{Child, Command, Stdio};

use tokio::net::{TcpListener, UdpSocket};

use crate::netns::ip;

pub const PHONE: Link = Link {
    relay: "mltphone",
    peer: "mltmoblin",
    subnet: 0,
};
pub const WAN_A: Link = Link {
    relay: "mltwana",
    peer: "mltingesta",
    subnet: 1,
};
pub const WAN_B: Link = Link {
    relay: "mltwanb",
    peer: "mltingestb",
    subnet: 2,
};

#[derive(Clone, Copy)]
pub struct Link {
    pub relay: &'static str,
    peer: &'static str,
    subnet: u8,
}

impl Link {
    pub fn ip(self, host: u8) -> Ipv4Addr {
        Ipv4Addr::new(10, 77, self.subnet, host)
    }

    pub fn add(self, peer: &Peer, host: u8) {
        ip(&[
            "link",
            "add",
            self.relay,
            "type",
            "veth",
            "peer",
            "name",
            self.peer,
            "netns",
            &peer.process.id().to_string(),
        ]);
        peer.enter(|| {
            ip(&[
                "addr",
                "add",
                &format!("{}/24", self.ip(2)),
                "dev",
                self.peer,
            ]);
            ip(&["link", "set", self.peer, "up"]);
        });
        self.change_address("add", host);
        ip(&["link", "set", self.relay, "up"]);
    }

    pub fn change_address(self, action: &str, host: u8) {
        ip(&[
            "addr",
            action,
            &format!("{}/24", self.ip(host)),
            "dev",
            self.relay,
        ]);
    }

    pub fn netem(self, peer: &Peer, impairment: &[&str]) {
        self.netem_on(self.relay, impairment);
        peer.enter(|| self.netem_on(self.peer, impairment));
    }

    fn netem_on(self, device: &str, impairment: &[&str]) {
        let mut args = vec!["qdisc", "replace", "dev", device, "root", "netem"];
        if impairment.is_empty() {
            tc(&["qdisc", "del", "dev", device, "root"]);
        } else {
            args.extend_from_slice(impairment);
            tc(&args);
        }
    }
}

fn tc(args: &[&str]) {
    let output = Command::new("tc").args(args).output().expect("run tc");
    assert!(output.status.success(), "tc {args:?}: {output:?}");
}

pub struct Peer {
    process: Child,
}

impl Peer {
    pub fn new() -> Self {
        let process = Command::new("unshare")
            .args([
                "--net",
                "--",
                "sh",
                "-ec",
                "ip link set lo up; echo ready; exec sleep infinity",
            ])
            .stdout(Stdio::piped())
            .spawn()
            .expect("spawn peer namespace");
        let mut peer = Self { process };
        let mut line = String::new();
        BufReader::new(peer.process.stdout.take().unwrap())
            .read_line(&mut line)
            .unwrap();
        assert_eq!(
            line.trim_end(),
            "ready",
            "peer namespace failed to initialize"
        );
        peer
    }

    /// Create sockets in the peer namespace on this thread, then return to the
    /// relay namespace before awaiting anything. Sockets retain their
    /// namespace.
    pub fn enter<T>(&self, run: impl FnOnce() -> T) -> T {
        let original = File::open("/proc/thread-self/ns/net").unwrap();
        let target = File::open(format!("/proc/{}/ns/net", self.process.id())).unwrap();
        struct Restore(File);
        impl Drop for Restore {
            fn drop(&mut self) {
                switch_to(&self.0);
            }
        }
        switch_to(&target);
        let _restore = Restore(original);
        run()
    }

    pub fn tcp(&self, addr: Ipv4Addr) -> TcpListener {
        TcpListener::from_std(self.enter(|| {
            let socket = std::net::TcpListener::bind(SocketAddrV4::new(addr, 0)).unwrap();
            socket.set_nonblocking(true).unwrap();
            socket
        }))
        .unwrap()
    }

    pub fn udp(&self, addr: Ipv4Addr) -> UdpSocket {
        UdpSocket::from_std(self.enter(|| {
            let socket = std::net::UdpSocket::bind(SocketAddrV4::new(addr, 0)).unwrap();
            socket.set_nonblocking(true).unwrap();
            socket
        }))
        .unwrap()
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

fn switch_to(namespace: &File) {
    // SAFETY: the open descriptor refers to a network namespace.
    let result = unsafe { libc::setns(namespace.as_raw_fd(), libc::CLONE_NEWNET) };
    assert_eq!(result, 0, "setns: {}", std::io::Error::last_os_error());
}
