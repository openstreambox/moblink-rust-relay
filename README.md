# Moblink (Rust🦀 edition)

Use spare devices as extra SRTLA bonding connections. The same functionality is part of [Moblin](https://github.com/eerimoq/moblin) on iOS.

Originally inspired by the Moblink Kotlin/Android code.

## Features

- **WebSocket Connection**: Connects to Moblin via WebSocket (e.g., `wss://...`)  
- **Auth Handling**: Implements the same challenge–response authentication logic as the Android client  
- **UDP Relay**: Forwards UDP packets between the remote streamer and a local destination.  
- **mDNS**: Automatically connect to nearby Moblink devices.

## Requirements

- **Rust** (stable, e.g., 1.85+)
- **Cargo** (for building)

## Usage

### Use on Belabox

```bash
ssh user@belabox.local
```

password: See belabox under advanced / developer

ssh to your belabox and run this:

```bash
wget -q -O - https://raw.githubusercontent.com/datagutt/moblink-rust/refs/heads/main/install/belabox/install.sh | sudo sh
```

Then start a Moblink relay and open <http://belabox.local/> in your browser.

If not, make sure your relay has a short name. At most ~10 characters
 --> Make sure the password is "1234"  and put it on AUTO on the android phone.

### Build

```bash
# 1. Clone this repository (or copy the code)
git clone https://github.com/datagutt/moblink-rust.git
cd moblink-rust

# Set nightly (optional)
rustup override set nightly

# 2. Build the project
cargo build --release
```

### Run Relay

```bash
./target/release/moblink-relay \
  --name "RelayName" \
  --id "UUID" \
  --streamer-url ws://192.168.1.2:7777 \
  --password "secret123" \
  --bind-address 192.168.1.10
  --log-level debug
```

#### Command-Line Arguments

| Argument         | Description                                                                  | Default       | Example                                     |
|------------------|------------------------------------------------------------------------------|---------------|---------------------------------------------|
| `--name`         | Name to identify the relay                                                   | Hostname      | `--name CameraRelay1`                       |
| `--id`           | UUID to identify the Relay                                                   | Generated     | `--id UUID`                                 |
| `--streamer-url` | WebSocket URL to connect to the streamer                                     | _None_ (multicast DNS) | `--streamer-url wss://example.com/ws` |
| `--password`     | Password used in the challenge–response authentication                       | `1234`        | `--password mySecret`                       |
| `--log-level`    | Logging verbosity (e.g., error, warn, info, debug, trace)                    | `info`        | `--log-level debug`                         |
| `--bind-address` | Local modem IP address to bind for UDP socket                                | `0.0.0.0`     | `--bind-address 192.168.1.10`               |
| `--status-executable` | Status executable. Print status to standard output on format {"batteryPercentage": 93} | _None_ | `--status-executable ./status.sh`   |
| `--status-file` | Status file. Contains status on format {"batteryPercentage": 93}              | _None_        | `--status-file status.json`                 |

Relay status (today only battery percentage) is sent to the streamer if `--status-executable` or `--status-file` is given and outputting a valid JSON object as seen above.

If `--status-executable` takes longer than three seconds, the relay reports
empty status and keeps forwarding. The timeout kills only the direct child
process. Any subprocesses it launched can continue running.

### Run Streamer

```bash
./target/release/moblink-streamer \
  --websocket-server-address 192.168.1.2 \
  --destination-address 172.120.50.214 \
  --destination-port 5000
```

#### Command-Line Arguments

| Argument         | Description                                                                  | Default       | Example                                     |
|------------------|------------------------------------------------------------------------------|---------------|---------------------------------------------|
| `--name`         | Name to identify the streamer                                                | Hostname      | `--name CameraRelay1`                       |
| `--id`           | Id to identify the streamer using multicast DNS                              | Hostname      | `--id UUID`                                 |
| `--password`     | Password used in the challenge–response authentication                       | `1234`        | `--password mySecret`                       |
| `--log-level`    | Logging verbosity (e.g., error, warn, info, debug, trace)                    | `info`        | `--log-level debug`                         |
| `--websocket-server-address` | Local IP address to bind websocket server to                     |               | `--websocket-server-address 192.168.1.10`   |
| `--websocket-server-port` | Local port to bind the websocket server to                          | `7777`        | `--websocket-server-port 7778`              |
| `--tun-ip-network` | TUN IP network (CIDR notation). TUN network interfaces will be assigned IP addresses from this network. | `10.3.3.0/24` | `--tun-ip-network 10.1.1.0/24` |
| `--destination-address` | Streaming destination address                                         |               | `--status-file status.json`                 |
| `--destination-port` | Streaming destination port                                               |               | `--status-file status.json`                 |

## Relay Architecture

1. **WebSocket Connection**  
   - Establishes a WebSocket to `streamer_url`, or if not provided, tries to find nearby Moblink streamers through multicast DNS.
   - Handles “Hello” messages, calculates authentication, and sends an “Identify” message.

2. **Handling Requests**  
   - When a `startTunnel` request is received, the relay spawns two async tasks:  
     - **(relay_to_destination)**: Forwards traffic from streamer → destination  
     - **(relay_to_streamer)**: Forwards traffic from destination → streamer  

3. **UDP Binding**  
   - By default, it binds a UDP socket to whatever we deem to be the main network interface.

## FAQ

**Q:** How do I integrate this into my own application?  
**A:** Use the moblink-rust [crate](https://crates.io/crates/moblink-rust)

---

**License**: This project is distributed under the terms of the MIT license.

Enjoy using **Moblink**!
