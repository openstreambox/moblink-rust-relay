# Relay service failure tests

The release-binary job runs five service scenarios on its Ubuntu GitHub runner.
Each scenario gets a fresh network namespace owned by a new user namespace and
a separate peer namespace. The host systemd starts a uniquely named runtime
system unit using the packaged BELABOX unit and [a test drop-in](fixtures/box.conf).
The unit joins the relay namespace through `NetworkNamespacePath`. The
runner removes the unit and its state at the end of the test.

| Scenario | Evidence |
| --- | --- |
| Two uplinks and netem | Separate egress receivers see tagged UDP and the expected source address. Netem on both ends of one uplink adds delay, loss and reordering. The independent uplink keeps carrying traffic during the blackhole. The impaired path recovers after blackhole, carrier cut, address removal and renumbering, and veth deletion and replug. Retired ports stay quiet, the interface UUID survives, and the service PID stays the same. |
| Systemd crash and rate limit | SIGKILL closes the original session and leaves its port quiet. The restarted service reauthenticates with a fresh challenge and the same UUID, publishes authenticated status under the new PID, and carries UDP. Failed pre-start checks exhaust the retry budget. After the input is restored, a start is refused until `reset-failed`, then fresh traffic succeeds. |
| Multicast DNS | The publisher and relay are on opposite veth ends. The relay discovers a DNS-SD advertisement, then carries traffic after the same instance is withdrawn and reannounced with a new IPv4 address. Distinct instances with the same display name each carry UDP, and an advertisement pointing at the relay itself is ignored. Fixed-URL mode carries traffic while bypassing the multicast advertisements. |
| Multicast DNS retirement | DNS-SD withdrawal closes the active relay and clears runtime status. The old listener stays available for longer than a retry interval and accepts no reconnect. Reannouncing the same instance starts one relay at the new address. |
| Supervisor rollback | The activation record gates the service and a legacy-owner fixture that launches the standalone relay. The test checks the disabled state, shuts down each old owner and checks that its last tunnel forwards nothing before activating the other owner, and checks traffic and legacy UUID restoration after rollback. |

## Run locally

Use a disposable x86_64 Linux VM with a systemd 255 or newer system manager,
`iproute2`, `util-linux`, Python 3, the veth and sch_netem kernel modules,
an executable `/run` mount, and noninteractive sudo. Use the Rust toolchain
named in the release workflow.
Extract its `Cargo.lock` artifact to the repository root and the corresponding
`moblink-x86_64-unknown-linux-musl` artifact to `dist/`, including its
`SHA256SUMS-x86_64-unknown-linux-musl`. From the repository root run:

```sh
RUSTUP_TOOLCHAIN=nightly-2026-05-25 bash tests/run-service-failures.sh
```

The launcher builds the test harness as the caller, stages and verifies the
release artifacts, then passes the verified service and standalone relay to the
harness as `MOBLINK_RELAY_SERVICE_BIN` and `MOBLINK_RELAY_BIN`. The launcher
owns the runtime unit names and the test process group. It kills and reaps
leftover helpers and removes each unit only after confirming it stopped. If
cleanup fails, it reports the errors together and retains staging under
`/run` for recovery. SIGKILL of the privileged supervisor itself bypasses
cleanup and requires an external owner, such as the disposable VM lifecycle.
The caller writes unit and legacy relay logs under `target/tmp/` for CI upload.
Plain `cargo test` skips the systemd scenarios because they need the launcher
and its release artifacts.

To rerun just the discovery retirement case that also runs in CI:

```sh
RUSTUP_TOOLCHAIN=nightly-2026-05-25 bash tests/run-service-failures.sh --strict-mdns
```

## Remaining qualification

The fixture exercises the planned activation and restart policy using the
current release's CLI with a synthetic password. The box candidate still needs
credential-file provisioning and secret non-exposure checks, live admission
and device binding (including overlapping addresses, map withdrawal and
default-route failover), and the actual backend and boot-slot handoff. Also
open are multi-address mDNS fallback and discovery soak. Budget and measure those with the pinned binary
and deployable box drop-ins.

Keep Mac and iPhone Moblin interoperability, Wi-Fi and cellular radio faults
(including SIM-backed donation), phone backgrounding, and end-to-end decoded
media continuity on the physical bench. The UDP checks here exercise the relay
transport.
