#!/usr/bin/env bash
# Run from the project root on a disposable Linux systemd host.
set -euo pipefail

case "${1:-}" in
  "") test_names=(service_crash_backoff_start_limit_and_operator_recovery
                  service_discovers_multicast_streamer_and_rediscovery
                  service_retires_withdrawn_mdns_url
                  service_rolls_back_to_one_legacy_owner
                  service_uplinks_survive_netem_cuts_renumbering_and_replug) ;;
  --strict-mdns) test_names=(service_retires_withdrawn_mdns_url) ;;
  *) echo "usage: bash tests/run-service-failures.sh [--strict-mdns]" >&2; exit 2 ;;
esac

artifact_dir="$PWD/dist"
target=x86_64-unknown-linux-musl
(
  cd "$artifact_dir"
  sha256sum --check "SHA256SUMS-$target"
  chmod +x "moblink-relay-$target" "moblink-relay-service-$target"
)

# Build as the caller. Select Cargo's exact test executable so sudo never
# invokes Cargo or picks up a stale binary in the target directory.
test_bin=$(
  cargo test --locked --test service_failures --no-run --message-format=json |
    python3 -c '
import json
import sys

executables = []
for line in sys.stdin:
    event = json.loads(line)
    if event["reason"] == "compiler-message":
        sys.stderr.write(event["message"].get("rendered") or "")
    if event["reason"] == "compiler-artifact" and event["target"]["name"] == "service_failures":
        executables.append(event["executable"])
assert len(executables) == 1 and executables[0], executables
print(executables[0])
'
)

sudo -n modprobe veth
sudo -n modprobe sch_netem
# Staging, the process group, runtime units and diagnostics share one
# privileged owner with cleanup even if an individual harness is killed.
log_dir="$PWD/target/tmp"
mkdir -p "$log_dir"
exec sudo -n python3 tests/run-service-cases.py "$test_bin" "$artifact_dir" "$log_dir" "${test_names[@]}"
