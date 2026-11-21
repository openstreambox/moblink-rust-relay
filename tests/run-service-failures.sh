#!/usr/bin/env bash
# Run from the project root on a disposable Linux systemd host.
set -euo pipefail

case "${1:-}" in
  "") test_names=(service_crash_backoff_start_limit_and_operator_recovery
                  service_discovers_multicast_streamer_and_rediscovery
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
# The inner user namespace cannot traverse arbitrary caller-owned paths.
# Stage only the test executable and release artifacts in a disposable path.
stage=$(sudo -n mktemp -d /run/moblink-tests.XXXXXX)
trap 'sudo -n rm -r -- "$stage"' EXIT
sudo -n install -m 0755 "$test_bin" "$stage/service_failures"
sudo -n install -m 0755 "$artifact_dir"/moblink-*-"$target" "$stage/"
sudo -n install -m 0644 "$artifact_dir/SHA256SUMS-$target" "$stage/"
sudo -n sh -ec 'cd "$1"; sha256sum --check "$2"' sh "$stage" "SHA256SUMS-$target"
for test_name in "${test_names[@]}"; do
  sudo -n unshare --map-root-user --net --fork --kill-child -- \
    env MOBLINK_TEST_NETNS=1 MOBLINK_TEST_SYSTEMD=1 \
      TMPDIR="$stage" MOBLINK_TEST_LOG_DIR="$stage" \
      MOBLINK_RELAY_BIN="$stage/moblink-relay-$target" \
      MOBLINK_RELAY_SERVICE_BIN="$stage/moblink-relay-service-$target" \
    sh -ec 'ip link set lo up; exec "$@"' sh \
      "$stage/service_failures" --ignored --test-threads=1 --nocapture --exact "$test_name"
done
