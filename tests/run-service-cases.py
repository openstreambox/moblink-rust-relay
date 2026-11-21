"""Stage and supervise the privileged half of run-service-failures.sh."""
import ctypes
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time

TARGET = "x86_64-unknown-linux-musl"
RUNTIME = Path("/run/systemd/system")
INTERRUPTED = None


def interrupted(signum, _frame):
    global INTERRUPTED
    INTERRUPTED = signum


def run(*args, **kwargs):
    return subprocess.run(args, timeout=15, **kwargs)


def reap_group(process):
    # The namespace keeper and legacy relay share this process group. Reap
    # both even when the Rust test is killed before its guards can run.
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait(timeout=15)
    while True:
        try:
            os.waitpid(-process.pid, 0)
        except ChildProcessError:
            break


def clean_units(prefix, logs):
    names = [f"{prefix}-{slot}.service" for slot in range(2)]
    reload_needed = False
    errors = []
    for name in names:
        unit = RUNTIME / name
        dropin = RUNTIME / f"{name}.d"
        load = run(
            "systemctl", "show", "--value", "--property=LoadState", name,
            capture_output=True,
        )
        if load.stdout.strip() != b"not-found":
            stopped = run("systemctl", "stop", name, capture_output=True)
            if stopped.returncode != 0:
                errors.append(f"{name}: {stopped.stderr.decode(errors='replace').strip()}")
            run("systemctl", "reset-failed", name, capture_output=True)
        # The harness can die before the manager has loaded the new files.
        if unit.exists() or dropin.exists():
            if dropin.exists():
                shutil.rmtree(dropin)
            unit.unlink(missing_ok=True)
            reload_needed = True
    if reload_needed:
        run("systemctl", "daemon-reload", check=True)

    # Journald retains the unit's events after normal Rust teardown too.
    run("journalctl", "--sync", check=True)
    for name in names:
        journal = run("journalctl", "--no-pager", "-u", name, capture_output=True)
        if journal.stdout.strip() not in (b"", b"-- No entries --"):
            path = logs / f"{name}.log"
            path.write_bytes(journal.stdout)
            path.chmod(0o644)
    if errors:
        raise RuntimeError("; ".join(errors))


def run_case(stage, logs, index, test_name):
    prefix = f"{stage.name}-{index}"
    executable = stage / "service_failures"
    selection = run(executable, "--ignored", "--exact", test_name,
                    "--list", "--format", "terse", check=True, text=True, capture_output=True)
    if selection.stdout.splitlines() != [f"{test_name}: test"]:
        raise RuntimeError(f"{test_name} must select exactly one test: {selection.stdout!r}")

    process = subprocess.Popen(
        ["unshare", "--map-root-user", "--net", "--",
         "sh", "-ec", 'ip link set lo up; exec "$@"', "sh", executable,
         "--ignored", "--test-threads=1", "--nocapture", "--exact", test_name],
        env={
            **os.environ, "LC_ALL": "C", "TMPDIR": str(stage),
            "MOBLINK_TEST_NETNS": "1", "MOBLINK_TEST_SYSTEMD": "1",
            "MOBLINK_TEST_UNIT_PREFIX": prefix, "MOBLINK_TEST_LOG_DIR": str(stage),
            "MOBLINK_RELAY_BIN": str(stage / f"moblink-relay-{TARGET}"),
            "MOBLINK_RELAY_SERVICE_BIN": str(stage / f"moblink-relay-service-{TARGET}"),
        },
        start_new_session=True,
    )
    try:
        while INTERRUPTED is None:
            # Keep the exited group leader unreaped until after killpg, so
            # its PID cannot be reused by an unrelated process group.
            if os.waitid(os.P_PID, process.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT):
                break
            time.sleep(0.1)
    finally:
        try:
            reap_group(process)
        finally:
            clean_units(prefix, logs)
    return process.returncode if INTERRUPTED is None else 128 + INTERRUPTED


def main():
    test_bin, artifact_dir, log_dir = map(Path, sys.argv[1:4])
    test_names = sys.argv[4:]
    logs = log_dir.resolve()
    # PR_SET_CHILD_SUBREAPER. If the harness dies, its helpers reparent
    # here instead of to PID 1, so the launcher can reap the entire group.
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "PR_SET_CHILD_SUBREAPER")
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, interrupted)

    with tempfile.TemporaryDirectory(prefix="moblink-tests.", dir="/run") as directory:
        stage = Path(directory)
        shutil.copyfile(test_bin, stage / "service_failures")
        (stage / "service_failures").chmod(0o755)
        for binary in sorted(artifact_dir.glob(f"moblink-*-{TARGET}")):
            shutil.copyfile(binary, stage / binary.name)
            (stage / binary.name).chmod(0o755)
        manifest = f"SHA256SUMS-{TARGET}"
        shutil.copyfile(artifact_dir / manifest, stage / manifest)
        run("sha256sum", "--check", manifest, cwd=stage, check=True)

        try:
            for index, test_name in enumerate(test_names):
                if INTERRUPTED is not None:
                    return 128 + INTERRUPTED
                result = run_case(stage, logs, index, test_name)
                if result != 0:
                    return 128 - result if result < 0 else result
        finally:
            for log in stage.glob("relay-*.log"):
                # Prefix prevents a later run from replacing this diagnostic.
                path = logs / f"{stage.name}-{log.name}"
                shutil.copyfile(log, path)
                path.chmod(0o644)
    return 0


if __name__ == "__main__":
    sys.exit(main())
