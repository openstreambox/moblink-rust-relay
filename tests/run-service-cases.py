"""Stage and supervise the privileged half of run-service-failures.sh."""
import ctypes
import json
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


def manager_command(errors, *args):
    try:
        result = run(*args, capture_output=True, text=True, errors="replace")
    except (OSError, subprocess.TimeoutExpired) as error:
        errors.append(f"{' '.join(args)}: {error}")
        return None
    if result.returncode != 0:
        errors.append(f"{' '.join(args)}: {result.stderr.strip() or result.returncode}")
        return None
    return result.stdout


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


def unit_state(name, errors):
    output = manager_command(
        errors, "systemctl", "show",
        "--property=LoadState,ActiveState,MainPID,ControlPID,Job", name,
    )
    return dict(
        line.split("=", 1) for line in (output or "").splitlines() if "=" in line
    )


def stopped(state):
    return (
        state.get("ActiveState") in ("inactive", "failed")
        and state.get("MainPID") == state.get("ControlPID") == "0"
        and state.get("Job") == ""
    )


def clean_units(prefix, logs, failed, errors):
    names = [f"{prefix}-{slot}.service" for slot in range(2)]
    reload_needed = False
    for name in names:
        unit = RUNTIME / name
        dropin = RUNTIME / f"{name}.d"
        state = unit_state(name, errors)
        if state.get("LoadState") != "not-found" or not stopped(state):
            manager_command(errors, "systemctl", "stop", name)
            state = unit_state(name, errors)
        if not stopped(state):
            errors.append(f"{name}: cannot confirm stopped, leaving unit files in place")
            continue
        if state["ActiveState"] == "failed":
            manager_command(errors, "systemctl", "reset-failed", name)
        # A harness can die while installing the unit or its drop-in.
        try:
            if dropin.exists():
                reload_needed = True
                shutil.rmtree(dropin)
            if unit.exists():
                reload_needed = True
                unit.unlink(missing_ok=True)
        except OSError as error:
            errors.append(f"{name}: {error}")
    if reload_needed:
        manager_command(errors, "systemctl", "daemon-reload")

    if failed or errors:
        manager_command(errors, "journalctl", "--sync")
    for name in names:
        # journalctl exits 1 when a reserved slot was never used.
        try:
            journal = run("journalctl", "--no-pager", "-u", name, capture_output=True)
        except (OSError, subprocess.TimeoutExpired) as error:
            errors.append(f"{name} journal: {error}")
            continue
        if journal.stderr:
            errors.append(f"{name} journal: {journal.stderr.decode(errors='replace').strip()}")
        if journal.stdout.strip() not in (b"", b"-- No entries --"):
            logs[f"{name}.log"] = journal.stdout.decode(errors="replace")


def run_case(stage, logs, index, test_name, errors):
    prefix = f"{stage.name}-{index}"
    executable = stage / "service_failures"
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
        stdout=sys.stderr,
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
        except (OSError, subprocess.TimeoutExpired) as error:
            errors.append(f"{prefix}: could not reap test process group: {error}")
        clean_units(prefix, logs, process.returncode != 0 or INTERRUPTED is not None, errors)
    return process.returncode if INTERRUPTED is None else 128 + INTERRUPTED


def supervise(test_bin, artifact_dir, test_names, logs):
    # PR_SET_CHILD_SUBREAPER. If the harness dies, its helpers reparent
    # here instead of to PID 1, so the launcher can reap the entire group.
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "PR_SET_CHILD_SUBREAPER")
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, interrupted)

    errors = []
    stage = Path(tempfile.mkdtemp(prefix="moblink-tests.", dir="/run"))
    try:
        shutil.copyfile(test_bin, stage / "service_failures")
        (stage / "service_failures").chmod(0o755)
        for binary in sorted(artifact_dir.glob(f"moblink-*-{TARGET}")):
            shutil.copyfile(binary, stage / binary.name)
            (stage / binary.name).chmod(0o755)
        manifest = f"SHA256SUMS-{TARGET}"
        shutil.copyfile(artifact_dir / manifest, stage / manifest)
        run("sha256sum", "--check", manifest, cwd=stage, check=True, stdout=sys.stderr)

        for index, test_name in enumerate(test_names):
            if INTERRUPTED is not None:
                return 128 + INTERRUPTED
            result = run_case(stage, logs, index, test_name, errors)
            if errors:
                return 1
            if result != 0:
                return 128 - result if result < 0 else result
    finally:
        try:
            for log in stage.glob("relay-*.log"):
                logs[f"{stage.name}-{log.name}"] = log.read_text(errors="replace")
        finally:
            if errors:
                print(f"Retained {stage} after fixture cleanup errors:", file=sys.stderr)
                for error in errors:
                    print(f"  {error}", file=sys.stderr)
            else:
                shutil.rmtree(stage)
    return 0


def launch(directory, arguments):
    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    process = None

    def forward(signum, frame):
        interrupted(signum, frame)
        if process is not None:
            process.send_signal(signum)

    try:
        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
            signal.signal(sig, forward)
        process = subprocess.Popen(
            ["sudo", "-n", "python3", __file__, *arguments],
            stdout=subprocess.PIPE,
        )
        if INTERRUPTED is not None:
            process.send_signal(INTERRUPTED)
        data, _ = process.communicate()
        logs = json.loads(data) if data else {}
        for name, content in logs.items():
            if Path(name).name != name or not name.endswith(".log"):
                raise ValueError(f"invalid log file name: {name!r}")
            log = os.open(
                name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                0o644, dir_fd=fd,
            )
            with os.fdopen(log, "w", encoding="utf-8") as output:
                output.write(content)
        return process.returncode if process.returncode >= 0 else 128 - process.returncode
    finally:
        os.close(fd)


def main():
    if sys.argv[1] == "--launch":
        return launch(sys.argv[2], sys.argv[3:])
    logs = {}
    try:
        return supervise(Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3:], logs)
    finally:
        json.dump(logs, sys.stdout)


if __name__ == "__main__":
    sys.exit(main())
