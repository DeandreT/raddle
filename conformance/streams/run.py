#!/usr/bin/env python3
"""Run one disposable open-source broker family at a time, with owned cleanup."""

import argparse
import fcntl
import json
import os
from pathlib import Path
import socket
import signal
import shutil
import subprocess
import time
import uuid


ROOT = Path(__file__).resolve().parents[2]
BUILD = Path("/mnt/perok/build/raddle/parity-native")


def command(args, *, timeout=300, check=True, env=None):
    # A timed-out flock must not orphan Cargo descendants holding the build lock.
    # Only this captured, newly created process group is ever signalled.
    process = subprocess.Popen(args, cwd=ROOT, env=env, text=True, stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, start_new_session=True)
    try:
        output, _ = process.communicate(timeout=timeout)
    except BaseException:
        try:
            os.killpg(process.pid, signal.SIGTERM)
            process.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.communicate()
        except ProcessLookupError:
            process.wait()
        raise
    result = subprocess.CompletedProcess(args, process.returncode, output)
    if check:
        result.check_returncode()
    return result


def phase(family, run, directory):
    images = {
        "nats": ["nats:2.14.7-alpine", "nats:2.12.5-alpine"],
        "redis": ["redis:8.2.9-alpine", "redis:8.2.4-alpine"],
        "mqtt": ["eclipse-mosquitto:2"],
    }[family]
    receipt = {"family": family, "run": run, "requested_image": images[0],
               "license": {"nats": "Apache-2.0", "redis": "AGPL-3.0-only (selected alternative)",
                           "mqtt": "EPL-2.0 OR BSD-3-Clause (EDL-1.0)"}[family]}
    path = directory / f"{family}.json"
    container = None
    container_name = f"raddle-streams-{run}-{family}"
    try:
        crate = {"nats": "raddle-nats", "redis": "raddle-redis", "mqtt": "raddle-mqtt"}[family]
        print(f"{family}: waiting for shared compile access before starting a broker", flush=True)
        # Native flock on the same project-wide file. Keep the lock through copying
        # executables, then release it before broker startup/runtime tests. Another
        # lane can rebuild its target without changing the binaries under this run.
        with (BUILD / "compile.lock").open("a") as lease:
            fcntl.flock(lease, fcntl.LOCK_EX)
            built = command(["cargo", "test", "-p", crate, "--tests", "--no-run", "--locked",
                             "--message-format=json", "--target-dir", str(BUILD / "streams-target"),
                             "--config", "profile.dev.debug=0", "-j", "1"], timeout=3600, check=False)
            (directory / f"{family}-compile.log").write_text(built.stdout)
            receipt["compile_exit"] = built.returncode
            if built.returncode:
                raise RuntimeError(f"{family} fixture compilation failed")
            binaries = []
            binary_directory = directory / "bin"
            binary_directory.mkdir(exist_ok=True)
            for line in built.stdout.splitlines():
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                identity = event.get("package_id", "").rsplit("#", 1)[-1].split("@", 1)[0]
                if (event.get("reason") == "compiler-artifact" and identity == crate
                        and event.get("executable") and event.get("profile", {}).get("test")):
                    destination = binary_directory / f"{family}-{event['target']['name']}"
                    shutil.copy2(event["executable"], destination)
                    binaries.append(destination)
            if not binaries:
                raise RuntimeError(f"{family} fixture compilation returned no test executables")
            receipt["test_binaries"] = [str(path) for path in binaries]
        for image in images:
            pulled = command(["docker", "pull", image], check=False)
            (directory / f"{family}-pull-{image.split(':')[-1]}.log").write_text(pulled.stdout)
            if pulled.returncode == 0:
                receipt["actual_image"] = image
                print(f"{family}: using {image}; requested {images[0]}", flush=True)
                break
        else:
            raise RuntimeError(f"No authorized {family} image available")
        state = directory / f"{family}-state"
        state.mkdir(mode=0o777)
        state.chmod(0o777)
        args = ["docker", "run", "-d", "--name", container_name,
                "--label", f"raddle.streams.run={run}", "--label", "raddle.streams.fixture=true",
                "--user", f"{os.getuid()}:{os.getgid()}",
                "--memory", "256m", "--cpus", "1", "-v", f"{state}:/data"]
        port = {"nats": 4222, "redis": 6379, "mqtt": 1883}[family]
        args.extend(["-p", f"127.0.0.1::{port}"])
        if family == "nats":
            config = directory / "nats.conf"
            config.write_text('port: 4222\njetstream {\n store_dir: "/data"\n max_memory_store: 67108864\n max_file_store: 268435456\n}\n')
            args.extend(["-v", f"{config}:/fixture/nats.conf:ro"])
        if family == "mqtt":
            config = directory / "mosquitto.conf"
            config.write_text("listener 1883 0.0.0.0\nallow_anonymous true\npersistence true\npersistence_location /data/\nlog_dest stdout\nmax_queued_messages 256\n")
            args.extend(["-v", f"{config}:/mosquitto/config/mosquitto.conf:ro"])
        elif family == "redis":
            # Invoke the unmodified core binary, not the image's wrapper that auto-loads
            # unrelated search/vector modules with a much larger memory footprint.
            args.extend(["--entrypoint", "/usr/local/bin/redis-server"])
        args.append(receipt["actual_image"])
        if family == "nats":
            args.extend(["-c", "/fixture/nats.conf"])
        elif family == "redis":
            args.extend(["--bind", "0.0.0.0", "--protected-mode", "no", "--appendonly", "yes", "--appendfsync", "always", "--maxmemory", "64mb",
                         "--maxmemory-policy", "noeviction", "--save", ""])
        created = command(args)
        candidate = created.stdout.strip().splitlines()[-1]
        if len(candidate) != 64 or any(char not in "0123456789abcdef" for char in candidate):
            raise RuntimeError("Docker did not return a container ID; recovering by owned name")
        container = candidate
        receipt["container_id"] = container
        path.write_text(json.dumps(receipt, indent=2))
        info = json.loads(command(["docker", "inspect", container]).stdout)[0]
        address = info["NetworkSettings"]["Ports"][f"{port}/tcp"][0]
        bound_port = int(address["HostPort"])
        receipt["loopback_port"] = bound_port
        # This is broker readiness, not polling a background build for completion.
        deadline = time.monotonic() + 120
        while True:
            try:
                with socket.create_connection(("127.0.0.1", bound_port), timeout=1) as connection:
                    if family == "redis":
                        connection.sendall(b"*1\r\n$4\r\nPING\r\n")
                        with connection.makefile("rb") as response:
                            pong = response.readline(64)
                        if pong != b"+PONG\r\n":
                            raise OSError("Redis is not ready for commands")
                    break
            except OSError:
                if time.monotonic() >= deadline:
                    raise RuntimeError(f"{family} startup deadline expired")
                time.sleep(0.25)
        env = os.environ.copy()
        variable = {"nats": "RADDLE_NATS_URL", "redis": "RADDLE_REDIS_URL", "mqtt": "RADDLE_MQTT_PORT"}[family]
        env[variable] = str(bound_port) if family == "mqtt" else f"{family}://127.0.0.1:{bound_port}"
        env["RADDLE_FIXTURE_IMAGE"] = receipt["actual_image"]
        outputs = []
        outcomes = []
        for binary in binaries:
            result = command([str(binary), "--ignored", "--nocapture", "--test-threads=1"], env=env, timeout=300, check=False)
            outputs.append(result.stdout)
            outcomes.append(result.returncode)
            print(result.stdout, flush=True)
        (directory / f"{family}-tests.log").write_text("\n".join(outputs))
        receipt["test_exit"] = max(outcomes)
        if any(outcomes):
            raise RuntimeError(f"{family} broker tests failed")
    finally:
        if container is None:
            # Docker can finish creation after a CLI timeout. Recover only our exact
            # run-unique name, verify ownership, then capture its ID for cleanup.
            found = command(["docker", "inspect", container_name], check=False)
            if found.returncode == 0:
                owned = json.loads(found.stdout)[0]
                if owned.get("Config", {}).get("Labels", {}).get("raddle.streams.run") == run:
                    container = owned["Id"]
                    receipt["container_id"] = container
        if container:
            logs = command(["docker", "logs", container], check=False)
            (directory / f"{family}-broker.log").write_text(logs.stdout)
            # Exact captured ID only; never ancestor/name-pattern cleanup.
            removed = command(["docker", "rm", "-f", container], check=False)
            receipt["cleanup_exit"] = removed.returncode
            (directory / f"{family}-cleanup.log").write_text(removed.stdout)
        path.write_text(json.dumps(receipt, indent=2))
        if receipt.get("cleanup_exit", 0):
            raise RuntimeError(f"{family} owned container cleanup failed; see receipt")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("families", nargs="*", choices=["nats", "redis", "mqtt"])
    args = parser.parse_args()
    mounted = command(["findmnt", "-n", "-o", "SOURCE,FSTYPE", "-T", "/mnt/perok"]).stdout.split()
    if mounted != ["/dev/sdb1", "btrfs"]:
        raise RuntimeError("Required /dev/sdb1 btrfs build disk is not mounted")
    run = uuid.uuid4().hex[:12]
    directory = BUILD / "streams-runs" / run
    directory.mkdir(parents=True)
    print(f"Fixture receipts/logs/state: {directory}", flush=True)
    failed = []
    for family in args.families or ["nats", "redis", "mqtt"]:
        try:
            phase(family, run, directory)
        except (RuntimeError, subprocess.SubprocessError) as error:
            print(f"{family}: {error}", flush=True)
            failed.append(family)
    if failed:
        raise SystemExit("Failed families: " + ", ".join(failed))


if __name__ == "__main__":
    main()
