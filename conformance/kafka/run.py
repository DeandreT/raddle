"""Run isolated Kafka sockets against a run-owned official Kafka 4 fixture."""

import argparse
import base64
import fcntl
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import socket
import subprocess
import sys
import uuid

from wire import WireProxy

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = Path('/mnt/perok/build/raddle/parity-native/kafka-fixtures')
IMAGE = 'apache/kafka:4.1.2'


def mounted():
    mount = subprocess.check_output(['findmnt', '-n', '-T', '/mnt/perok', '-o', 'SOURCE,FSTYPE'], text=True).split()
    if mount != ['/dev/sdb1', 'btrfs']:
        raise RuntimeError('The verified perok build volume is required')


def build_directory(path):
    path = path.resolve()
    if not path.is_relative_to(Path('/mnt/perok/build')):
        raise RuntimeError('Fixture and build output must remain below the perok build directory')
    existing = path
    while not existing.exists():
        existing = existing.parent
    mount = subprocess.check_output(['findmnt', '-n', '-T', str(existing), '-o', 'SOURCE,FSTYPE'], text=True).split()
    if mount != ['/dev/sdb1', 'btrfs']:
        raise RuntimeError('Output path is not on the verified perok build volume')
    path.mkdir(parents=True, exist_ok=True)
    return path


def docker(*arguments, capture=False, timeout=600):
    return subprocess.run(['docker', *arguments], check=True, text=True,
                          capture_output=capture, timeout=timeout)


def resolve_image():
    # Pull by digest so this task creates no mutable shared official image tag.
    manifest = json.loads(docker('manifest', 'inspect', '--verbose', IMAGE, capture=True).stdout)
    entries = manifest if isinstance(manifest, list) else [manifest]
    architecture = {'x86_64': 'amd64', 'aarch64': 'arm64'}.get(platform.machine())
    for entry in entries:
        descriptor = entry.get('Descriptor', {})
        target = descriptor.get('platform', {})
        digest = descriptor.get('digest', '')
        if target.get('os') == 'linux' and target.get('architecture') == architecture and digest.startswith('sha256:'):
            return 'apache/kafka@' + digest
    raise RuntimeError('No pinned native Linux Kafka image descriptor found')


def compose(state, *arguments, timeout=600):
    return docker('compose', '-p', state['project'], '-f', state['compose'], *arguments, timeout=timeout)


def up(proxy=None):
    mounted()
    build_directory(FIXTURES)
    if shutil.disk_usage(FIXTURES.parent).free < 5 * 1024**3:
        raise RuntimeError('Insufficient fixture disk space')
    image = resolve_image()
    run = uuid.uuid4().hex
    directory = FIXTURES / run
    directory.mkdir(parents=True, mode=0o700)
    data = directory / 'data'
    data.mkdir(mode=0o777)
    # The image's unprivileged account writes synthetic fixture data only.
    data.chmod(0o777)
    with socket.socket() as reservation:
        reservation.bind(('127.0.0.1', 0))
        port = reservation.getsockname()[1]
    if proxy is not None:
        proxy.backend = port
    advertised = port if proxy is None else proxy.port
    environment = {
        'CLUSTER_ID': base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip('='),
        'KAFKA_NODE_ID': '1', 'KAFKA_PROCESS_ROLES': 'broker,controller',
        'KAFKA_CONTROLLER_QUORUM_VOTERS': '1@broker:9093',
        'KAFKA_CONTROLLER_LISTENER_NAMES': 'CONTROLLER',
        'KAFKA_LISTENERS': 'INTERNAL://:9092,CONTROLLER://:9093,EXTERNAL://:29092',
        'KAFKA_ADVERTISED_LISTENERS': f'INTERNAL://broker:9092,EXTERNAL://127.0.0.1:{advertised}',
        'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP': 'INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT',
        'KAFKA_INTER_BROKER_LISTENER_NAME': 'INTERNAL',
        'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR': '1',
        'KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS': '3',
        'KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR': '1',
        'KAFKA_TRANSACTION_STATE_LOG_MIN_ISR': '1',
        'KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS': '3',
        'KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS': '0',
        # Leave broker auto-creation enabled to test the adapter's explicit
        # allow.auto.create.topics=false on both producer and consumer clients.
        'KAFKA_AUTO_CREATE_TOPICS_ENABLE': 'true',
        'KAFKA_LOG_DIRS': '/var/lib/kafka/data',
        'KAFKA_LOG_SEGMENT_BYTES': '16777216', 'KAFKA_LOG_RETENTION_BYTES': '67108864',
        'KAFKA_LOG_RETENTION_HOURS': '1', 'KAFKA_MESSAGE_MAX_BYTES': '2097152',
        'KAFKA_HEAP_OPTS': '-Xms256m -Xmx512m',
    }
    config = {'services': {'broker': {
        'image': image, 'cpus': 2, 'mem_limit': '2g',
        'environment': environment, 'labels': {'raddle.kafka.fixture': run},
        'ports': [f'127.0.0.1:{port}:29092'], 'volumes': [f'{data}:/var/lib/kafka/data'],
        'healthcheck': {'test': ['CMD', '/opt/kafka/bin/kafka-broker-api-versions.sh', '--bootstrap-server', 'localhost:9092'],
                        'interval': '5s', 'timeout': '20s', 'retries': 60, 'start_period': '20s'},
    }}}
    compose_file = directory / 'compose.json'
    compose_file.write_text(json.dumps(config, indent=2) + '\n')
    state = {'schema': 1, 'run': run, 'project': 'raddle-kafka-' + run,
             'compose': str(compose_file), 'image': image, 'bootstrap': f'127.0.0.1:{advertised}',
             'directory': str(directory), 'containers': []}
    if proxy is not None:
        state.update(mode='crc', proxy=proxy.descriptor())
    state_file = directory / 'state.json'
    state_file.write_text(json.dumps(state, indent=2) + '\n')
    try:
        compose(state, 'up', '-d', '--wait', '--wait-timeout', '360')
        result = docker('compose', '-p', state['project'], '-f', state['compose'], 'ps', '-aq', capture=True)
        state['containers'] = result.stdout.split()
        state_file.write_text(json.dumps(state, indent=2) + '\n')
        return state_file
    except BaseException:
        compose(state, 'down', '--volumes', '--remove-orphans', timeout=180)
        raise


def load(path):
    mounted()
    path = path.resolve()
    if not path.is_relative_to(FIXTURES.resolve()) or path.name != 'state.json':
        raise RuntimeError('State must belong to the verified Kafka fixture directory')
    state = json.loads(path.read_text())
    directory = path.parent
    if state['directory'] != str(directory) or state['run'] != directory.name or state['project'] != 'raddle-kafka-' + directory.name:
        raise RuntimeError('Fixture ownership receipt mismatch')
    if state['compose'] != str(directory / 'compose.json'):
        raise RuntimeError('Fixture compose path mismatch')
    for container in state['containers']:
        label = docker('inspect', '--format', '{{index .Config.Labels "raddle.kafka.fixture"}}', container, capture=True).stdout.strip()
        if label != state['run']:
            raise RuntimeError('Container does not belong to this run')
    return state


def down(path):
    state = load(path)
    try:
        logs = docker('compose', '-p', state['project'], '-f', state['compose'], 'logs', '--no-color', capture=True, timeout=60)
        (Path(state['directory']) / 'broker.log').write_text(logs.stdout + logs.stderr)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        (Path(state['directory']) / 'broker.log').write_text('Broker log capture failed; teardown still attempted.\n')
    compose(state, 'down', '--volumes', '--remove-orphans', timeout=180)
    data = Path(state['directory']) / 'data'
    if data.is_dir() and not data.is_symlink():
        shutil.rmtree(data)


def build_inputs(target, mode='broker'):
    mounted()
    target = target.resolve()
    if not target.is_relative_to(Path('/mnt/perok/build')):
        raise RuntimeError('Build output must remain below the perok build directory')
    build_directory(target.parent)
    env = os.environ.copy()
    for key in ['CARGO_TARGET_DIR', 'CARGO_BUILD_TARGET_DIR', 'CARGO_BUILD_BUILD_DIR', 'CARGO_BUILD_JOBS', 'CARGO_INCREMENTAL']:
        env.pop(key, None)
    env['TMPDIR'] = '/mnt/perok/build/scratch/raddle'
    test_target = 'crc_wire' if mode == 'crc' else 'broker'
    command = ['cargo', 'test', '-p', 'raddle-kafka', '--test', test_target, '--locked', '-j1', '--target-dir', str(target), '--config', 'profile.dev.debug=0']
    return command, env


def test(path, command, env, proxy=None):
    state = load(path)
    mode = 'crc' if proxy is not None else 'broker'
    if state.get('mode', 'broker') != mode:
        raise RuntimeError('CRC tests require the explicit live one-shot CRC fixture mode')
    if len(state['containers']) != 1:
        raise RuntimeError('Expected one owned Kafka fixture container for broker diagnostics')
    env = env.copy()
    env['RADDLE_KAFKA_TEST_BROKERS'] = state['bootstrap']
    env['RADDLE_KAFKA_TEST_CONTAINER'] = state['containers'][0]
    for key in ['RADDLE_KAFKA_CRC_CONTROL', 'RADDLE_KAFKA_CRC_TOKEN']:
        env.pop(key, None)
    if proxy is not None:
        if state['proxy'] != proxy.descriptor() or state['bootstrap'] != proxy.descriptor()['bootstrap']:
            raise RuntimeError('Live CRC proxy ownership/routing mismatch')
        env['RADDLE_KAFKA_CRC_CONTROL'] = proxy.descriptor()['control']
        env['RADDLE_KAFKA_CRC_TOKEN'] = proxy.token
    return subprocess.run([*command, '--', '--ignored', '--test-threads=1', '--show-output'], cwd=ROOT, env=env).returncode


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['up', 'down', 'test'])
    parser.add_argument('--state', type=Path)
    parser.add_argument('--mode', choices=['broker', 'crc'], default='broker')
    checkout = hashlib.sha256(str(ROOT).encode()).hexdigest()[:12]
    parser.add_argument('--target-dir', type=Path, default=Path('/mnt/perok/build/raddle/kafka') / checkout / 'target')
    args = parser.parse_args()
    if args.mode == 'crc' and (args.action != 'test' or args.state is not None):
        parser.error('CRC mode requires one-shot test without --state')
    if args.action == 'up':
        print(up(), flush=True)
        return 0
    if args.action == 'down':
        if args.state is None:
            parser.error('down requires --state')
        down(args.state)
        return 0
    mounted()
    # Acquire the compile lease before creating the one-shot broker. Native
    # fixtures must not sit idle while unrelated long builds own that lease.
    lock = build_directory(FIXTURES.parent) / 'compile.lock'
    with lock.open('a') as lease:
        fcntl.flock(lease, fcntl.LOCK_EX)
        command, env = build_inputs(args.target_dir) if args.mode == 'broker' else build_inputs(args.target_dir, args.mode)
        built = subprocess.run([*command, '--no-run'], cwd=ROOT, env=env)
        if built.returncode:
            return built.returncode
        owned = args.state is None
        proxy = WireProxy().start() if args.mode == 'crc' else None
        state = None
        try:
            state = (up() if proxy is None else up(proxy)) if owned else args.state
            result = test(state, command, env) if proxy is None else test(state, command, env, proxy)
            if proxy is not None and (proxy.errors or len(proxy.receipts) != 3 or proxy.arm is not None):
                return result or 1
            return result
        finally:
            try:
                if proxy is not None:
                    proxy.close()
                    if state is not None:
                        (state.parent / 'wire-receipt.json').write_text(json.dumps(proxy.status(), indent=2) + '\n')
            finally:
                if owned and state is not None:
                    down(state)


if __name__ == '__main__':
    sys.exit(main())
