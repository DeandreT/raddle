"""Read-only ownership gates and run-local fixture cleanup contracts."""

import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock


spec = importlib.util.spec_from_file_location('raddle_kafka_fixture', Path(__file__).with_name('run.py'))
fixture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixture)


class FixtureContracts(unittest.TestCase):
    def state(self, root):
        directory = root / ('a' * 32)
        directory.mkdir()
        state = {'run': directory.name, 'project': 'raddle-kafka-' + directory.name,
                 'directory': str(directory), 'compose': str(directory / 'compose.json'),
                 'containers': []}
        path = directory / 'state.json'
        path.write_text(json.dumps(state))
        return path, state

    def test_wrong_mount_fails_before_any_docker_operation(self):
        with mock.patch.object(fixture.subprocess, 'check_output', return_value='/dev/other ext4\n'), \
                mock.patch.object(fixture, 'docker') as docker:
            with self.assertRaises(RuntimeError):
                fixture.mounted()
            docker.assert_not_called()

    def test_digest_resolution_selects_only_matching_linux_architecture(self):
        def entry(architecture, digit):
            return {'Descriptor': {'platform': {'os': 'linux', 'architecture': architecture},
                                   'digest': 'sha256:' + digit * 64}}
        manifest = json.dumps([entry('arm64', 'a'), entry('amd64', 'b')])
        with mock.patch.object(fixture.platform, 'machine', return_value='x86_64'), \
                mock.patch.object(fixture, 'docker', return_value=SimpleNamespace(stdout=manifest)):
            self.assertEqual(fixture.resolve_image(), 'apache/kafka@sha256:' + 'b' * 64)

    def test_outside_state_and_inconsistent_ownership_are_refused(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path, state = self.state(root)
            with mock.patch.object(fixture, 'mounted'), mock.patch.object(fixture, 'FIXTURES', root), \
                    mock.patch.object(fixture, 'docker') as docker:
                with self.assertRaises(RuntimeError):
                    fixture.load(root.parent / 'state.json')
                state['project'] = 'someone-else'
                path.write_text(json.dumps(state))
                with self.assertRaises(RuntimeError):
                    fixture.load(path)
                docker.assert_not_called()

    def test_mismatching_container_label_never_grants_cleanup_authority(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path, state = self.state(root)
            state['containers'] = ['captured-container']
            path.write_text(json.dumps(state))
            with mock.patch.object(fixture, 'mounted'), mock.patch.object(fixture, 'FIXTURES', root), \
                    mock.patch.object(fixture, 'docker', return_value=SimpleNamespace(stdout='different-run\n')):
                with self.assertRaises(RuntimeError):
                    fixture.load(path)

    def test_log_failure_does_not_skip_owned_teardown(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path, state = self.state(root)
            data = path.parent / 'data'
            data.mkdir()
            (data / 'synthetic-record').write_text('fixture-only')
            with mock.patch.object(fixture, 'load', return_value=state), \
                    mock.patch.object(fixture, 'docker', side_effect=subprocess.CalledProcessError(1, ['docker'])), \
                    mock.patch.object(fixture, 'compose') as compose:
                fixture.down(path)
                compose.assert_called_once_with(state, 'down', '--volumes', '--remove-orphans', timeout=180)
            self.assertFalse(data.exists())
            self.assertIn('teardown still attempted', (path.parent / 'broker.log').read_text())

    def test_failed_container_teardown_preserves_data_for_recovery(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path, state = self.state(root)
            data = path.parent / 'data'
            data.mkdir()
            with mock.patch.object(fixture, 'load', return_value=state), \
                    mock.patch.object(fixture, 'docker', return_value=SimpleNamespace(stdout='', stderr='')), \
                    mock.patch.object(fixture, 'compose', side_effect=subprocess.CalledProcessError(1, ['docker'])):
                with self.assertRaises(subprocess.CalledProcessError):
                    fixture.down(path)
            self.assertTrue(data.exists())

    def test_failed_compilation_never_starts_a_broker(self):
        with tempfile.TemporaryDirectory() as temporary:
            with mock.patch.object(fixture.sys, 'argv', ['run.py', 'test']), \
                    mock.patch.object(fixture, 'mounted'), \
                    mock.patch.object(fixture, 'build_directory', return_value=Path(temporary)), \
                    mock.patch.object(fixture.fcntl, 'flock') as lock, \
                    mock.patch.object(fixture, 'build_inputs', return_value=(['cargo', 'test'], {})), \
                    mock.patch.object(fixture.subprocess, 'run', return_value=SimpleNamespace(returncode=17)), \
                    mock.patch.object(fixture, 'up') as up:
                self.assertEqual(fixture.main(), 17)
                lock.assert_called_once()
                up.assert_not_called()

    def test_broker_diagnostics_receive_only_the_verified_owned_container(self):
        state = {'containers': ['a' * 64], 'bootstrap': '127.0.0.1:29092'}
        inherited = {'INHERITED': 'preserved'}
        with mock.patch.object(fixture, 'load', return_value=state), \
                mock.patch.object(fixture.subprocess, 'run', return_value=SimpleNamespace(returncode=0)) as run:
            self.assertEqual(fixture.test(Path('verified-state'), ['cargo', 'test'], inherited), 0)
            env = run.call_args.kwargs['env']
            self.assertEqual(env['RADDLE_KAFKA_TEST_CONTAINER'], 'a' * 64)
            self.assertEqual(env['RADDLE_KAFKA_TEST_BROKERS'], state['bootstrap'])
            self.assertEqual(inherited, {'INHERITED': 'preserved'})
        with mock.patch.object(fixture, 'load', return_value={'containers': []}), \
                mock.patch.object(fixture.subprocess, 'run') as run:
            with self.assertRaises(RuntimeError):
                fixture.test(Path('verified-state'), ['cargo', 'test'], inherited)
            run.assert_not_called()

    def test_one_shot_owns_lease_before_build_and_fixture_lifecycle(self):
        events = []
        with tempfile.TemporaryDirectory() as temporary:
            with mock.patch.object(fixture.sys, 'argv', ['run.py', 'test']), \
                    mock.patch.object(fixture, 'mounted'), \
                    mock.patch.object(fixture, 'build_directory', return_value=Path(temporary)), \
                    mock.patch.object(fixture.fcntl, 'flock', side_effect=lambda *args: events.append('lease')), \
                    mock.patch.object(fixture, 'build_inputs', return_value=(['cargo', 'test'], {})), \
                    mock.patch.object(fixture.subprocess, 'run', side_effect=lambda *args, **kwargs: events.append('build') or SimpleNamespace(returncode=0)), \
                    mock.patch.object(fixture, 'up', side_effect=lambda: events.append('up') or Path('owned-state')), \
                    mock.patch.object(fixture, 'test', side_effect=lambda *args: events.append('test') or 0), \
                    mock.patch.object(fixture, 'down', side_effect=lambda *args: events.append('down')):
                self.assertEqual(fixture.main(), 0)
        self.assertEqual(events, ['lease', 'build', 'up', 'test', 'down'])

    def test_crc_cli_cannot_reuse_an_inert_or_retained_fixture(self):
        for arguments in [['up', '--mode', 'crc'], ['test', '--mode', 'crc', '--state', 'old-state']]:
            with mock.patch.object(fixture.sys, 'argv', ['run.py', *arguments]), \
                    mock.patch.object(fixture, 'up') as up:
                with self.assertRaises(SystemExit):
                    fixture.main()
                up.assert_not_called()
        with mock.patch.object(fixture, 'load', return_value={'containers': ['owned'], 'mode': 'crc'}), \
                mock.patch.object(fixture.subprocess, 'run') as run:
            with self.assertRaises(RuntimeError):
                fixture.test(Path('inert'), ['cargo', 'test'], {})
            run.assert_not_called()

    def test_crc_advertises_only_proxy_while_docker_maps_backend(self):
        with tempfile.TemporaryDirectory() as temporary:
            proxy = SimpleNamespace(port=23456, backend=None, descriptor=lambda: {'bootstrap': '127.0.0.1:23456', 'pid': 123, 'control': '127.0.0.1:23457'})
            with mock.patch.object(fixture, 'mounted'), \
                    mock.patch.object(fixture, 'build_directory'), \
                    mock.patch.object(fixture, 'FIXTURES', Path(temporary)), \
                    mock.patch.object(fixture, 'resolve_image', return_value='apache/kafka@sha256:' + 'a' * 64), \
                    mock.patch.object(fixture.shutil, 'disk_usage', return_value=SimpleNamespace(free=10 * 1024**3)), \
                    mock.patch.object(fixture, 'compose'), \
                    mock.patch.object(fixture, 'docker', return_value=SimpleNamespace(stdout='owned-container\n')):
                path = fixture.up(proxy)
            state = json.loads(path.read_text())
            config = json.loads(Path(state['compose']).read_text())['services']['broker']
            self.assertEqual(state['bootstrap'], '127.0.0.1:23456')
            self.assertEqual(state['mode'], 'crc')
            self.assertEqual(config['ports'], [f'127.0.0.1:{proxy.backend}:29092'])
            self.assertEqual(config['environment']['KAFKA_ADVERTISED_LISTENERS'], 'INTERNAL://broker:9092,EXTERNAL://127.0.0.1:23456')
            self.assertEqual(config['labels']['raddle.kafka.fixture'], state['run'])

    def test_test_target_selection_and_control_credentials_are_mode_scoped(self):
        with tempfile.TemporaryDirectory() as temporary:
            with mock.patch.object(fixture, 'mounted'), mock.patch.object(fixture, 'build_directory'):
                ordinary, _ = fixture.build_inputs(Path('/mnt/perok/build/ordinary'))
                crc, _ = fixture.build_inputs(Path('/mnt/perok/build/crc'), 'crc')
            self.assertEqual(ordinary[ordinary.index('--test') + 1], 'broker')
            self.assertEqual(crc[crc.index('--test') + 1], 'crc_wire')
        state = {'containers': ['owned'], 'bootstrap': '127.0.0.1:29092'}
        with mock.patch.object(fixture, 'load', return_value=state), \
                mock.patch.object(fixture.subprocess, 'run', return_value=SimpleNamespace(returncode=0)) as run:
            fixture.test(Path('owned'), ['cargo'], {'RADDLE_KAFKA_CRC_CONTROL': 'inherited', 'RADDLE_KAFKA_CRC_TOKEN': 'inherited'})
            self.assertNotIn('RADDLE_KAFKA_CRC_CONTROL', run.call_args.kwargs['env'])
            self.assertNotIn('RADDLE_KAFKA_CRC_TOKEN', run.call_args.kwargs['env'])


if __name__ == '__main__':
    unittest.main()
