"""Build-lane mount gates without Docker or writes outside a mocked target."""

import importlib.util
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest import mock


spec = importlib.util.spec_from_file_location('raddle_stream_fixture', Path(__file__).with_name('run.py'))
fixture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixture)


class TargetContracts(unittest.TestCase):
    def test_outside_build_path_is_refused_before_mount_probe_or_write(self):
        with mock.patch.object(fixture, 'command') as command, \
                mock.patch.object(Path, 'mkdir') as mkdir:
            with self.assertRaises(RuntimeError):
                fixture.target_directory(Path('/tmp/not-a-build-lane'))
            command.assert_not_called()
            mkdir.assert_not_called()

    def test_unmounted_target_ancestor_is_refused_before_creating_directories(self):
        with mock.patch.object(Path, 'exists', lambda value: str(value) == '/mnt/perok/build'), \
                mock.patch.object(fixture, 'command', return_value=SimpleNamespace(stdout='/dev/other ext4\n')), \
                mock.patch.object(Path, 'mkdir') as mkdir:
            with self.assertRaises(RuntimeError):
                fixture.target_directory(Path('/mnt/perok/build/new-checkout/target'))
            mkdir.assert_not_called()

    def test_verified_ancestor_allows_only_the_selected_parent(self):
        with mock.patch.object(Path, 'exists', lambda value: str(value) == '/mnt/perok/build'), \
                mock.patch.object(fixture, 'command', return_value=SimpleNamespace(stdout='/dev/sdb1 btrfs\n')) as command, \
                mock.patch.object(Path, 'mkdir', autospec=True) as mkdir:
            target = fixture.target_directory(Path('/mnt/perok/build/new-checkout/target'))
            self.assertEqual(target, Path('/mnt/perok/build/new-checkout/target'))
            command.assert_called_once_with(['findmnt', '-n', '-o', 'SOURCE,FSTYPE', '-T', '/mnt/perok/build'])
            mkdir.assert_called_once_with(Path('/mnt/perok/build/new-checkout'), parents=True, exist_ok=True)


class Peer:
    def __init__(self, response):
        self.response = response
        self.sent = []

    def recv(self, size):
        # Exercise fragmented protocol reads, not just one ready-made packet.
        part, self.response = self.response[:min(size, 2)], self.response[min(size, 2):]
        return part

    def sendall(self, data):
        self.sent.append(data)


class ReadinessContracts(unittest.TestCase):
    def test_open_proxy_without_protocol_response_is_not_ready(self):
        for family in ['redis', 'nats', 'mqtt']:
            with self.subTest(family=family), self.assertRaises(OSError):
                fixture.broker_ready(family, Peer(b''))

    def test_mqtt_success_requires_positive_connack_then_disconnects(self):
        peer = Peer(b'\x20\x03\x00\x00\x00')
        fixture.broker_ready('mqtt', peer)
        self.assertIn(b'MQTT\x05', peer.sent[0])
        self.assertEqual(peer.sent[-1], b'\xe0\x00')

    def test_mqtt_refusal_truncation_and_oversized_declared_body_are_not_ready(self):
        for reply in [b'\x20\x03\x00\x87\x00', b'\x20\x03\x00', b'\x20\x81\x80\x01',
                      b'\x20\x80\x80\x80\x80', b'\x30\x00']:
            with self.subTest(reply=reply), self.assertRaises(OSError):
                fixture.broker_ready('mqtt', Peer(reply))

    def test_nats_and_redis_require_real_protocol_responses(self):
        fixture.broker_ready('redis', Peer(b'+PONG\r\n'))
        fixture.broker_ready('nats', Peer(b'INFO {"server_id":"fixture"}\r\n'))
        for family, reply in [('redis', b'-NOAUTH'), ('nats', b'HTTP/1.1 200 OK\r\n'),
                              ('nats', b'INFO {}\r\n')]:
            with self.subTest(family=family, reply=reply), self.assertRaises(OSError):
                fixture.broker_ready(family, Peer(reply))


if __name__ == '__main__':
    unittest.main()
