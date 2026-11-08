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


if __name__ == '__main__':
    unittest.main()
