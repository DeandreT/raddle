"""CRC parser, fault scope, bounds and live loopback proxy lifecycle guards."""

import json
import socket
import struct
import threading
import unittest

from wire import MAX_FRAME, WireProxy, cap_fetch, corrupt_fetch, crc32c, request_header


def batch(offset=2, value=b'unchanged-record', count=1):
    body = struct.pack('>hiqqqhii', 0, 0, 0, 0, -1, -1, -1, count) + value
    content = struct.pack('>iBI', 0, 2, crc32c(body)) + body
    return struct.pack('>qi', offset, len(content)) + content


def fetch(records=None):
    records = batch() if records is None else records
    topic = b'owned-topic'
    return (struct.pack('>iiih', 7, 0, 1, len(topic)) + topic
            + struct.pack('>iihqqii', 1, 0, 0, 3, 3, -1, len(records)) + records)


def versions():
    # Flexible ApiVersions v3, two APIs, empty tags on each struct.
    return struct.pack('>ihB', 7, 0, 3) + struct.pack('>hhhBhhhB', 1, 4, 18, 0, 3, 0, 12, 0) + struct.pack('>iB', 0, 0)


def header(key, version, client):
    client = client.encode()
    return struct.pack('>hhih', key, version, 7, len(client)) + client


def exchange(stream, frame):
    stream.sendall(struct.pack('>i', len(frame)) + frame)
    def exact(size):
        result = b''
        while len(result) < size:
            chunk = stream.recv(size - len(result))
            if not chunk:
                raise EOFError('unexpected fixture socket EOF')
            result += chunk
        return result
    return exact(struct.unpack('>i', exact(4))[0])


class WireContracts(unittest.TestCase):
    target = {'client': 'raddle-crc-owned', 'topic': 'owned-topic', 'partition': 0, 'offset': 2}

    def test_castagnoli_known_vector_and_exact_single_crc_bit_change(self):
        self.assertEqual(crc32c(b'123456789'), 0xe3069283)
        original = fetch()
        mutated, receipt = corrupt_fetch(original, self.target)
        changes = [i for i, (a, b) in enumerate(zip(original, mutated)) if a != b]
        self.assertEqual(changes, [receipt['crc_position']])
        self.assertEqual(original[changes[0]] ^ mutated[changes[0]], 1)
        self.assertEqual(receipt['crc_before'] ^ receipt['crc_after'], 0x01000000)
        self.assertEqual(len(original), len(mutated))
        self.assertEqual(original[changes[0] + 4:], mutated[changes[0] + 4:])
        with self.assertRaisesRegex(ValueError, 'original batch CRC'):
            corrupt_fetch(mutated, self.target)

    def test_only_exact_topic_partition_base_offset_and_single_batch_can_mutate(self):
        for key, wrong in [('topic', 'unowned'), ('partition', 1), ('offset', 3)]:
            frame = fetch()
            self.assertEqual(corrupt_fetch(frame, dict(self.target, **{key: wrong})), (frame, None))
        frame = fetch(batch(1) + batch(2) + batch(3))
        mutated, receipt = corrupt_fetch(frame, self.target)
        self.assertEqual(receipt['base_offset'], 2)
        self.assertEqual(sum(a != b for a, b in zip(frame, mutated)), 1)
        with self.assertRaisesRegex(ValueError, 'separate single-record'):
            corrupt_fetch(fetch(batch(count=2)), self.target)

    def test_truncation_tail_array_and_unsupported_schema_fail_closed(self):
        for frame in [fetch()[:-1], fetch() + b'extra', struct.pack('>iii', 7, 0, 5000)]:
            with self.assertRaises(ValueError):
                corrupt_fetch(frame, self.target)
        with self.assertRaises(ValueError):
            cap_fetch(versions(), 5)
        with self.assertRaises(ValueError):
            cap_fetch(versions().replace(struct.pack('>hhh', 1, 4, 18), struct.pack('>hhh', 1, 5, 18)), 3)

    def test_api_negotiation_changes_only_fetch_maximum_and_preserves_header(self):
        frame = versions()
        capped, supported = cap_fetch(frame, 3)
        self.assertEqual(supported, (4, 18))
        self.assertEqual(sum(a != b for a, b in zip(frame, capped)), 1)
        self.assertEqual(capped[11:13], struct.pack('>h', 4))
        self.assertEqual(request_header(header(1, 4, 'raddle-crc-owned')), (1, 4, 7, 'raddle-crc-owned'))

    def test_control_auth_scope_overlap_disarm_and_receipt_bound(self):
        proxy = WireProxy()
        self.assertEqual(proxy.command({'op': 'arm', 'target': self.target}), {'error': 'unauthorized'})
        command = {'token': proxy.token, 'op': 'arm', 'target': self.target}
        self.assertIn('error', proxy.command(dict(command, target=dict(self.target, client='ordinary'))))
        self.assertTrue(proxy.command(command)['armed'])
        self.assertIn('error', proxy.command(command))
        self.assertFalse(proxy.command({'token': proxy.token, 'op': 'disarm'})['armed'])
        proxy.receipts = [{}] * 8
        self.assertIn('error', proxy.command(command))

    def test_real_loopback_transparency_named_scope_and_cleanup(self):
        with socket.socket() as backend:
            backend.bind(('127.0.0.1', 0))
            backend.listen(1)
            backend.settimeout(5)
            proxy = WireProxy().start()
            proxy.backend = backend.getsockname()[1]
            failures = []

            def serve():
                try:
                    with backend.accept()[0] as stream:
                        stream.settimeout(5)
                        for response in [versions(), fetch(), fetch()]:
                            size = struct.unpack('>i', stream.recv(4, socket.MSG_WAITALL))[0]
                            stream.recv(size, socket.MSG_WAITALL)
                            stream.sendall(struct.pack('>i', len(response)) + response)
                except BaseException as error:
                    failures.append(error)

            thread = threading.Thread(target=serve)
            thread.start()
            try:
                with socket.create_connection(('127.0.0.1', proxy.port), timeout=5) as stream:
                    self.assertNotEqual(exchange(stream, header(18, 3, 'raddle-crc-owned')), versions())
                    self.assertTrue(proxy.command({'token': proxy.token, 'op': 'arm', 'target': self.target})['armed'])
                    self.assertEqual(exchange(stream, header(1, 4, 'ordinary')), fetch())
                    self.assertNotEqual(exchange(stream, header(1, 4, 'raddle-crc-owned')), fetch())
                with socket.create_connection(('127.0.0.1', proxy.control_port), timeout=5) as stream:
                    stream.sendall(b'{"op":"status","token":"wrong"}\n')
                    self.assertEqual(json.loads(stream.makefile().readline()), {'error': 'unauthorized'})
            finally:
                thread.join(timeout=10)
                proxy.close()
            self.assertFalse(thread.is_alive())
            self.assertEqual(failures, [])
            self.assertFalse(proxy.thread.is_alive())
            self.assertTrue(proxy.closed)
            self.assertEqual(proxy.errors, [])
            self.assertEqual(len(proxy.receipts), 1)
            self.assertFalse(proxy.arm)
            self.assertFalse(proxy.connections)
            for port in [proxy.port, proxy.control_port]:
                with self.assertRaises(OSError):
                    socket.create_connection(('127.0.0.1', port), timeout=1)

    def test_frame_bound_is_finite(self):
        self.assertLessEqual(MAX_FRAME, 4 * 1024 * 1024)


if __name__ == '__main__':
    unittest.main()
