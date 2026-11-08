"""Bounded loopback Kafka CRC fixture, not a general Kafka protocol proxy.

Only named CRC clients negotiate Fetch v4. All other traffic is byte-transparent.
Schemas: https://kafka.apache.org/41/design/protocol/ and
https://kafka.apache.org/41/implementation/message-format/ (public specifications).
"""

import asyncio
import concurrent.futures
import hashlib
import hmac
import json
import os
import secrets
import struct
import threading

MAX_FRAME = 4 * 1024 * 1024
MAX_CONNECTIONS = 48
MAX_FRAMES = 100000
PREFIX = 'raddle-crc-'


class Cursor:
    def __init__(self, data):
        self.data = data
        self.pos = 0

    def take(self, size):
        if size < 0 or self.pos + size > len(self.data):
            raise ValueError('truncated protocol field')
        start = self.pos
        self.pos += size
        return self.data[start:self.pos]

    def number(self, fmt):
        return struct.unpack('>' + fmt, self.take(struct.calcsize('>' + fmt)))[0]

    def count(self, nullable=False):
        value = self.number('i')
        if value == -1 and nullable:
            return 0
        if not 0 <= value <= 4096:
            raise ValueError('array bound')
        return value

    def varint(self):
        value = 0
        for shift in range(0, 35, 7):
            byte = self.number('B')
            value |= (byte & 127) << shift
            if byte < 128:
                return value
        raise ValueError('varint bound')

    def tags(self):
        count = self.varint()
        if count > 128:
            raise ValueError('tag bound')
        for _ in range(count):
            self.varint()
            self.take(self.varint())

    def string(self):
        return self.take(self.number('h')).decode('utf8')

    def end(self):
        if self.pos != len(self.data):
            raise ValueError('unexpected protocol tail')


def request_header(frame):
    cursor = Cursor(frame)
    key, version, correlation = cursor.number('h'), cursor.number('h'), cursor.number('i')
    length = cursor.number('h')
    client = '' if length == -1 else cursor.take(length).decode('utf8')
    if len(client) > 256:
        raise ValueError('client identifier bound')
    return key, version, correlation, client


def cap_fetch(frame, version):
    # ApiVersions always uses response header v0, including flexible v3/v4.
    if version not in (0, 1, 2, 3, 4):
        raise ValueError('unsupported ApiVersions schema')
    cursor = Cursor(frame)
    cursor.take(4)
    if cursor.number('h') != 0:
        raise ValueError('ApiVersions error')
    flexible = version >= 3
    count = cursor.varint() - 1 if flexible else cursor.count()
    if not 0 <= count <= 256:
        raise ValueError('API array bound')
    output = bytearray(frame)
    supported = None
    for _ in range(count):
        key, minimum = cursor.number('h'), cursor.number('h')
        maximum_position = cursor.pos
        maximum = cursor.number('h')
        if key == 1:
            if supported is not None or not minimum <= 4 <= maximum:
                raise ValueError('broker does not offer Fetch v4')
            supported = (minimum, maximum)
            struct.pack_into('>h', output, maximum_position, 4)
        if flexible:
            cursor.tags()
    if version >= 1:
        cursor.take(4)  # throttle_time_ms
    if flexible:
        cursor.tags()
    cursor.end()
    if supported is None:
        raise ValueError('Fetch API missing')
    return bytes(output), supported


def crc32c(data):
    crc = 0xffffffff
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ (0x82f63b78 if crc & 1 else 0)
    return crc ^ 0xffffffff


def corrupt_fetch(frame, target):
    """Change exactly one bit in one valid magic-2 batch's CRC, never its body."""
    cursor = Cursor(frame)
    correlation = cursor.number('i')
    cursor.take(4)  # throttle
    found = None
    for _ in range(cursor.count()):
        topic = cursor.string()
        for _ in range(cursor.count()):
            partition, error = cursor.number('i'), cursor.number('h')
            cursor.take(16)  # high watermark and last stable offset
            cursor.take(cursor.count(nullable=True) * 16)  # aborted transactions
            size = cursor.number('i')
            if size == -1:
                continue
            start = cursor.pos
            records = Cursor(cursor.take(size))
            if topic != target['topic'] or partition != target['partition']:
                continue
            if error:
                raise ValueError('target partition Fetch error')
            while records.pos < len(records.data):
                batch_start = records.pos
                base = records.number('q')
                length = records.number('i')
                batch = records.take(length)
                if length < 49 or batch[4] != 2:
                    raise ValueError('target requires complete magic-2 batches')
                if base != target['offset']:
                    continue
                if found is not None:
                    raise ValueError('duplicate target batch')
                before = struct.unpack('>I', batch[5:9])[0]
                if before != crc32c(batch[9:]):
                    raise ValueError('original batch CRC was not valid')
                if struct.unpack('>i', batch[45:49])[0] != 1:
                    raise ValueError('target must be a separate single-record batch')
                position = start + batch_start + 17
                found = {
                    'correlation': correlation, 'topic': topic, 'partition': partition,
                    'base_offset': base, 'fetch_version': 4, 'magic': 2,
                    'crc_position': position, 'crc_before': before, 'crc_after': before ^ 0x01000000,
                    'frame_bytes': len(frame), 'batch_bytes': length + 12,
                    'body_sha256': hashlib.sha256(batch[9:]).hexdigest(),
                    'changed_bytes': 1, 'changed_bits': 1,
                }
    cursor.end()
    if found is None:
        return frame, None
    output = bytearray(frame)
    output[found['crc_position']] ^= 1
    return bytes(output), found


class WireProxy:
    """One in-process run-owned proxy with authenticated, bounded control frames."""

    def __init__(self):
        self.token = secrets.token_hex(32)
        self.backend = None
        self.arm = None
        self.receipts = []
        self.negotiations = []
        self.fetches = 0
        self.frames = 0
        self.errors = []
        self.connections = set()
        self.tasks = set()
        self.closed = False
        self.ready = concurrent.futures.Future()
        self.thread = threading.Thread(target=self._thread, name='kafka-crc-wire')

    def start(self):
        self.thread.start()
        self.ready.result(timeout=10)
        return self

    def _thread(self):
        try:
            asyncio.run(self._serve())
        except BaseException as error:
            if not self.ready.done():
                self.ready.set_exception(error)
            else:
                self.errors.append(type(error).__name__)

    async def _serve(self):
        self.loop = asyncio.get_running_loop()
        self.stop = asyncio.Event()
        self.server = await asyncio.start_server(self._connection, '127.0.0.1', 0, limit=MAX_FRAME + 4)
        self.control = await asyncio.start_server(self._control, '127.0.0.1', 0, limit=4096)
        self.port = self.server.sockets[0].getsockname()[1]
        self.control_port = self.control.sockets[0].getsockname()[1]
        self.ready.set_result(None)
        await self.stop.wait()
        self.server.close()
        self.control.close()
        await self.server.wait_closed()
        await self.control.wait_closed()
        for writer in list(self.connections):
            writer.close()
        for task in list(self.tasks):
            task.cancel()
        await asyncio.gather(*list(self.tasks), return_exceptions=True)
        self.closed = True

    def close(self):
        self.loop.call_soon_threadsafe(self.stop.set)
        self.thread.join(timeout=20)
        if self.thread.is_alive() or not self.closed:
            raise RuntimeError('owned wire proxy did not stop')

    def descriptor(self):
        return {'pid': os.getpid(), 'bootstrap': f'127.0.0.1:{self.port}',
                'control': f'127.0.0.1:{self.control_port}'}

    def status(self):
        return {'receipts': self.receipts, 'negotiations': self.negotiations,
                'fetches': self.fetches, 'frames': self.frames, 'errors': self.errors,
                'armed': self.arm is not None}

    def command(self, request):
        if not hmac.compare_digest(str(request.get('token', '')), self.token):
            return {'error': 'unauthorized'}
        operation = request.get('op')
        if operation == 'arm':
            target = request.get('target', {})
            if (self.arm is not None or len(self.receipts) >= 8
                    or not isinstance(target.get('client'), str)
                    or not target['client'].startswith(PREFIX) or len(target['client']) > 100
                    or not isinstance(target.get('topic'), str) or not 1 <= len(target['topic']) <= 249
                    or target.get('partition') != 0
                    or type(target.get('offset')) is not int or not 0 <= target['offset'] < 2**63):
                return {'error': 'invalid or overlapping arm'}
            self.arm = {key: target[key] for key in ('client', 'topic', 'partition', 'offset')}
        elif operation == 'disarm':
            self.arm = None
        elif operation != 'status':
            return {'error': 'unknown operation'}
        return self.status()

    async def _control(self, reader, writer):
        task = asyncio.current_task()
        if len(self.tasks) >= MAX_CONNECTIONS * 2:
            writer.close()
            return
        self.tasks.add(task)
        try:
            async with asyncio.timeout(5):
                line = await reader.readline()
                request = json.loads(line)
                response = self.command(request) if isinstance(request, dict) else {'error': 'object required'}
                writer.write(json.dumps(response).encode() + b'\n')
                await writer.drain()
        except (ValueError, TimeoutError, ConnectionError):
            pass
        finally:
            writer.close()
            self.tasks.discard(task)

    async def _frame(self, reader):
        async with asyncio.timeout(90):
            size = struct.unpack('>i', await reader.readexactly(4))[0]
            if not 4 <= size <= MAX_FRAME or self.frames >= MAX_FRAMES:
                raise ValueError('wire frame bound')
            self.frames += 1
            return await reader.readexactly(size)

    async def _send(self, writer, frame):
        writer.write(struct.pack('>i', len(frame)) + frame)
        async with asyncio.timeout(10):
            await writer.drain()

    async def _connection(self, reader, writer):
        task = asyncio.current_task()
        if len(self.connections) >= MAX_CONNECTIONS or self.backend is None:
            writer.close()
            return
        self.tasks.add(task)
        self.connections.add(writer)
        backend_writer = None
        directions = []
        try:
            async with asyncio.timeout(10):
                backend_reader, backend_writer = await asyncio.open_connection('127.0.0.1', self.backend)
            pending = {}

            async def requests():
                while True:
                    frame = await self._frame(reader)
                    key, version, correlation, client = request_header(frame)
                    if len(pending) >= 128 or correlation in pending:
                        raise ValueError('inflight request bound')
                    pending[correlation] = (key, version, client)
                    if key == 1 and client.startswith(PREFIX):
                        if version != 4:
                            raise ValueError('named CRC client did not negotiate Fetch v4')
                        self.fetches += 1
                    await self._send(backend_writer, frame)

            async def responses():
                while True:
                    frame = await self._frame(backend_reader)
                    correlation = struct.unpack('>i', frame[:4])[0]
                    key, version, client = pending.pop(correlation)
                    if key == 18 and client.startswith(PREFIX):
                        frame, supported = cap_fetch(frame, version)
                        receipt = {'client': client, 'api_versions_version': version,
                                   'broker_fetch_min': supported[0], 'broker_fetch_max': supported[1], 'capped_fetch_max': 4}
                        if receipt not in self.negotiations:
                            if len(self.negotiations) >= 32:
                                raise ValueError('negotiation receipt bound')
                            self.negotiations.append(receipt)
                    if key == 1 and self.arm is not None and self.arm['client'] == client:
                        if version != 4:
                            raise ValueError('unsupported armed Fetch version')
                        frame, receipt = corrupt_fetch(frame, self.arm)
                        if receipt is not None:
                            receipt['client'] = client
                            self.receipts.append(receipt)
                            self.arm = None
                    await self._send(writer, frame)

            directions = [asyncio.create_task(requests()), asyncio.create_task(responses())]
            done, _ = await asyncio.wait(directions, return_when=asyncio.FIRST_COMPLETED)
            for direction in done:
                direction.result()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass  # Ordinary client destruction/EOF is not an injected fault.
        except (ValueError, KeyError, TimeoutError) as error:
            if len(self.errors) < 32:
                self.errors.append(type(error).__name__ + ': ' + str(error)[:120])
        finally:
            for direction in directions:
                direction.cancel()
            await asyncio.gather(*directions, return_exceptions=True)
            writer.close()
            if backend_writer is not None:
                backend_writer.close()
            self.connections.discard(writer)
            self.tasks.discard(task)
