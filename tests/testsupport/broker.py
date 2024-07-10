# Copyright (C) 2021
#
# This program and the accompanying materials are made available under the
# terms of the Eclipse Public License 2.0 which is available at
# http://www.eclipse.org/legal/epl-2.0.
#
# SPDX-License-Identifier: EPL-2.0
#
# This Source Code may also be made available under the following Secondary
# Licenses when the conditions for such availability set forth in the Eclipse
# Public License, v. 2.0 are satisfied: GNU General Public License as published
# by the Free Software Foundation, either version 2 of the License, or (at your
# option) any later version, with the GNU Classpath Exception which is available
# at https://www.gnu.org/software/classpath/license.html.

import contextlib
import socket
import socketserver
import threading

import pytest

from tests.testsupport import paho_test


class FakeBroker:
    def __init__(self):
        # Bind to "localhost" for maximum performance, as described in:
        # http://docs.python.org/howto/sockets.html#ipc
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.settimeout(5)
        sock.bind(("localhost", 0))
        self.port = sock.getsockname()[1]
        sock.listen(1)

        self._sock = sock
        self._conn = None

    def start(self):
        if self._sock is None:
            raise ValueError("Socket is not open")

        (conn, address) = self._sock.accept()
        conn.settimeout(5)
        self._conn = conn

    def finish(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None

        if self._sock is not None:
            self._sock.close()
            self._sock = None

    def receive_packet(self, num_bytes):
        if self._conn is None:
            raise ValueError("Connection is not open")

        packet_in = self._conn.recv(num_bytes)
        return packet_in

    def send_packet(self, packet_out):
        if self._conn is None:
            raise ValueError("Connection is not open")

        count = self._conn.send(packet_out)
        return count

    def expect_packet(self, name, packet):
        if self._conn is None:
            raise ValueError("Connection is not open")

        paho_test.expect_packet(self._conn, name, packet)


@pytest.fixture
def fake_broker():
    broker = FakeBroker()

    yield broker

    broker.finish()


class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    pass


class FakeWebsocketBroker(threading.Thread):
    def __init__(self):
        super().__init__()

        self.host = "localhost"
        self.port = -1  # Will be set by `serve()`

        self._server = None
        self._running = True
        self.handler_cls = False

    @contextlib.contextmanager
    def serve(self, tcphandler):
        self._server = ThreadedTCPServer((self.host, 0), tcphandler)

        try:
            self.start()
            self.port = self._server.server_address[1]

            if not self._running:
                raise RuntimeError("Error starting server")
            yield
        finally:
            if self._server:
                self._server.shutdown()
                self._server.server_close()

    def run(self):
        self._running = True
        self._server.serve_forever()


@pytest.fixture
def fake_websocket_broker():
    broker = FakeWebsocketBroker()

    yield broker
