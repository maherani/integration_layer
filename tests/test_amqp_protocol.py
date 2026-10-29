# -*- coding: utf-8 -*-
"""Unit and protocol tests for AMQP 0-9-1 client."""

import socket
import struct
import threading
import time
import unittest

from amqp_client import (
    AMQPClient,
    PublishStatus,
    CLASS_CONNECTION,
    METHOD_CONNECTION_START,
    CLASS_BASIC,
    METHOD_BASIC_RETURN,
    METHOD_BASIC_ACK,
    METHOD_BASIC_NACK,
    FRAME_METHOD,
    FRAME_HEADER,
    FRAME_BODY,
    FRAME_END,
    build_frame,
    pack_short_string,
    pack_long_string,
)

class MockAMQPServer:
    """Mock AMQP broker running on a socket pair to verify protocol frames."""

    def __init__(self, return_behavior: str = "ack"):
        self.return_behavior = return_behavior # "ack", "return", "nack", "disconnect"
        self.client_sock, self.server_sock = socket.socketpair()
        self.thread = threading.Thread(target=self._run_server, daemon=True)
        self.thread.start()

    def _recv_exact(self, count: int) -> bytes:
        buf = bytearray()
        while len(buf) < count:
            chunk = self.server_sock.recv(count - len(buf))
            if not chunk:
                raise ConnectionResetError()
            buf.extend(chunk)
        return bytes(buf)

    def _read_frame(self):
        hdr = self._recv_exact(7)
        ft, ch, size = struct.unpack(">BHI", hdr)
        payload = self._recv_exact(size)
        end = self._recv_exact(1)
        return ft, ch, payload

    def _send_frame(self, frame_type: int, channel: int, payload: bytes):
        self.server_sock.sendall(build_frame(frame_type, channel, payload))

    def _run_server(self):
        try:
            # 1. Read Protocol Header
            hdr = self._recv_exact(8)
            if hdr != b"AMQP\x00\x00\x09\x01":
                return

            # 2. Send Connection.Start (10, 10)
            start_payload = (
                struct.pack(">HH", 10, 10)
                + b"\x00\x09" # version-major=0, minor=9
                + struct.pack(">I", 0) # empty server_properties table
                + pack_long_string(b"PLAIN") # mechanisms
                + pack_long_string(b"en_US") # locales
            )
            self._send_frame(FRAME_METHOD, 0, start_payload)

            # 3. Read Connection.StartOk (10, 11)
            ft, ch, payload = self._read_frame()

            # 4. Send Connection.Tune (10, 30)
            tune_payload = struct.pack(">HHHII", 10, 30, 256, 131072, 60)
            self._send_frame(FRAME_METHOD, 0, tune_payload)

            # 5. Read Connection.TuneOk (10, 31)
            ft, ch, payload = self._read_frame()

            # 6. Read Connection.Open (10, 40)
            ft, ch, payload = self._read_frame()

            # 7. Send Connection.OpenOk (10, 41)
            open_ok_payload = struct.pack(">HH", 10, 41) + b"\x00"
            self._send_frame(FRAME_METHOD, 0, open_ok_payload)

            # 8. Read Channel.Open (20, 10)
            ft, ch, payload = self._read_frame()

            # 9. Send Channel.OpenOk (20, 11)
            ch_open_ok = struct.pack(">HH", 20, 11) + struct.pack(">I", 0)
            self._send_frame(FRAME_METHOD, ch, ch_open_ok)

            # 10. Read Confirm.Select (85, 10)
            ft, ch, payload = self._read_frame()

            # 11. Send Confirm.SelectOk (85, 11)
            confirm_select_ok = struct.pack(">HH", 85, 11)
            self._send_frame(FRAME_METHOD, ch, confirm_select_ok)

            # Now handle Basic.Publish messages
            delivery_tag = 1
            while True:
                # Expect Method frame (Basic.Publish)
                ft_m, ch_m, payload_m = self._read_frame()
                # Expect Header frame
                ft_h, ch_h, payload_h = self._read_frame()
                # Expect Body frame
                ft_b, ch_b, payload_b = self._read_frame()

                if self.return_behavior == "disconnect":
                    self.server_sock.close()
                    break

                elif self.return_behavior == "return":
                    # Simulate mandatory routing failure:
                    # 1. Send Basic.Return (60, 50) with code 312 (NO_ROUTE)
                    return_payload = (
                        struct.pack(">HHH", CLASS_BASIC, METHOD_BASIC_RETURN, 312)
                        + pack_short_string("NO_ROUTE")
                        + pack_short_string("archive.events")
                        + pack_short_string("file.created")
                    )
                    self._send_frame(FRAME_METHOD, ch_m, return_payload)
                    # Send returned content header and body
                    self._send_frame(FRAME_HEADER, ch_h, payload_h)
                    self._send_frame(FRAME_BODY, ch_b, payload_b)

                    # 2. Broker still acknowledges receipt of the frame via Basic.Ack
                    ack_payload = struct.pack(">HHQB", CLASS_BASIC, METHOD_BASIC_ACK, delivery_tag, 0)
                    self._send_frame(FRAME_METHOD, ch_m, ack_payload)

                elif self.return_behavior == "nack":
                    # Simulate Broker NACK
                    nack_payload = struct.pack(">HHQB", CLASS_BASIC, METHOD_BASIC_NACK, delivery_tag, 0)
                    self._send_frame(FRAME_METHOD, ch_m, nack_payload)

                else: # "ack"
                    # Normal confirmed publication
                    ack_payload = struct.pack(">HHQB", CLASS_BASIC, METHOD_BASIC_ACK, delivery_tag, 0)
                    self._send_frame(FRAME_METHOD, ch_m, ack_payload)

                delivery_tag += 1

        except (ConnectionResetError, BrokenPipeError, OSError):
            pass
        finally:
            try:
                self.server_sock.close()
            except Exception:
                pass

    def close(self):
        try:
            self.client_sock.close()
        except Exception:
            pass
        try:
            self.server_sock.close()
        except Exception:
            pass

class TestAMQPProtocol(unittest.TestCase):
    """Test AMQP 0-9-1 handshake, confirm select, mandatory routing, and return handling."""

    def test_handshake_and_confirmed_publish(self):
        server = MockAMQPServer(return_behavior="ack")
        client = AMQPClient(timeout=2.0)
        try:
            client.connect(custom_socket=server.client_sock)
            self.assertTrue(client.is_connected)
            self.assertTrue(client.confirms_enabled)

            # Publish persistent message with mandatory flag
            res = client.publish(
                exchange="archive.events",
                routing_key="file.created",
                body=b'{"event_id": "evt_test_1"}',
                mandatory=True,
                delivery_mode=2,
                message_id="evt_test_1"
            )

            self.assertEqual(res.status, PublishStatus.CONFIRMED)
            self.assertTrue(res.routed)
            self.assertEqual(res.delivery_tag, 1)
        finally:
            client.close()
            server.close()

    def test_mandatory_unroutable_message_returns(self):
        """Verify that basic.return on unroutable message marks status=RETURNED and routed=False."""
        server = MockAMQPServer(return_behavior="return")
        client = AMQPClient(timeout=2.0)
        try:
            client.connect(custom_socket=server.client_sock)
            res = client.publish(
                exchange="archive.events",
                routing_key="file.created",
                body=b'{"event_id": "evt_unroutable"}',
                mandatory=True,
                delivery_mode=2,
            )

            self.assertEqual(res.status, PublishStatus.RETURNED)
            self.assertFalse(res.routed)
            self.assertEqual(res.reply_code, 312)
            self.assertIn("NO_ROUTE", res.reply_text)
        finally:
            client.close()
            server.close()

    def test_broker_nack(self):
        """Verify broker rejection results in NACKED status."""
        server = MockAMQPServer(return_behavior="nack")
        client = AMQPClient(timeout=2.0)
        try:
            client.connect(custom_socket=server.client_sock)
            res = client.publish(
                exchange="archive.events",
                routing_key="file.created",
                body=b'{"event_id": "evt_nack"}',
                mandatory=True,
            )

            self.assertEqual(res.status, PublishStatus.NACKED)
            self.assertFalse(res.routed)
        finally:
            client.close()
            server.close()

    def test_broker_disconnect(self):
        """Verify abrupt disconnection results in DISCONNECTED status."""
        server = MockAMQPServer(return_behavior="disconnect")
        client = AMQPClient(timeout=2.0)
        try:
            client.connect(custom_socket=server.client_sock)
            res = client.publish(
                exchange="archive.events",
                routing_key="file.created",
                body=b'{"event_id": "evt_disconnect"}',
                mandatory=True,
            )

            self.assertEqual(res.status, PublishStatus.DISCONNECTED)
            self.assertFalse(res.routed)
        finally:
            client.close()
            server.close()

if __name__ == "__main__":
    unittest.main()
