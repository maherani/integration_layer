# -*- coding: utf-8 -*-
"""Pure Python AMQP 0-9-1 Client with Publisher Confirms, Persistent Delivery, and Return Handling."""

import enum
import json
import logging
import socket
import struct
import time
from dataclasses import dataclass
from typing import Optional, Tuple, Dict, Any

logger = logging.getLogger("IntegrationLayer.AMQP")

# AMQP Frame Types
FRAME_METHOD = 1
FRAME_HEADER = 2
FRAME_BODY = 3
FRAME_HEARTBEAT = 8
FRAME_END = 0xCE

# AMQP Class & Method IDs
CLASS_CONNECTION = 10
METHOD_CONNECTION_START = 10
METHOD_CONNECTION_START_OK = 11
METHOD_CONNECTION_TUNE = 30
METHOD_CONNECTION_TUNE_OK = 31
METHOD_CONNECTION_OPEN = 40
METHOD_CONNECTION_OPEN_OK = 41
METHOD_CONNECTION_CLOSE = 50
METHOD_CONNECTION_CLOSE_OK = 51

CLASS_CHANNEL = 20
METHOD_CHANNEL_OPEN = 10
METHOD_CHANNEL_OPEN_OK = 11
METHOD_CHANNEL_CLOSE = 40
METHOD_CHANNEL_CLOSE_OK = 41

CLASS_CONFIRM = 85
METHOD_CONFIRM_SELECT = 10
METHOD_CONFIRM_SELECT_OK = 11

CLASS_EXCHANGE = 40
METHOD_EXCHANGE_DECLARE = 10
METHOD_EXCHANGE_DECLARE_OK = 11

CLASS_QUEUE = 50
METHOD_QUEUE_DECLARE = 10
METHOD_QUEUE_DECLARE_OK = 11
METHOD_QUEUE_BIND = 20
METHOD_QUEUE_BIND_OK = 21

CLASS_BASIC = 60
METHOD_BASIC_QOS = 10
METHOD_BASIC_QOS_OK = 11
METHOD_BASIC_PUBLISH = 40
METHOD_BASIC_RETURN = 50
METHOD_BASIC_GET = 70
METHOD_BASIC_GET_OK = 71
METHOD_BASIC_GET_EMPTY = 72
METHOD_BASIC_ACK = 80
METHOD_BASIC_NACK = 120

class PublishStatus(enum.Enum):
    CONFIRMED = "CONFIRMED"
    RETURNED = "RETURNED"
    NACKED = "NACKED"
    DISCONNECTED = "DISCONNECTED"
    ERROR = "ERROR"

@dataclass
class PublishResult:
    status: PublishStatus
    delivery_tag: int
    routed: bool
    reply_code: Optional[int] = None
    reply_text: Optional[str] = None
    error: Optional[str] = None

def pack_short_string(s: str) -> bytes:
    data = s.encode("utf-8")
    if len(data) > 255:
        raise ValueError(f"Short string exceeds 255 bytes: {s[:20]}...")
    return bytes([len(data)]) + data

def pack_long_string(data: bytes) -> bytes:
    return struct.pack(">I", len(data)) + data

def unpack_short_string(buf: bytes, offset: int) -> Tuple[str, int]:
    length = buf[offset]
    offset += 1
    val = buf[offset:offset + length].decode("utf-8", errors="replace")
    return val, offset + length

def build_frame(frame_type: int, channel: int, payload: bytes) -> bytes:
    header = struct.pack(">BHI", frame_type, channel, len(payload))
    return header + payload + bytes([FRAME_END])

class AMQPClient:
    """Robust, pure Python AMQP 0-9-1 client supporting publisher confirms and mandatory returns."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 5672,
        user: str = "guest",
        password: str = "guest",
        vhost: str = "/",
        timeout: float = 5.0,
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.vhost = vhost
        self.timeout = timeout
        self.sock: Optional[socket.socket] = None
        self.channel_id = 1
        self.is_connected = False
        self.confirms_enabled = False
        self.next_delivery_tag = 1
        self._returned_tags: Dict[int, Tuple[int, str]] = {}

    def connect(self, custom_socket: Optional[socket.socket] = None) -> None:
        """Establish connection and channel, enabling publisher confirms."""
        if custom_socket is not None:
            self.sock = custom_socket
        else:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(self.timeout)
            self.sock.connect((self.host, self.port))

        # 1. Protocol Header
        self._send_all(b"AMQP\x00\x00\x09\x01")

        # 2. Receive Connection.Start (10, 10)
        ft, ch, payload = self._read_frame()
        if ft != FRAME_METHOD or len(payload) < 4:
            raise ConnectionError(f"Unexpected initial frame from AMQP broker: type={ft}")
        cid, mid = struct.unpack_from(">HH", payload, 0)
        if (cid, mid) != (CLASS_CONNECTION, METHOD_CONNECTION_START):
            raise ConnectionError(f"Expected Connection.Start, got ({cid}, {mid})")

        # 3. Send Connection.StartOk (10, 11) with PLAIN credentials
        response_bytes = f"\x00{self.user}\x00{self.password}".encode("utf-8")
        start_ok_payload = (
            struct.pack(">HH", CLASS_CONNECTION, METHOD_CONNECTION_START_OK)
            + struct.pack(">I", 0) # empty client_properties table
            + pack_short_string("PLAIN")
            + pack_long_string(response_bytes)
            + pack_short_string("en_US")
        )
        self._send_frame(FRAME_METHOD, 0, start_ok_payload)

        # 4. Receive Connection.Tune (10, 30)
        ft, ch, payload = self._read_frame()
        cid, mid = struct.unpack_from(">HH", payload, 0)
        if (cid, mid) != (CLASS_CONNECTION, METHOD_CONNECTION_TUNE):
            raise ConnectionError(f"Expected Connection.Tune, got ({cid}, {mid})")
        channel_max, frame_max, heartbeat = struct.unpack_from(">HIH", payload, 4)

        # 5. Send Connection.TuneOk (10, 31)
        tune_ok_payload = struct.pack(">HHHIH", CLASS_CONNECTION, METHOD_CONNECTION_TUNE_OK, channel_max, frame_max, 0)
        self._send_frame(FRAME_METHOD, 0, tune_ok_payload)

        # 6. Send Connection.Open (10, 40)
        open_payload = struct.pack(">HH", CLASS_CONNECTION, METHOD_CONNECTION_OPEN) + pack_short_string(self.vhost) + b"\x00\x00"
        self._send_frame(FRAME_METHOD, 0, open_payload)

        # 7. Receive Connection.OpenOk (10, 41)
        ft, ch, payload = self._read_frame()
        cid, mid = struct.unpack_from(">HH", payload, 0)
        if (cid, mid) != (CLASS_CONNECTION, METHOD_CONNECTION_OPEN_OK):
            raise ConnectionError(f"Expected Connection.OpenOk, got ({cid}, {mid})")

        # 8. Open Channel 1 (20, 10)
        ch_open_payload = struct.pack(">HH", CLASS_CHANNEL, METHOD_CHANNEL_OPEN) + b"\x00"
        self._send_frame(FRAME_METHOD, self.channel_id, ch_open_payload)

        # 9. Receive Channel.OpenOk (20, 11)
        ft, ch, payload = self._read_frame()
        cid, mid = struct.unpack_from(">HH", payload, 0)
        if (cid, mid) != (CLASS_CHANNEL, METHOD_CHANNEL_OPEN_OK):
            raise ConnectionError(f"Expected Channel.OpenOk, got ({cid}, {mid})")

        # 10. Enable Publisher Confirms (85, 10)
        confirm_select_payload = struct.pack(">HHB", CLASS_CONFIRM, METHOD_CONFIRM_SELECT, 0)
        self._send_frame(FRAME_METHOD, self.channel_id, confirm_select_payload)

        # 11. Receive Confirm.SelectOk (85, 11)
        ft, ch, payload = self._read_frame()
        cid, mid = struct.unpack_from(">HH", payload, 0)
        if (cid, mid) != (CLASS_CONFIRM, METHOD_CONFIRM_SELECT_OK):
            raise ConnectionError(f"Expected Confirm.SelectOk, got ({cid}, {mid})")

        self.is_connected = True
        self.confirms_enabled = True
        self.next_delivery_tag = 1
        logger.info("Connected to AMQP broker with publisher confirms enabled on channel %d", self.channel_id)

    def publish(
        self,
        exchange: str,
        routing_key: str,
        body: bytes,
        mandatory: bool = True,
        delivery_mode: int = 2, # 2 = persistent
        content_type: str = "application/json",
        message_id: str = "",
    ) -> PublishResult:
        """Publish a message with persistent delivery, mandatory routing, and publisher confirm."""
        if not self.is_connected or self.sock is None:
            return PublishResult(
                status=PublishStatus.DISCONNECTED,
                delivery_tag=0,
                routed=False,
                error="Client is not connected to AMQP broker"
            )

        current_tag = self.next_delivery_tag
        self.next_delivery_tag += 1

        try:
            # 1. Method Frame: Basic.Publish (60, 40)
            # flags: mandatory (bit 0 = 0x01), immediate (bit 1 = 0x02)
            flags = 0x01 if mandatory else 0x00
            publish_payload = (
                struct.pack(">HHH", CLASS_BASIC, METHOD_BASIC_PUBLISH, 0) # reserved-1 = 0
                + pack_short_string(exchange)
                + pack_short_string(routing_key)
                + struct.pack(">B", flags)
            )

            # 2. Header Frame: Properties (class=60, weight=0, body_size)
            # Property flags:
            # content_type: 0x8000 (bit 15)
            # delivery_mode: 0x1000 (bit 12)
            # message_id: 0x0080 (bit 7)
            prop_flags = 0x8000 | 0x1000
            prop_bytes = pack_short_string(content_type) + struct.pack(">B", delivery_mode)
            if message_id:
                prop_flags |= 0x0080
                prop_bytes += pack_short_string(message_id)

            header_payload = (
                struct.pack(">HHQ", CLASS_BASIC, 0, len(body)) # weight=0, body_size=len
                + struct.pack(">H", prop_flags)
                + prop_bytes
            )

            # Send Method, Header, and Body frames
            self._send_frame(FRAME_METHOD, self.channel_id, publish_payload)
            self._send_frame(FRAME_HEADER, self.channel_id, header_payload)
            self._send_frame(FRAME_BODY, self.channel_id, body)

            # 3. Await Confirmation & handle mandatory returns
            return self._await_confirmation(current_tag)

        except (socket.error, ConnectionError, OSError) as e:
            logger.error("AMQP publish error on tag %d: %s", current_tag, str(e))
            self.is_connected = False
            return PublishResult(
                status=PublishStatus.DISCONNECTED,
                delivery_tag=current_tag,
                routed=False,
                error=f"Socket error: {str(e)}"
            )

    def _await_confirmation(self, expected_tag: int) -> PublishResult:
        """Wait for basic.ack or basic.nack, capturing any preceding basic.return."""
        start_time = time.time()
        was_returned = False
        return_code = None
        return_text = None

        while True:
            if time.time() - start_time > self.timeout:
                return PublishResult(
                    status=PublishStatus.ERROR,
                    delivery_tag=expected_tag,
                    routed=False,
                    error=f"Timeout waiting for publisher confirm on tag {expected_tag}"
                )

            ft, ch, payload = self._read_frame()

            if ft == FRAME_HEARTBEAT:
                # Echo heartbeat
                self._send_frame(FRAME_HEARTBEAT, 0, b"")
                continue

            if ft != FRAME_METHOD:
                continue

            cid, mid = struct.unpack_from(">HH", payload, 0)

            # Handle Basic.Return (60, 50)
            if (cid, mid) == (CLASS_BASIC, METHOD_BASIC_RETURN):
                return_code = struct.unpack_from(">H", payload, 4)[0]
                return_text, offset = unpack_short_string(payload, 6)
                was_returned = True
                logger.warning(
                    "Message tag %d returned by broker: code=%d, text='%s'",
                    expected_tag, return_code, return_text
                )
                # Consume the returned content header and body frames
                self._read_frame() # Header frame
                self._read_frame() # Body frame
                continue

            # Handle Basic.Ack (60, 80)
            if (cid, mid) == (CLASS_BASIC, METHOD_BASIC_ACK):
                ack_tag, multiple = struct.unpack_from(">QB", payload, 4)
                if ack_tag >= expected_tag:
                    if was_returned:
                        # Message was returned by broker due to mandatory routing failure!
                        return PublishResult(
                            status=PublishStatus.RETURNED,
                            delivery_tag=expected_tag,
                            routed=False,
                            reply_code=return_code,
                            reply_text=return_text,
                            error=f"Mandatory message could not be routed: {return_text} ({return_code})"
                        )
                    return PublishResult(
                        status=PublishStatus.CONFIRMED,
                        delivery_tag=expected_tag,
                        routed=True
                    )

            # Handle Basic.Nack (60, 120)
            if (cid, mid) == (CLASS_BASIC, METHOD_BASIC_NACK):
                nack_tag, flags = struct.unpack_from(">QB", payload, 4)
                if nack_tag >= expected_tag:
                    return PublishResult(
                        status=PublishStatus.NACKED,
                        delivery_tag=expected_tag,
                        routed=False,
                        error=f"Broker rejected message tag {nack_tag} (NACK)"
                    )


    def exchange_declare(self, exchange: str, exchange_type: str = "direct", durable: bool = True) -> None:
        """Declare an exchange idempotently."""
        flags = 0x02 if durable else 0x00
        payload = (
            struct.pack(">H", 0)
            + pack_short_string(exchange)
            + pack_short_string(exchange_type)
            + struct.pack(">B", flags)
            + struct.pack(">I", 0)
        )
        self._send_frame(FRAME_METHOD, self.channel_id, struct.pack(">HH", CLASS_EXCHANGE, METHOD_EXCHANGE_DECLARE) + payload)
        ft, ch, res_payload = self._read_frame()
        cid, mid = struct.unpack_from(">HH", res_payload, 0)
        if (cid, mid) != (CLASS_EXCHANGE, METHOD_EXCHANGE_DECLARE_OK):
            raise ConnectionError(f"Expected Exchange.DeclareOk (40, 11), got ({cid}, {mid})")

    def queue_declare(self, queue: str, durable: bool = True) -> str:
        """Declare a queue idempotently."""
        flags = 0x02 if durable else 0x00
        payload = (
            struct.pack(">H", 0)
            + pack_short_string(queue)
            + struct.pack(">B", flags)
            + struct.pack(">I", 0)
        )
        self._send_frame(FRAME_METHOD, self.channel_id, struct.pack(">HH", CLASS_QUEUE, METHOD_QUEUE_DECLARE) + payload)
        ft, ch, res_payload = self._read_frame()
        cid, mid = struct.unpack_from(">HH", res_payload, 0)
        if (cid, mid) != (CLASS_QUEUE, METHOD_QUEUE_DECLARE_OK):
            raise ConnectionError(f"Expected Queue.DeclareOk (50, 11), got ({cid}, {mid})")
        declared_queue, _ = unpack_short_string(res_payload, 4)
        return declared_queue

    def queue_bind(self, queue: str, exchange: str, routing_key: str) -> None:
        """Bind a queue to an exchange with a routing key."""
        payload = (
            struct.pack(">H", 0)
            + pack_short_string(queue)
            + pack_short_string(exchange)
            + pack_short_string(routing_key)
            + struct.pack(">B", 0)
            + struct.pack(">I", 0)
        )
        self._send_frame(FRAME_METHOD, self.channel_id, struct.pack(">HH", CLASS_QUEUE, METHOD_QUEUE_BIND) + payload)
        ft, ch, res_payload = self._read_frame()
        cid, mid = struct.unpack_from(">HH", res_payload, 0)
        if (cid, mid) != (CLASS_QUEUE, METHOD_QUEUE_BIND_OK):
            raise ConnectionError(f"Expected Queue.BindOk (50, 21), got ({cid}, {mid})")

    def setup_topology(
        self,
        exchange: str = "archive.events",
        queue: str = "ai_box.file_events",
        routing_key: str = "file.created",
        exchange_type: str = "direct"
    ) -> None:
        """Idempotently declare durable exchange, durable queue, and binding."""
        self.exchange_declare(exchange, exchange_type=exchange_type, durable=True)
        self.queue_declare(queue, durable=True)
        self.queue_bind(queue, exchange, routing_key)
        logger.info("AMQP topology verified: exchange='%s', queue='%s', routing_key='%s'", exchange, queue, routing_key)

    def basic_get(self, queue: str, no_ack: bool = False) -> Optional[Tuple[int, bytes, str]]:
        """Polls for a single message from the queue via Basic.Get."""
        payload = (
            struct.pack(">HH", CLASS_BASIC, METHOD_BASIC_GET)
            + struct.pack(">H", 0)
            + pack_short_string(queue)
            + struct.pack(">B", 1 if no_ack else 0)
        )
        self._send_frame(FRAME_METHOD, self.channel_id, payload)
        ft, ch, res_payload = self._read_frame()
        cid, mid = struct.unpack_from(">HH", res_payload, 0)
        if (cid, mid) == (CLASS_BASIC, METHOD_BASIC_GET_EMPTY):
            return None
        if (cid, mid) != (CLASS_BASIC, METHOD_BASIC_GET_OK):
            raise ConnectionError(f"Unexpected Basic.Get response: ({cid}, {mid})")
        delivery_tag, redelivered = struct.unpack_from(">QB", res_payload, 4)
        offset = 13
        exch, offset = unpack_short_string(res_payload, offset)
        rk, offset = unpack_short_string(res_payload, offset)

        ft, ch, hp = self._read_frame()
        if ft != FRAME_HEADER:
            raise ConnectionError(f"Expected Header frame, got type {ft}")
        body_size = struct.unpack_from(">Q", hp, 4)[0]

        body_bytes = b""
        while len(body_bytes) < body_size:
            ft, ch, bp = self._read_frame()
            if ft != FRAME_BODY:
                raise ConnectionError(f"Expected Body frame, got type {ft}")
            body_bytes += bp

        return (delivery_tag, body_bytes, rk)

    def basic_ack(self, delivery_tag: int, multiple: bool = False) -> None:
        """Send Basic.Ack for delivery_tag."""
        payload = struct.pack(">HHQB", CLASS_BASIC, METHOD_BASIC_ACK, delivery_tag, 1 if multiple else 0)
        self._send_frame(FRAME_METHOD, self.channel_id, payload)

    def basic_nack(self, delivery_tag: int, multiple: bool = False, requeue: bool = True) -> None:
        """Send Basic.Nack for delivery_tag."""
        flags = (1 if multiple else 0) | (2 if requeue else 0)
        payload = struct.pack(">HHQB", CLASS_BASIC, METHOD_BASIC_NACK, delivery_tag, flags)
        self._send_frame(FRAME_METHOD, self.channel_id, payload)

    def close(self) -> None:
        """Close connection cleanly."""
        self.is_connected = False
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None

    def _send_all(self, data: bytes) -> None:
        if self.sock is None:
            raise ConnectionError("Socket not open")
        self.sock.sendall(data)

    def _send_frame(self, frame_type: int, channel: int, payload: bytes) -> None:
        frame = build_frame(frame_type, channel, payload)
        self._send_all(frame)

    def _read_frame(self) -> Tuple[int, int, bytes]:
        if self.sock is None:
            raise ConnectionError("Socket not open")
        header = self._recv_exact(7)
        frame_type, channel, size = struct.unpack(">BHI", header)
        payload = self._recv_exact(size)
        end = self._recv_exact(1)
        if end != bytes([FRAME_END]):
            raise ConnectionError(f"Malformed frame: end marker {end!r} != {FRAME_END!r}")
        return frame_type, channel, payload

    def _recv_exact(self, count: int) -> bytes:
        buf = bytearray()
        while len(buf) < count:
            chunk = self.sock.recv(count - len(buf))
            if not chunk:
                raise ConnectionResetError("Connection closed while reading frame")
            buf.extend(chunk)
        return bytes(buf)


# ---------------------------------------------------------------------------
# Production Pika Client Adapter & Factory
# ---------------------------------------------------------------------------

try:
    import pika  # type: ignore
    HAS_PIKA = True
except ImportError:
    HAS_PIKA = False


class PikaAMQPClient:
    """AMQP client adapter utilizing the industry-standard 'pika' library.

    Provides production-grade maintainability, automatic heartbeat management,
    and robust connection recovery when pika is installed.

    NOTE: In the current offline/testing environment where a live RabbitMQ broker
    and Docker are not running, wire protocol compatibility remains UNVERIFIED
    against a real broker until live integration testing is conducted.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 5672,
        user: str = "guest",
        password: str = "guest",
        vhost: str = "/",
    ):
        if not HAS_PIKA:
            raise ImportError(
                "The 'pika' library is not installed. Install it via 'pip install pika' "
                "or 'apt install python3-pika'."
            )
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.vhost = vhost
        self.connection = None
        self.channel = None
        self.is_connected = False
        self._delivery_tag = 0

    def connect(self) -> None:
        credentials = pika.PlainCredentials(self.user, self.password)
        parameters = pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            virtual_host=self.vhost,
            credentials=credentials,
            heartbeat=60,
        )
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.channel.confirm_delivery()
        self.is_connected = True

    def publish(
        self,
        exchange: str,
        routing_key: str,
        body: bytes,
        mandatory: bool = True,
        delivery_mode: int = 2,
        content_type: str = "application/json",
        message_id: Optional[str] = None,
    ) -> PublishResult:
        if not self.is_connected or self.channel is None:
            return PublishResult(PublishStatus.DISCONNECTED, 0, False, "Not connected to RabbitMQ broker")

        self._delivery_tag += 1
        properties = pika.BasicProperties(
            delivery_mode=delivery_mode,
            content_type=content_type,
            message_id=message_id,
        )
        try:
            self.channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                body=body,
                properties=properties,
                mandatory=mandatory,
            )
            return PublishResult(PublishStatus.CONFIRMED, self._delivery_tag, True)
        except pika.exceptions.UnroutableError as e:
            return PublishResult(PublishStatus.RETURNED, self._delivery_tag, False, f"Message unroutable: {e}")
        except pika.exceptions.NackError as e:
            return PublishResult(PublishStatus.NACKED, self._delivery_tag, False, f"Broker nacked: {e}")
        except Exception as e:
            return PublishResult(PublishStatus.ERROR, self._delivery_tag, False, str(e))


    def exchange_declare(self, exchange: str, exchange_type: str = "direct", durable: bool = True) -> None:
        """Declare an exchange idempotently."""
        flags = 0x02 if durable else 0x00
        payload = (
            struct.pack(">H", 0)
            + pack_short_string(exchange)
            + pack_short_string(exchange_type)
            + struct.pack(">B", flags)
            + struct.pack(">I", 0)
        )
        self._send_frame(FRAME_METHOD, self.channel_id, struct.pack(">HH", CLASS_EXCHANGE, METHOD_EXCHANGE_DECLARE) + payload)
        ft, ch, res_payload = self._read_frame()
        cid, mid = struct.unpack_from(">HH", res_payload, 0)
        if (cid, mid) != (CLASS_EXCHANGE, METHOD_EXCHANGE_DECLARE_OK):
            raise ConnectionError(f"Expected Exchange.DeclareOk (40, 11), got ({cid}, {mid})")

    def queue_declare(self, queue: str, durable: bool = True) -> str:
        """Declare a queue idempotently."""
        flags = 0x02 if durable else 0x00
        payload = (
            struct.pack(">H", 0)
            + pack_short_string(queue)
            + struct.pack(">B", flags)
            + struct.pack(">I", 0)
        )
        self._send_frame(FRAME_METHOD, self.channel_id, struct.pack(">HH", CLASS_QUEUE, METHOD_QUEUE_DECLARE) + payload)
        ft, ch, res_payload = self._read_frame()
        cid, mid = struct.unpack_from(">HH", res_payload, 0)
        if (cid, mid) != (CLASS_QUEUE, METHOD_QUEUE_DECLARE_OK):
            raise ConnectionError(f"Expected Queue.DeclareOk (50, 11), got ({cid}, {mid})")
        declared_queue, _ = unpack_short_string(res_payload, 4)
        return declared_queue

    def queue_bind(self, queue: str, exchange: str, routing_key: str) -> None:
        """Bind a queue to an exchange with a routing key."""
        payload = (
            struct.pack(">H", 0)
            + pack_short_string(queue)
            + pack_short_string(exchange)
            + pack_short_string(routing_key)
            + struct.pack(">B", 0)
            + struct.pack(">I", 0)
        )
        self._send_frame(FRAME_METHOD, self.channel_id, struct.pack(">HH", CLASS_QUEUE, METHOD_QUEUE_BIND) + payload)
        ft, ch, res_payload = self._read_frame()
        cid, mid = struct.unpack_from(">HH", res_payload, 0)
        if (cid, mid) != (CLASS_QUEUE, METHOD_QUEUE_BIND_OK):
            raise ConnectionError(f"Expected Queue.BindOk (50, 21), got ({cid}, {mid})")

    def setup_topology(
        self,
        exchange: str = "archive.events",
        queue: str = "ai_box.file_events",
        routing_key: str = "file.created",
        exchange_type: str = "direct"
    ) -> None:
        """Idempotently declare durable exchange, durable queue, and binding."""
        self.exchange_declare(exchange, exchange_type=exchange_type, durable=True)
        self.queue_declare(queue, durable=True)
        self.queue_bind(queue, exchange, routing_key)
        logger.info("AMQP topology verified: exchange='%s', queue='%s', routing_key='%s'", exchange, queue, routing_key)

    def basic_get(self, queue: str, no_ack: bool = False) -> Optional[Tuple[int, bytes, str]]:
        """Polls for a single message from the queue via Basic.Get."""
        payload = (
            struct.pack(">HH", CLASS_BASIC, METHOD_BASIC_GET)
            + struct.pack(">H", 0)
            + pack_short_string(queue)
            + struct.pack(">B", 1 if no_ack else 0)
        )
        self._send_frame(FRAME_METHOD, self.channel_id, payload)
        ft, ch, res_payload = self._read_frame()
        cid, mid = struct.unpack_from(">HH", res_payload, 0)
        if (cid, mid) == (CLASS_BASIC, METHOD_BASIC_GET_EMPTY):
            return None
        if (cid, mid) != (CLASS_BASIC, METHOD_BASIC_GET_OK):
            raise ConnectionError(f"Unexpected Basic.Get response: ({cid}, {mid})")
        delivery_tag, redelivered = struct.unpack_from(">QB", res_payload, 4)
        offset = 13
        exch, offset = unpack_short_string(res_payload, offset)
        rk, offset = unpack_short_string(res_payload, offset)

        ft, ch, hp = self._read_frame()
        if ft != FRAME_HEADER:
            raise ConnectionError(f"Expected Header frame, got type {ft}")
        body_size = struct.unpack_from(">Q", hp, 4)[0]

        body_bytes = b""
        while len(body_bytes) < body_size:
            ft, ch, bp = self._read_frame()
            if ft != FRAME_BODY:
                raise ConnectionError(f"Expected Body frame, got type {ft}")
            body_bytes += bp

        return (delivery_tag, body_bytes, rk)

    def basic_ack(self, delivery_tag: int, multiple: bool = False) -> None:
        """Send Basic.Ack for delivery_tag."""
        payload = struct.pack(">HHQB", CLASS_BASIC, METHOD_BASIC_ACK, delivery_tag, 1 if multiple else 0)
        self._send_frame(FRAME_METHOD, self.channel_id, payload)

    def basic_nack(self, delivery_tag: int, multiple: bool = False, requeue: bool = True) -> None:
        """Send Basic.Nack for delivery_tag."""
        flags = (1 if multiple else 0) | (2 if requeue else 0)
        payload = struct.pack(">HHQB", CLASS_BASIC, METHOD_BASIC_NACK, delivery_tag, flags)
        self._send_frame(FRAME_METHOD, self.channel_id, payload)

    def close(self) -> None:
        self.is_connected = False
        if self.connection and self.connection.is_open:
            try:
                self.connection.close()
            except Exception:
                pass
        self.channel = None
        self.connection = None


def get_amqp_client(
    host: str = "127.0.0.1",
    port: int = 5672,
    user: str = "guest",
    password: str = "guest",
    vhost: str = "/",
    prefer_pika: bool = True,
):
    """Factory to create an AMQP client, prioritizing pika if installed."""
    if prefer_pika and HAS_PIKA:
        return PikaAMQPClient(host=host, port=port, user=user, password=password, vhost=vhost)
    return AMQPClient(host=host, port=port, user=user, password=password, vhost=vhost)
