# -*- coding: utf-8 -*-
"""Outbox Publisher Integration and Recovery Tests."""

import http.server
import json
import threading
import unittest
from typing import Dict, Any

from config import IntegrationConfig
from amqp_client import AMQPClient, PublishStatus, PublishResult
from publisher import OutboxPublisher

class MockArchiveHandler(http.server.BaseHTTPRequestHandler):
    """Simulates Enterprise Archive Outbox API with authentication and persistent state."""

    outbox_db: Dict[str, Dict[str, Any]] = {}
    simulate_ack_failure: bool = False

    def log_message(self, format, *args):
        pass # Silence logs during tests

    def do_GET(self):
        # 1. Auth check
        auth = self.headers.get("Authorization", "")
        if auth != "Bearer test_service_token":
            self.send_error(401, "Unauthorized")
            return

        if self.path.startswith("/api/v1/outbox/pending"):
            # Return pending/failed events with retry_count < 10
            pending = [
                e for e in self.outbox_db.values()
                if e["status"] in ("PENDING", "FAILED") and e.get("retry_count", 0) < 10
            ]
            payload = json.dumps({"status": "success", "events": pending}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        else:
            self.send_error(404, "Not Found")

    def do_POST(self):
        auth = self.headers.get("Authorization", "")
        if auth != "Bearer test_service_token":
            self.send_error(401, "Unauthorized")
            return

        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length).decode("utf-8")) if length > 0 else {}
        event_id = body.get("event_id")

        if self.path == "/api/v1/outbox/ack":
            if self.simulate_ack_failure:
                # Simulate temporary failure when registering ACK (e.g. timeout / DB lock)
                self.send_error(500, "Simulated Database Lock")
                return

            if event_id in self.outbox_db:
                self.outbox_db[event_id]["status"] = "PUBLISHED"
                self.outbox_db[event_id]["published_at"] = 1700000100
                res = json.dumps({"status": "success", "event_id": event_id, "state": "PUBLISHED"}).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(res)))
                self.end_headers()
                self.wfile.write(res)
            else:
                self.send_error(404, "Event Not Found")

        elif self.path == "/api/v1/outbox/fail":
            if event_id in self.outbox_db:
                self.outbox_db[event_id]["status"] = "FAILED"
                self.outbox_db[event_id]["retry_count"] = self.outbox_db[event_id].get("retry_count", 0) + 1
                self.outbox_db[event_id]["last_error"] = body.get("error", "Unknown error")
                res = json.dumps({"status": "success", "event_id": event_id, "state": "FAILED"}).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(res)))
                self.end_headers()
                self.wfile.write(res)
            else:
                self.send_error(404, "Event Not Found")
        else:
            self.send_error(404, "Not Found")

class MockConfigurableAMQPClient(AMQPClient):
    """AMQP client mock returning predefined PublishResults for unit testing."""

    def __init__(self, mode: str = "confirm"):
        super().__init__()
        self.mode = mode # "confirm", "return", "disconnect", "nack"
        self.is_connected = True
        self.published_messages = []

    def connect(self, custom_socket=None):
        if self.mode == "disconnect":
            raise ConnectionError("Broker unreachable")
        self.is_connected = True

    def publish(self, exchange, routing_key, body, mandatory=True, delivery_mode=2, content_type="application/json", message_id=""):
        self.published_messages.append({
            "exchange": exchange,
            "routing_key": routing_key,
            "body": json.loads(body.decode("utf-8")),
            "mandatory": mandatory,
            "delivery_mode": delivery_mode,
            "message_id": message_id,
        })

        if self.mode == "confirm":
            return PublishResult(status=PublishStatus.CONFIRMED, delivery_tag=len(self.published_messages), routed=True)
        elif self.mode == "return":
            return PublishResult(
                status=PublishStatus.RETURNED,
                delivery_tag=len(self.published_messages),
                routed=False,
                reply_code=312,
                reply_text="NO_ROUTE",
                error="Mandatory message could not be routed: NO_ROUTE (312)"
            )
        elif self.mode == "disconnect":
            self.is_connected = False
            return PublishResult(status=PublishStatus.DISCONNECTED, delivery_tag=0, routed=False, error="Broker disconnected")
        elif self.mode == "nack":
            return PublishResult(status=PublishStatus.NACKED, delivery_tag=len(self.published_messages), routed=False, error="Broker NACK")
        return PublishResult(status=PublishStatus.ERROR, delivery_tag=0, routed=False)

class TestPublisher(unittest.TestCase):
    """Verify Outbox publishing, broker disconnection safety, routing failure handling, and retry recovery."""

    @classmethod
    def setUpClass(cls):
        cls.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), MockArchiveHandler)
        cls.server_port = cls.server.server_port
        cls.server_thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.server_thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    def setUp(self):
        MockArchiveHandler.simulate_ack_failure = False
        MockArchiveHandler.outbox_db = {
            "evt_001": {
                "event_id": "evt_001",
                "file_id": 1001,
                "file_name": "contract.md",
                "file_path": "/Legal/contract.md",
                "owner_uid": "lawyer1",
                "group_name": "Legal",
                "mime_type": "text/markdown",
                "file_size": 2048,
                "sha256_hash": "f" * 64,
                "status": "PENDING",
                "retry_count": 0,
                "created_at": 1700000001,
            }
        }
        self.config = IntegrationConfig(
            archive_base_url=f"http://127.0.0.1:{self.server_port}",
            archive_service_token="test_service_token",
        )

    def test_happy_path_confirmed_publish_and_ack(self):
        """Outbox event is read, published with confirm, and successfully marked PUBLISHED in Outbox."""
        amqp = MockConfigurableAMQPClient(mode="confirm")
        pub = OutboxPublisher(self.config, amqp_client=amqp)

        success, fail = pub.poll_once()

        self.assertEqual(success, 1)
        self.assertEqual(fail, 0)
        self.assertEqual(len(amqp.published_messages), 1)

        msg = amqp.published_messages[0]
        self.assertEqual(msg["body"]["event_id"], "evt_001")
        self.assertEqual(msg["body"]["file_id"], 1001)
        self.assertNotIn("content", msg["body"]) # Ensures NO file bytes are included
        self.assertTrue(msg["mandatory"])
        self.assertEqual(msg["delivery_mode"], 2) # Persistent

        # Outbox status in Archive MUST now be PUBLISHED
        self.assertEqual(MockArchiveHandler.outbox_db["evt_001"]["status"], "PUBLISHED")

    def test_broker_disconnect_leaves_outbox_unacked(self):
        """When RabbitMQ is down/disconnected, event must NOT be published or acked, remaining recoverable."""
        amqp = MockConfigurableAMQPClient(mode="disconnect")
        pub = OutboxPublisher(self.config, amqp_client=amqp)

        success, fail = pub.poll_once()

        self.assertEqual(success, 0)
        self.assertEqual(fail, 1)

        # Status must remain recoverable (FAILED with error), NEVER PUBLISHED
        outbox_entry = MockArchiveHandler.outbox_db["evt_001"]
        self.assertNotEqual(outbox_entry["status"], "PUBLISHED")
        self.assertEqual(outbox_entry["status"], "FAILED")
        self.assertEqual(outbox_entry["retry_count"], 1)
        self.assertIn("Broker", outbox_entry["last_error"])

    def test_unroutable_message_does_not_mark_published(self):
        """When basic.return is received due to routing failure, message is NOT marked PUBLISHED."""
        amqp = MockConfigurableAMQPClient(mode="return")
        pub = OutboxPublisher(self.config, amqp_client=amqp)

        success, fail = pub.poll_once()

        self.assertEqual(success, 0)
        self.assertEqual(fail, 1)

        outbox_entry = MockArchiveHandler.outbox_db["evt_001"]
        self.assertEqual(outbox_entry["status"], "FAILED")
        self.assertIn("NO_ROUTE", outbox_entry["last_error"])

    def test_archive_ack_failure_preserves_event_for_retry(self):
        """If broker confirms publish but Archive ACK HTTP call fails, event remains recoverable and is retried safely."""
        MockArchiveHandler.simulate_ack_failure = True
        amqp = MockConfigurableAMQPClient(mode="confirm")
        pub = OutboxPublisher(self.config, amqp_client=amqp)

        # Run first poll: RabbitMQ confirms, but Archive ACK fails
        pub.poll_once()

        # Archive DB still has it as PENDING because ACK failed
        self.assertEqual(MockArchiveHandler.outbox_db["evt_001"]["status"], "PENDING")

        # Now Archive recovers (simulate_ack_failure = False)
        MockArchiveHandler.simulate_ack_failure = False

        # Next poll cycle retries the exact same event
        success, fail = pub.poll_once()
        self.assertEqual(success, 1)
        self.assertEqual(MockArchiveHandler.outbox_db["evt_001"]["status"], "PUBLISHED")

        # Confirm idempotent event_id was preserved
        self.assertEqual(amqp.published_messages[1]["body"]["event_id"], "evt_001")

    def test_outbox_recovery_of_failed_events(self):
        """Events in FAILED status are picked up during subsequent polls and successfully PUBLISHED."""
        MockArchiveHandler.outbox_db["evt_001"]["status"] = "FAILED"
        MockArchiveHandler.outbox_db["evt_001"]["retry_count"] = 2

        amqp = MockConfigurableAMQPClient(mode="confirm")
        pub = OutboxPublisher(self.config, amqp_client=amqp)

        success, fail = pub.poll_once()
        self.assertEqual(success, 1)
        self.assertEqual(MockArchiveHandler.outbox_db["evt_001"]["status"], "PUBLISHED")

if __name__ == "__main__":
    unittest.main()
