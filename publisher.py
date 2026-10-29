# -*- coding: utf-8 -*-
"""Outbox Relay Worker for standalone Integration Layer.

Safely relays events from Enterprise Archive Outbox API to RabbitMQ with
Publisher Confirms, mandatory routing verification, and persistent delivery.
"""

import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import List, Dict, Any, Optional, Tuple

from config import IntegrationConfig, default_config
from amqp_client import AMQPClient, PublishStatus, PublishResult

logger = logging.getLogger("IntegrationLayer.Publisher")

class OutboxPublisher:
    """Relays pending events from Enterprise Archive Outbox API to RabbitMQ."""

    def __init__(
        self,
        config: Optional[IntegrationConfig] = None,
        amqp_client: Optional[AMQPClient] = None,
    ):
        self.config = config or default_config
        self.amqp_client = amqp_client or AMQPClient(
            host=self.config.rabbitmq_host,
            port=self.config.rabbitmq_port,
            user=self.config.rabbitmq_user,
            password=self.config.rabbitmq_password,
            vhost=self.config.rabbitmq_vhost,
        )

    def _ensure_amqp_connected(self) -> bool:
        if not self.amqp_client.is_connected:
            try:
                self.amqp_client.connect()
                if hasattr(self.amqp_client, "setup_topology"):
                    self.amqp_client.setup_topology(
                        exchange=self.config.rabbitmq_exchange,
                        queue="ai_box.file_events",
                        routing_key=self.config.rabbitmq_routing_key,
                        exchange_type="direct",
                    )
                return True
            except Exception as e:
                logger.error("Failed to connect to RabbitMQ broker: %s", str(e))
                return False
        return True

    def fetch_pending_events(self) -> List[Dict[str, Any]]:
        """Fetch pending events from Enterprise Archive Outbox API."""
        url = f"{self.config.archive_base_url.rstrip('/')}/api/v1/outbox/pending?limit={self.config.batch_size}"
        req = urllib.request.Request(
            url,
            headers={
                "Authorization": f"Bearer {self.config.archive_service_token}",
                "Accept": "application/json",
                "User-Agent": "IntegrationLayer-Publisher/1.0",
            },
            method="GET",
        )
        try:
            with urllib.request.urlopen(req, timeout=10.0) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                return data.get("events", [])
        except urllib.error.HTTPError as e:
            logger.error("Archive Outbox API returned HTTP error: %d %s", e.code, e.reason)
            return []
        except Exception as e:
            logger.error("Failed to query Archive Outbox API at %s: %s", url, str(e))
            return []

    def ack_event(self, event_id: str) -> bool:
        """Acknowledge successful message publication to Archive Outbox."""
        url = f"{self.config.archive_base_url.rstrip('/')}/api/v1/outbox/ack"
        payload = json.dumps({"event_id": event_id}).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=payload,
            headers={
                "Authorization": f"Bearer {self.config.archive_service_token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "IntegrationLayer-Publisher/1.0",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=10.0) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                return data.get("status") == "success"
        except Exception as e:
            logger.error("Failed to ACK Outbox event %s in Archive: %s", event_id, str(e))
            return False

    def fail_event(self, event_id: str, error_msg: str) -> bool:
        """Report publishing failure to Archive Outbox."""
        url = f"{self.config.archive_base_url.rstrip('/')}/api/v1/outbox/fail"
        payload = json.dumps({"event_id": event_id, "error": error_msg}).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=payload,
            headers={
                "Authorization": f"Bearer {self.config.archive_service_token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "IntegrationLayer-Publisher/1.0",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=10.0) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                return data.get("status") == "success"
        except Exception as e:
            logger.error("Failed to report failure for event %s in Archive: %s", event_id, str(e))
            return False

    def process_event(self, event: Dict[str, Any]) -> PublishResult:
        """Publish a single Outbox event to RabbitMQ and update Outbox status accordingly."""
        event_id = event.get("event_id")
        if not event_id:
            logger.error("Invalid event object missing event_id: %s", event)
            return PublishResult(
                status=PublishStatus.ERROR,
                delivery_tag=0,
                routed=False,
                error="Missing event_id"
            )

        # Build clean message payload WITHOUT file bytes
        amqp_payload = {
            "event_id": event_id,
            "event_type": event.get("event_type", "file.created"),
            "file_id": event.get("file_id"),
            "file_name": event.get("file_name", ""),
            "file_path": event.get("file_path", ""),
            "owner_uid": event.get("owner_uid", ""),
            "group_name": event.get("group_name", "General"),
            "scope": event.get("scope") or ("personal" if event.get("group_name") == "personal" else "group"),
            "workspace_slug": event.get("workspace_slug"),
            "workspace_name": event.get("workspace_name"),
            "old_workspace_slug": event.get("old_workspace_slug"),
            "mime_type": event.get("mime_type", "text/plain"),
            "file_size": event.get("file_size", 0),
            "sha256": event.get("sha256_hash", ""),
            "timestamp": event.get("created_at"),
        }
        body_bytes = json.dumps(amqp_payload, ensure_ascii=False).encode("utf-8")

        if not self._ensure_amqp_connected():
            error_msg = "RabbitMQ broker connection unavailable"
            logger.warning("Cannot publish event %s: %s", event_id, error_msg)
            self.fail_event(event_id, error_msg)
            return PublishResult(
                status=PublishStatus.DISCONNECTED,
                delivery_tag=0,
                routed=False,
                error=error_msg
            )

        # Publish with persistent delivery (mode=2) and mandatory routing
        result = self.amqp_client.publish(
            exchange=self.config.rabbitmq_exchange,
            routing_key=self.config.rabbitmq_routing_key,
            body=body_bytes,
            mandatory=True,
            delivery_mode=2,
            content_type="application/json",
            message_id=event_id,
        )

        if result.status == PublishStatus.CONFIRMED and result.routed:
            logger.info("Event %s successfully published and routed (tag %d)", event_id, result.delivery_tag)
            # Only mark PUBLISHED in Outbox after broker confirmed routing
            acked = self.ack_event(event_id)
            if not acked:
                logger.warning(
                    "Event %s was published to RabbitMQ but Archive ACK call failed; "
                    "Outbox remains pending and will be safely re-published on retry.",
                    event_id
                )
        else:
            # Routing failure, broker rejection, or socket disconnect
            err = result.error or f"Publication rejected with status {result.status.value}"
            logger.warning("Event %s publication failed: %s", event_id, err)
            self.fail_event(event_id, err)

        return result

    def poll_once(self) -> Tuple[int, int]:
        """Fetch a batch of pending events and publish them."""
        events = self.fetch_pending_events()
        if not events:
            return 0, 0

        logger.info("Fetched %d pending events from Archive Outbox", len(events))
        success_count = 0
        failure_count = 0

        for event in events:
            res = self.process_event(event)
            if res.status == PublishStatus.CONFIRMED and res.routed:
                success_count += 1
            else:
                failure_count += 1

        return success_count, failure_count

    def run_loop(self, stop_event=None) -> None:
        """Run publisher polling loop."""
        logger.info("Starting Outbox Publisher polling loop...")
        while True:
            if stop_event and stop_event.is_set():
                logger.info("Stopping Outbox Publisher loop.")
                break
            try:
                self.poll_once()
            except Exception as e:
                logger.error("Unexpected error in publisher poll loop: %s", str(e), exc_info=True)
            time.sleep(self.config.poll_interval_seconds)
