# -*- coding: utf-8 -*-
"""Configuration module for the standalone Integration Layer."""

import os
from dataclasses import dataclass

@dataclass(frozen=True)
class IntegrationConfig:
    # Enterprise Archive Outbox & Secure File API
    archive_base_url: str = os.environ.get(
        "ARCHIVE_API_BASE_URL", 
        "http://127.0.0.1:80/index.php/apps/archive_autotag"
    )
    archive_service_token: str = os.environ.get(
        "ARCHIVE_SERVICE_TOKEN", 
        "ai_service_token_secret"
    )

    # RabbitMQ Connection
    rabbitmq_host: str = os.environ.get("RABBITMQ_HOST", "127.0.0.1")
    rabbitmq_port: int = int(os.environ.get("RABBITMQ_PORT", "5672"))
    rabbitmq_user: str = os.environ.get("RABBITMQ_USER", "guest")
    rabbitmq_password: str = os.environ.get("RABBITMQ_PASSWORD", "guest")
    rabbitmq_vhost: str = os.environ.get("RABBITMQ_VHOST", "/")
    rabbitmq_exchange: str = os.environ.get("RABBITMQ_EXCHANGE", "archive.events")
    rabbitmq_routing_key: str = os.environ.get("RABBITMQ_ROUTING_KEY", "file.created")

    # File Retrieval Streaming Proxy for AI Box
    proxy_host: str = os.environ.get("PROXY_HOST", "0.0.0.0")
    proxy_port: int = int(os.environ.get("PROXY_PORT", "8090"))
    proxy_auth_token: str = os.environ.get("PROXY_AUTH_TOKEN", "ai_box_proxy_secret_token")
    stream_chunk_size: int = int(os.environ.get("STREAM_CHUNK_SIZE", str(64 * 1024))) # 64 KiB

    # Outbox Polling
    poll_interval_seconds: float = float(os.environ.get("POLL_INTERVAL_SECONDS", "2.0"))
    batch_size: int = int(os.environ.get("BATCH_SIZE", "50"))
    max_retries: int = int(os.environ.get("MAX_RETRIES", "10"))

default_config = IntegrationConfig()
