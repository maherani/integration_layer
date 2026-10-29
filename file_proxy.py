# -*- coding: utf-8 -*-
"""Streaming File Retrieval Proxy API for AI Box.

Acts as an isolated, authenticated intermediary between AI Box and Enterprise Archive.
Streams file contents directly through memory in bounded chunks without disk caching.
"""

import http.server
import json
import logging
import re
import socket
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid
from typing import Optional

from config import IntegrationConfig, default_config

logger = logging.getLogger("IntegrationLayer.FileProxy")

FILE_PATH_PATTERN = re.compile(r"^/api/v1/files/(\d+)$")

class StreamingProxyHandler(http.server.BaseHTTPRequestHandler):
    """HTTP Request Handler that streams file content from Archive to AI Box."""

    server: "StreamingProxyServer"

    def log_message(self, format: str, *args) -> None:
        logger.info("%s - - [%s] %s", self.client_address[0], self.log_date_time_string(), format % args)

    def do_GET(self) -> None:
        parsed_url = urllib.parse.urlparse(self.path)
        path = parsed_url.path

        # Health endpoint
        if path == "/healthz" or path == "/health":
            self.send_json_response(200, {"status": "ok", "service": "integration-layer-file-proxy"})
            return

        # Match /api/v1/files/{file_id}
        match = FILE_PATH_PATTERN.match(path)
        if not match:
            self.send_json_response(404, {"error": "Not Found", "path": path})
            return

        file_id = match.group(1)

        # 1. Authenticate AI Box Client
        auth_header = self.headers.get("Authorization", "")
        expected_token = f"Bearer {self.server.config.proxy_auth_token}"
        if not auth_header or auth_header.strip() != expected_token:
            self.send_json_response(
                401,
                {"error": "Unauthorized: valid proxy bearer token required"},
                {"WWW-Authenticate": 'Bearer realm="Integration Layer Proxy"'}
            )
            return

        # 2. Extract Delegation & Correlation Headers
        on_behalf_of = self.headers.get("X-On-Behalf-Of")
        request_id = self.headers.get("X-Request-ID") or f"req_proxy_{uuid.uuid4().hex[:12]}"

        # 3. Stream from Enterprise Archive Secure API
        self._stream_archive_file(file_id, on_behalf_of, request_id)

    def _stream_archive_file(self, file_id: str, on_behalf_of: Optional[str], request_id: str) -> None:
        """Fetch file from Archive and stream directly to client without disk storage."""
        upstream_url = f"{self.server.config.archive_base_url.rstrip('/')}/api/v1/ai/files/{file_id}"

        upstream_headers = {
            "Authorization": f"Bearer {self.server.config.archive_service_token}",
            "X-Request-ID": request_id,
            "Accept": "application/octet-stream, */*",
            "User-Agent": "IntegrationLayer-Proxy/1.0",
        }
        if on_behalf_of:
            upstream_headers["X-On-Behalf-Of"] = on_behalf_of

        req = urllib.request.Request(upstream_url, headers=upstream_headers, method="GET")

        try:
            with urllib.request.urlopen(req, timeout=30.0) as upstream_resp:
                status_code = upstream_resp.status

                # Prepare downstream response headers
                self.send_response(status_code)
                for header_name in ["Content-Type", "Content-Length", "Content-Disposition", "X-Content-SHA256"]:
                    val = upstream_resp.headers.get(header_name)
                    if val is not None:
                        self.send_header(header_name, val)

                # Strict cache prevention for secure documents
                self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
                self.send_header("Pragma", "no-cache")
                self.send_header("X-Request-ID", request_id)
                self.end_headers()

                # Stream binary content chunk-by-chunk directly into socket wfile
                # Memory footprint is strictly bounded by chunk_size (O(1) memory)
                chunk_size = self.server.config.stream_chunk_size
                total_bytes = 0
                while True:
                    chunk = upstream_resp.read(chunk_size)
                    if not chunk:
                        break
                    self.wfile.write(chunk)
                    self.wfile.flush()
                    total_bytes += len(chunk)

                logger.info(
                    "Streamed %d bytes for file ID %s to AI Box (request_id=%s)",
                    total_bytes, file_id, request_id
                )

        except urllib.error.HTTPError as e:
            # Map upstream Archive errors cleanly without leaking internal tokens or data
            if e.code == 401:
                logger.error("Upstream Archive authentication failed (401)")
                self.send_json_response(502, {"error": "Upstream Archive service authentication failed", "request_id": request_id})
            elif e.code == 403:
                logger.warning("Upstream Archive permission denied (403) for file ID %s", file_id)
                self.send_json_response(403, {"error": "Access denied: insufficient permissions in Enterprise Archive", "request_id": request_id})
            elif e.code == 404:
                logger.info("File ID %s not found in Archive (404)", file_id)
                self.send_json_response(404, {"error": f"File ID {file_id} not found in Enterprise Archive", "request_id": request_id})
            else:
                logger.error("Upstream Archive error %d: %s", e.code, e.reason)
                self.send_json_response(e.code, {"error": f"Archive error: {e.reason}", "request_id": request_id})

        except (BrokenPipeError, ConnectionResetError):
            logger.warning("AI Box client aborted download connection during streaming of file ID %s", file_id)
        except Exception as e:
            logger.error("Failed to connect to Archive service at %s: %s", upstream_url, str(e))
            self.send_json_response(502, {"error": "Failed to connect to Enterprise Archive service", "request_id": request_id})

    def send_json_response(self, status: int, data: dict, extra_headers: Optional[dict] = None) -> None:
        payload = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        if extra_headers:
            for k, v in extra_headers.items():
                self.send_header(k, v)
        self.end_headers()
        self.wfile.write(payload)

class StreamingProxyServer(http.server.ThreadingHTTPServer):
    """Threaded HTTP Server for the streaming file proxy."""

    def __init__(self, config: Optional[IntegrationConfig] = None):
        self.config = config or default_config
        server_address = (self.config.proxy_host, self.config.proxy_port)
        super().__init__(server_address, StreamingProxyHandler)

def create_server(config: Optional[IntegrationConfig] = None) -> StreamingProxyServer:
    return StreamingProxyServer(config)
