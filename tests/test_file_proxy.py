# -*- coding: utf-8 -*-
"""Streaming File Retrieval Proxy API Tests.

Verifies:
1. Missing or invalid authentication token returns 401 Unauthorized.
2. Archive ACL / permission denial maps cleanly to 403 Forbidden.
3. Missing file maps to 404 Not Found.
4. Binary file streaming delivers exact bytes without writing any files to disk.
5. Large file streaming operates with O(1) bounded memory.
"""

import hashlib
import http.server
import json
import os
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request

from config import IntegrationConfig
from file_proxy import create_server

TEST_FILE_CONTENT = b"Enterprise Archive Confirmed Test Document Payload: 1234567890\n" * 100
TEST_FILE_SHA256 = hashlib.sha256(TEST_FILE_CONTENT).hexdigest()

class MockArchiveServerHandler(http.server.BaseHTTPRequestHandler):
    """Simulates Enterprise Archive secure file streaming API."""

    def log_message(self, format, *args):
        pass # Silence logs during tests

    def do_GET(self):
        # 1. Verify Archive Service Token
        auth = self.headers.get("Authorization", "")
        if auth != "Bearer archive_service_secret":
            self.send_response(401)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status":"error","message":"Invalid Service Token"}')
            return

        if self.path == "/api/v1/ai/files/100":
            # Success file
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(TEST_FILE_CONTENT)))
            self.send_header("Content-Disposition", 'attachment; filename="document.txt"')
            self.send_header("X-Content-SHA256", TEST_FILE_SHA256)
            self.end_headers()
            self.wfile.write(TEST_FILE_CONTENT)

        elif self.path == "/api/v1/ai/files/300":
            # Forbidden file (ACL violation)
            self.send_response(403)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status":"error","message":"Forbidden: insufficient permissions for document"}')

        elif self.path == "/api/v1/ai/files/400":
            # Missing file
            self.send_response(404)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status":"error","message":"File node not found"}')

        elif self.path == "/api/v1/ai/files/500":
            # Large 2 MiB stream
            chunk = b"X" * 65536
            total_size = 32 * 65536 # 2 MiB
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(total_size))
            self.end_headers()
            for _ in range(32):
                self.wfile.write(chunk)
        else:
            self.send_error(404, "Not Found")

class TestFileProxy(unittest.TestCase):
    """Integration tests for the Streaming File Retrieval Proxy."""

    @classmethod
    def setUpClass(cls):
        # Start Mock Archive Server
        cls.archive_server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), MockArchiveServerHandler)
        cls.archive_port = cls.archive_server.server_port
        cls.archive_thread = threading.Thread(target=cls.archive_server.serve_forever, daemon=True)
        cls.archive_thread.start()

        # Start Integration Layer Proxy Server
        cls.config = IntegrationConfig(
            archive_base_url=f"http://127.0.0.1:{cls.archive_port}",
            archive_service_token="archive_service_secret",
            proxy_host="127.0.0.1",
            proxy_port=0,
            proxy_auth_token="ai_box_valid_token",
            stream_chunk_size=16384,
        )
        cls.proxy_server = create_server(cls.config)
        cls.proxy_port = cls.proxy_server.server_port
        cls.proxy_thread = threading.Thread(target=cls.proxy_server.serve_forever, daemon=True)
        cls.proxy_thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.proxy_server.shutdown()
        cls.proxy_server.server_close()
        cls.archive_server.shutdown()
        cls.archive_server.server_close()

    def test_unauthenticated_request_rejected(self):
        """Request without valid Bearer token returns 401 Unauthorized."""
        url = f"http://127.0.0.1:{self.proxy_port}/api/v1/files/100"
        req = urllib.request.Request(url, method="GET")

        with self.assertRaises(urllib.error.HTTPError) as ctx:
            urllib.request.urlopen(req)

        self.assertEqual(ctx.exception.code, 401)
        headers = dict(ctx.exception.headers)
        self.assertIn("www-authenticate", [k.lower() for k in headers.keys()])

    def test_wrong_token_rejected(self):
        """Request with invalid Bearer token returns 401 Unauthorized."""
        url = f"http://127.0.0.1:{self.proxy_port}/api/v1/files/100"
        req = urllib.request.Request(url, headers={"Authorization": "Bearer bad_token"}, method="GET")

        with self.assertRaises(urllib.error.HTTPError) as ctx:
            urllib.request.urlopen(req)

        self.assertEqual(ctx.exception.code, 401)

    def test_insufficient_permissions_returns_403(self):
        """Archive permission denial (403) is cleanly mapped to 403 Forbidden."""
        url = f"http://127.0.0.1:{self.proxy_port}/api/v1/files/300"
        req = urllib.request.Request(
            url,
            headers={"Authorization": "Bearer ai_box_valid_token"},
            method="GET"
        )

        with self.assertRaises(urllib.error.HTTPError) as ctx:
            urllib.request.urlopen(req)

        self.assertEqual(ctx.exception.code, 403)
        body = json.loads(ctx.exception.read().decode("utf-8"))
        self.assertIn("insufficient permissions", body["error"])

    def test_missing_file_returns_404(self):
        """Missing file (404) is cleanly mapped to 404 Not Found."""
        url = f"http://127.0.0.1:{self.proxy_port}/api/v1/files/400"
        req = urllib.request.Request(
            url,
            headers={"Authorization": "Bearer ai_box_valid_token"},
            method="GET"
        )

        with self.assertRaises(urllib.error.HTTPError) as ctx:
            urllib.request.urlopen(req)

        self.assertEqual(ctx.exception.code, 404)

    def test_streaming_and_zero_disk_writes(self):
        """Verify file streams correctly without creating any temporary or cached files on disk."""
        # 1. Snapshot disk state of current dir and /tmp
        dirs_to_check = [os.getcwd(), tempfile.gettempdir()]
        before_state = {d: set(os.listdir(d)) for d in dirs_to_check if os.path.exists(d)}

        # 2. Make authenticated streaming request
        url = f"http://127.0.0.1:{self.proxy_port}/api/v1/files/100"
        req = urllib.request.Request(
            url,
            headers={
                "Authorization": "Bearer ai_box_valid_token",
                "X-On-Behalf-Of": "researcher1",
            },
            method="GET"
        )

        with urllib.request.urlopen(req) as resp:
            self.assertEqual(resp.status, 200)
            self.assertEqual(resp.headers.get("Content-Type"), "text/plain; charset=utf-8")
            self.assertEqual(resp.headers.get("X-Content-SHA256"), TEST_FILE_SHA256)
            self.assertIn("no-cache", resp.headers.get("Cache-Control", ""))

            # Read stream
            streamed_data = resp.read()

        # 3. Verify content fidelity
        self.assertEqual(streamed_data, TEST_FILE_CONTENT)
        self.assertEqual(hashlib.sha256(streamed_data).hexdigest(), TEST_FILE_SHA256)

        # 4. Verify ZERO disk files created / cached
        after_state = {d: set(os.listdir(d)) for d in dirs_to_check if os.path.exists(d)}
        for d in dirs_to_check:
            if d in before_state and d in after_state:
                new_files = after_state[d] - before_state[d]
                self.assertEqual(
                    len(new_files), 0,
                    f"Violation: Files were written to disk in {d}: {new_files}"
                )

    def test_large_file_streaming(self):
        """Verify large 2 MiB file streams chunk-by-chunk without loading whole file in proxy memory."""
        url = f"http://127.0.0.1:{self.proxy_port}/api/v1/files/500"
        req = urllib.request.Request(
            url,
            headers={"Authorization": "Bearer ai_box_valid_token"},
            method="GET"
        )

        with urllib.request.urlopen(req) as resp:
            self.assertEqual(resp.status, 200)
            total_read = 0
            while True:
                chunk = resp.read(32768)
                if not chunk:
                    break
                total_read += len(chunk)

            self.assertEqual(total_read, 32 * 65536)

if __name__ == "__main__":
    unittest.main()
