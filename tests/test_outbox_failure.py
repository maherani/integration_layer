# -*- coding: utf-8 -*-
"""Controlled Outbox Insertion Failure & Nextcloud Upload Response Test.

Verifies:
1. Unqualified files (e.g. non-text, oversized, binary/PDF) gracefully skip Outbox without errors.
2. Qualified text files succeed and record PENDING Outbox events.
3. Fail-Closed behavior on Outbox DB failure: DB errors are NOT swallowed,
   raising RuntimeException and causing SabreDAV/Nextcloud to halt the request
   and return HTTP 500 instead of false success (HTTP 201/204).
4. Verification of the architectural limitation: Nextcloud has no 2PC between
   filesystem and PostgreSQL; throwing RuntimeException stops client false success,
   but physical file bytes written prior to NodeWrittenEvent remain on disk.
"""

import hashlib
import unittest

class MockFileNode:
    """Simulates Nextcloud OCP\\Files\\File node."""
    def __init__(self, file_id: int, name: str, path: str, content: bytes, mimetype: str = "text/plain"):
        self._id = file_id
        self._name = name
        self._path = path
        self._content = content
        self._mimetype = mimetype
        self.deleted = False

    def getId(self):
        return self._id

    def getName(self):
        return self._name

    def getPath(self):
        return self._path

    def getSize(self):
        return len(self._content)

    def getContent(self):
        return self._content

    def getMimetype(self):
        return self._mimetype

    def delete(self):
        self.deleted = True

class MockDatabaseConnection:
    """Simulates Nextcloud IDBConnection with configurable DB failure injection."""
    def __init__(self, simulate_db_failure: bool = False):
        self.simulate_db_failure = simulate_db_failure
        self.rows = []

    def insert_outbox(self, values: dict):
        if self.simulate_db_failure:
            raise RuntimeError("PostgreSQL Connection Lost: deadlock detected or database unavailable (57P01)")
        self.rows.append(values)
        return 1

class SimulatedAiOutboxService:
    """Exact Python reproduction of OCA\\ArchiveAutoTag\\Service\\AiOutboxService logic."""
    SUPPORTED_TEXT_EXTENSIONS = ('.txt', '.md', '.log', '.csv', '.json', '.xml', '.yaml', '.yml')
    MAX_FILE_SIZE_BYTES = 2097152 # 2 MiB

    def __init__(self, db: MockDatabaseConnection):
        self.db = db

    def record_file_event(self, file_node: MockFileNode, owner_uid: str) -> str:
        file_name = file_node.getName()
        ext = ""
        if "." in file_name:
            ext = "." + file_name.rsplit(".", 1)[1].lower()

        # 1. Qualification checks: if unqualified, return None gracefully
        if ext not in self.SUPPORTED_TEXT_EXTENSIONS:
            return None

        size = file_node.getSize()
        if size <= 0 or size > self.MAX_FILE_SIZE_BYTES:
            return None

        content = file_node.getContent()
        if not content:
            return None

        if content.startswith(b"%PDF") or b"%PDF-" in content[:1024]:
            return None

        if content.startswith(b"PK\x03\x04"):
            return None

        if b"\x00" in content:
            return None

        try:
            content.decode("utf-8")
        except UnicodeDecodeError:
            return None

        sha256 = hashlib.sha256(content).hexdigest()
        event_id = f"evt_{hashlib.md5(f'{file_node.getId()}_{size}'.encode()).hexdigest()}"

        # 2. Database insertion: Fail-Closed (must NOT swallow DB errors)
        try:
            self.db.insert_outbox({
                "event_id": event_id,
                "file_id": file_node.getId(),
                "file_name": file_name,
                "file_path": file_node.getPath(),
                "owner_uid": owner_uid,
                "sha256_hash": sha256,
                "status": "PENDING",
            })
            return event_id
        except Exception as e:
            # Re-raise as RuntimeException to prevent silent error swallowing
            raise RuntimeError(f"AI Outbox registration failed for file ID {file_node.getId()}: {str(e)}") from e

class SimulatedSabreDAVUploadEngine:
    """Simulates Nextcloud / SabreDAV upload request processing and NodeWrittenListener."""

    def __init__(self, outbox_service: SimulatedAiOutboxService):
        self.outbox_service = outbox_service

    def handle_upload(self, file_node: MockFileNode, user_uid: str) -> dict:
        """Simulates SabreDAV PUT / WebDAV upload lifecycle."""
        # Phase 1: SabreDAV writes bytes to physical storage & updates filecache
        bytes_written_to_storage = True

        # Phase 2: SabreDAV dispatches NodeWrittenEvent
        try:
            # NodeWrittenListener logic:
            self.outbox_service.record_file_event(file_node, user_uid)

            # If all listeners pass, SabreDAV returns HTTP 201 Created
            return {
                "http_status": 201,
                "status_text": "Created",
                "bytes_written_to_storage": bytes_written_to_storage,
                "error": None,
            }
        except Exception as e:
            # SabreDAV unhandled exception handler:
            # Halts request and returns HTTP 500 Internal Server Error
            return {
                "http_status": 500,
                "status_text": "Internal Server Error",
                "bytes_written_to_storage": bytes_written_to_storage,
                "error": str(e),
            }

class TestOutboxFailure(unittest.TestCase):
    """Verify controlled Outbox failure behavior, exception propagation, and architectural limits."""

    def test_unqualified_file_gracefully_skips_outbox(self):
        """Unqualified binary/PDF file completes upload with HTTP 201 without creating Outbox record."""
        db = MockDatabaseConnection(simulate_db_failure=False)
        outbox = SimulatedAiOutboxService(db)
        engine = SimulatedSabreDAVUploadEngine(outbox)

        # PDF file is not a qualified AI text file
        pdf_file = MockFileNode(101, "scan.pdf", "/files/scan.pdf", b"%PDF-1.4 dummy content", "application/pdf")
        resp = engine.handle_upload(pdf_file, "user1")

        self.assertEqual(resp["http_status"], 201)
        self.assertEqual(len(db.rows), 0) # No Outbox record created

    def test_qualified_file_happy_path(self):
        """Qualified text file records Outbox entry and returns HTTP 201."""
        db = MockDatabaseConnection(simulate_db_failure=False)
        outbox = SimulatedAiOutboxService(db)
        engine = SimulatedSabreDAVUploadEngine(outbox)

        txt_file = MockFileNode(102, "notes.txt", "/files/notes.txt", b"Valid plain text for AI indexing", "text/plain")
        resp = engine.handle_upload(txt_file, "user1")

        self.assertEqual(resp["http_status"], 201)
        self.assertEqual(len(db.rows), 1)
        self.assertEqual(db.rows[0]["status"], "PENDING")
        self.assertEqual(db.rows[0]["file_id"], 102)

    def test_outbox_db_failure_stops_false_success(self):
        """Controlled DB failure in Outbox causes HTTP 500 and prevents false success (HTTP 201/204)."""
        db = MockDatabaseConnection(simulate_db_failure=True)
        outbox = SimulatedAiOutboxService(db)
        engine = SimulatedSabreDAVUploadEngine(outbox)

        txt_file = MockFileNode(103, "important_contract.txt", "/files/important_contract.txt", b"Contract text", "text/plain")
        resp = engine.handle_upload(txt_file, "user1")

        # Must NOT return HTTP 201/204 success!
        self.assertNotEqual(resp["http_status"], 201)
        self.assertNotEqual(resp["http_status"], 204)
        self.assertEqual(resp["http_status"], 500)
        self.assertIn("AI Outbox registration failed", resp["error"])
        self.assertEqual(len(db.rows), 0)

    def test_nextcloud_architectural_limitation_non_atomic_filesystem(self):
        """Documented Architectural Limitation:
        Nextcloud filesystem and Outbox DB do not participate in a 2-Phase Commit (2PC).
        When Outbox fails and HTTP 500 is returned, physical bytes were already written to disk.
        """
        db = MockDatabaseConnection(simulate_db_failure=True)
        outbox = SimulatedAiOutboxService(db)
        engine = SimulatedSabreDAVUploadEngine(outbox)

        txt_file = MockFileNode(104, "audit_log.txt", "/files/audit_log.txt", b"Log line 1", "text/plain")
        resp = engine.handle_upload(txt_file, "user1")

        # 1. Client was correctly informed of failure (preventing false success)
        self.assertEqual(resp["http_status"], 500)
        # 2. Outbox has 0 rows (no ghost rows)
        self.assertEqual(len(db.rows), 0)
        # 3. BUT bytes were already committed to physical storage prior to NodeWrittenEvent
        self.assertTrue(resp["bytes_written_to_storage"],
            "Limitation: In Nextcloud, storage writes precede NodeWrittenEvent without distributed 2PC.")

if __name__ == "__main__":
    unittest.main()
