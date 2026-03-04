import os
import tempfile
import unittest

import localDB


class TestConcurrencyLimits(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.old_ingest = localDB.INGEST_DB_NAME
        localDB.INGEST_DB_NAME = os.path.join(self.tmpdir.name, "ingestao_test.db")
        localDB.init_ingest_db()

        with localDB.get_conn(localDB.INGEST_DB_NAME) as conn:
            conn.execute(
                "INSERT INTO documents (id, sha256, original_name, mime, size_bytes, storage_uri_raw, status) VALUES (?, ?, ?, ?, ?, ?, ?)",
                ("doc-1", "sha-1", "a.pdf", "application/pdf", 10, "raw://a", localDB.STATUS_STORED),
            )
            conn.execute(
                "INSERT INTO documents (id, sha256, original_name, mime, size_bytes, storage_uri_raw, status) VALUES (?, ?, ?, ?, ?, ?, ?)",
                ("doc-2", "sha-2", "b.pdf", "application/pdf", 10, "raw://b", localDB.STATUS_STORED),
            )

    def tearDown(self):
        localDB.INGEST_DB_NAME = self.old_ingest
        self.tmpdir.cleanup()

    def test_try_acquire_processing_slot_respects_limit(self):
        ok1 = localDB.try_acquire_processing_slot("doc-1", max_active_docs=1)
        self.assertTrue(ok1)

        ok2 = localDB.try_acquire_processing_slot("doc-2", max_active_docs=1)
        self.assertFalse(ok2)

        docs = {d["id"]: d for d in localDB.list_ingest_documents()}
        self.assertEqual(docs["doc-1"]["status"], localDB.STATUS_PROCESSING_TEXT)
        self.assertEqual(docs["doc-2"]["status"], localDB.STATUS_STORED)


if __name__ == "__main__":
    unittest.main()
