import json
import os
import tempfile
import unittest

import localDB


class TestIngestHashDedup(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.old_db = localDB.DB_NAME
        self.old_ingest = localDB.INGEST_DB_NAME

        localDB.DB_NAME = os.path.join(self.tmpdir.name, "dados_financeiros_test.db")
        localDB.INGEST_DB_NAME = os.path.join(self.tmpdir.name, "ingestao_test.db")

        localDB.init_db()
        localDB.init_ingest_db()

    def tearDown(self):
        localDB.DB_NAME = self.old_db
        localDB.INGEST_DB_NAME = self.old_ingest
        self.tmpdir.cleanup()

    def test_store_raw_document_reuses_same_raw_hash(self):
        content = b"mesmo-arquivo"
        first = localDB.store_raw_document("nota.pdf", "application/pdf", content, storage_root=self.tmpdir.name)
        second = localDB.store_raw_document("nota-2.pdf", "application/pdf", content, storage_root=self.tmpdir.name)

        self.assertFalse(first["is_duplicate"])
        self.assertTrue(second["is_duplicate"])
        self.assertEqual(first["id"], second["id"])
        self.assertEqual(first["raw_hash"], localDB.compute_raw_hash(content))

    def test_finalize_skips_when_payload_hash_already_finalized(self):
        payload = [
            {"data": "2026-01-01", "descricao": "Item A", "valor": 10.0, "tipo": "saida", "categoria": "Outros"}
        ]
        payload_hash = localDB.compute_payload_hash(payload)

        review_path = os.path.join(self.tmpdir.name, "approved.json")
        with open(review_path, "w", encoding="utf-8") as handler:
            json.dump(payload, handler)

        with localDB.sqlite3.connect(localDB.INGEST_DB_NAME) as conn:
            conn.execute(
                "INSERT INTO documents (id, sha256, original_name, mime, size_bytes, storage_uri_raw, status, payload_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                ("doc-final", "sha1", "a.pdf", "application/pdf", 1, "raw://a", localDB.STATUS_FINALIZED, payload_hash),
            )
            conn.execute(
                "INSERT INTO documents (id, sha256, original_name, mime, size_bytes, storage_uri_raw, status) VALUES (?, ?, ?, ?, ?, ?, ?)",
                ("doc-new", "sha2", "b.pdf", "application/pdf", 1, "raw://b", localDB.STATUS_FINALIZE_PENDING),
            )
            conn.execute(
                "INSERT INTO reviews (document_id, reviewer, decision, edited_payload_uri, notes) VALUES (?, ?, ?, ?, ?)",
                ("doc-new", "tester", "APPROVED", review_path, "ok"),
            )

        ok, msg = localDB.finalize_document("doc-new")
        self.assertTrue(ok)
        self.assertIn("Finalização pulada", msg)

        docs = {d["id"]: d for d in localDB.list_ingest_documents()}
        self.assertEqual(docs["doc-new"]["status"], localDB.STATUS_FINALIZED)
        self.assertEqual(docs["doc-new"]["payload_hash"], payload_hash)

        tx = localDB.get_all_transactions()
        self.assertTrue(tx.empty)


if __name__ == "__main__":
    unittest.main()
